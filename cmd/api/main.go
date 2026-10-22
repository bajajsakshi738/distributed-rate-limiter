// Command api runs one instance of the rate-limited API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/api"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/config"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/httpx"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

func main() {
	// Distroless images have no curl or wget, so the binary checks itself.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck("http://127.0.0.1:9090/healthz"))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hostname, _ := os.Hostname()
	instance := config.String("INSTANCE_ID", hostname)
	log = log.With("instance", instance)

	dbURL, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	keySecret, err := config.Required("KEY_SECRET")
	if err != nil {
		return err
	}
	signer, err := apikey.NewSigner(keySecret)
	if err != nil {
		return err
	}
	defaultStore := config.String("LIMITER_STORE", "redis")
	if defaultStore != "memory" && defaultStore != "redis" {
		return errors.New("LIMITER_STORE must be memory or redis")
	}

	db, err := store.Open(ctx, dbURL, int32(config.Int("DB_MAX_CONNS", 10)))
	if err != nil {
		return err
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     config.String("REDIS_ADDR", "localhost:6379"),
		Password: config.String("REDIS_PASSWORD", ""),
		// Without this, go-redis ignores context deadlines on the socket,
		// and the 15ms limiter timeout would not actually be enforced.
		ContextTimeoutEnabled: true,
		DialTimeout:           200 * time.Millisecond,
		ReadTimeout:           100 * time.Millisecond,
		WriteTimeout:          100 * time.Millisecond,
		MaxRetries:            -1, // no retries: a slow answer is worse than a fallback decision
		PoolSize:              64,
		MinIdleConns:          8,
	})
	defer rdb.Close()

	// Wait for the schema (the first service to start applies migrations).
	for {
		if err = db.Migrate(ctx); err == nil {
			break
		}
		log.Warn("waiting for postgres", "err", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(2 * time.Second):
		}
	}

	watcher := catalog.NewWatcher(db, catalog.RedisVersion{Client: rdb}, log)
	loadCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err = watcher.LoadInitial(loadCtx)
	cancel()
	if err != nil {
		return err
	}

	redisLimiter := limiter.NewRedis(rdb)
	if err := redisLimiter.Preload(ctx); err != nil {
		log.Warn("could not preload Lua scripts (Redis down?); they load on first use", "err", err)
	}
	resilient := limiter.NewResilient(
		redisLimiter,
		// Opens when, within 1 second, at least 10 Redis calls failed and they
		// were at least half of all calls; retries after 2 seconds.
		limiter.NewBreaker(config.Int("BREAKER_MIN_FAILURES", 10), 0.5, time.Second, config.Duration("BREAKER_COOLDOWN", 2*time.Second)),
		config.Duration("REDIS_TIMEOUT", 15*time.Millisecond),
		config.Int("INSTANCE_COUNT", 3),
		func() limiter.FailureMode { return watcher.Current().Settings.FailureMode },
	)
	memory := limiter.NewMemory()
	go memory.RunSweeper(ctx, 30*time.Second)
	go resilient.Fallback().RunSweeper(ctx, 30*time.Second)

	srv := api.New(api.Options{
		Signer:       signer,
		Instance:     instance,
		DefaultStore: defaultStore,
		Keys:         db,
		Catalog:      watcher,
		Memory:       memory,
		Redis:        resilient,
		Log:          log,
	})
	watcher.OnKeysChange(srv.PurgeKeyCaches)
	go watcher.Run(ctx)

	public := httpx.NewServer(config.String("LISTEN_ADDR", ":8080"), srv.PublicHandler(), log)
	internal := httpx.NewServer(config.String("INTERNAL_ADDR", ":9090"), srv.InternalHandler(), log)

	errc := make(chan error, 2)
	for _, s := range []*http.Server{public, internal} {
		go func(s *http.Server) {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}(s)
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	public.Shutdown(shutdownCtx)
	internal.Shutdown(shutdownCtx)
	log.Info("shut down cleanly")
	return nil
}

func healthcheck(url string) int {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
