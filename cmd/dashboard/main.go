// Command dashboard runs the web UI and the load generator.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/config"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/dashboard"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/httpx"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

func main() {
	// Distroless images have no curl or wget, so the binary checks itself.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck("http://127.0.0.1:8000/healthz"))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dashboard")
	if err := run(log); err != nil {
		log.Error("dashboard exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	user, err := config.Required("ADMIN_USER")
	if err != nil {
		return err
	}
	pass, err := config.Required("ADMIN_PASSWORD")
	if err != nil {
		return err
	}
	if len(pass) < 12 {
		return errors.New("ADMIN_PASSWORD must be at least 12 characters")
	}
	keySecret, err := config.Required("KEY_SECRET")
	if err != nil {
		return err
	}
	signer, err := apikey.NewSigner(keySecret)
	if err != nil {
		return err
	}
	csrf := []byte(os.Getenv("CSRF_SECRET"))
	if len(csrf) < 32 {
		csrf = make([]byte, 32)
		rand.Read(csrf)
		log.Info("CSRF_SECRET not set; using a random one (forms reset on restart)")
	}
	var instances []string
	for _, u := range strings.Split(config.String("INSTANCE_URLS", "http://api-1:9090,http://api-2:9090,http://api-3:9090"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			instances = append(instances, strings.TrimRight(u, "/"))
		}
	}

	db, err := store.Open(ctx, dbURL, 10)
	if err != nil {
		return err
	}
	defer db.Close()
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
	if n, err := db.CleanupOrphans(ctx); err != nil {
		log.Warn("orphan cleanup failed", "err", err)
	} else if n > 0 {
		log.Info("revoked leftover load-test keys", "count", n)
	}

	// Public demo key on the tiny "demo" plan, shown on the live page so
	// visitors can trigger 429s themselves. Derived from KEY_SECRET, so it's
	// the same after every restart. Set DEMO_KEY=off to hide it.
	var demoKey string
	if config.String("DEMO_KEY", "on") != "off" {
		if plan, err := db.PlanByName(ctx, "demo"); err != nil {
			log.Warn("demo plan missing; demo key disabled", "err", err)
		} else {
			k := signer.Derive("public-demo")
			if err := db.EnsureKey(ctx, k, "public-demo", plan.ID); err != nil {
				log.Warn("could not create demo key", "err", err)
			} else {
				demoKey = k
			}
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:                  config.String("REDIS_ADDR", "localhost:6379"),
		Password:              config.String("REDIS_PASSWORD", ""),
		ContextTimeoutEnabled: true,
		DialTimeout:           500 * time.Millisecond,
		ReadTimeout:           500 * time.Millisecond,
		WriteTimeout:          500 * time.Millisecond,
	})
	defer rdb.Close()

	srv, err := dashboard.New(dashboard.Options{
		Store:         db,
		Signer:        signer,
		DemoKey:       demoKey,
		Bumper:        catalog.RedisVersion{Client: rdb},
		InstanceURLs:  instances,
		LoadTarget:    config.String("LOADTEST_TARGET", "http://nginx/api/data"),
		AdminUser:     user,
		AdminPassword: pass,
		CSRFSecret:    csrf,
		DefaultStore:  config.String("LIMITER_STORE", "redis"),
		MaxRPS:        config.Int("LOADTEST_MAX_RPS", 1000),
		MaxDuration:   config.Duration("LOADTEST_MAX_DURATION", 60*time.Second),
		MaxClients:    config.Int("LOADTEST_MAX_CLIENTS", 50),
		Log:           log,
	})
	if err != nil {
		return err
	}
	srv.Start(ctx)

	hs := httpx.NewServer(config.String("LISTEN_ADDR", ":8000"), srv.Handler(), log)
	hs.WriteTimeout = 30 * time.Second
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", hs.Addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
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
