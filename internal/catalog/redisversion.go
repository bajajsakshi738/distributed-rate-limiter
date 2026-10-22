package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/redis/go-redis/v9"
)

const (
	ConfigVersionKey = "config:version" // rules and settings changed
	KeysVersionKey   = "keys:version"   // API keys revoked
)

// RedisVersion reads and changes the version markers.
type RedisVersion struct {
	Client redis.UniversalClient
}

func (r RedisVersion) Versions(ctx context.Context) (Versions, error) {
	vals, err := r.Client.MGet(ctx, ConfigVersionKey, KeysVersionKey).Result()
	if err != nil {
		return Versions{}, err
	}
	str := func(v any) string {
		s, _ := v.(string)
		return s
	}
	return Versions{Config: str(vals[0]), Keys: str(vals[1])}, nil
}

// BumpConfig tells every instance to reload rules and settings.
func (r RedisVersion) BumpConfig(ctx context.Context) error { return r.bump(ctx, ConfigVersionKey) }

// BumpKeys tells every instance to clear its API key caches.
func (r RedisVersion) BumpKeys(ctx context.Context) error { return r.bump(ctx, KeysVersionKey) }

// bump stores a new random value. If Redis is down this fails, and instances
// pick the change up on their 60-second full reload / cache expiry instead.
func (r RedisVersion) bump(ctx context.Context, key string) error {
	b := make([]byte, 8)
	rand.Read(b)
	return r.Client.Set(ctx, key, hex.EncodeToString(b), 0).Err()
}
