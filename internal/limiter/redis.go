package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/token_bucket.lua
var tokenBucketLua string

//go:embed scripts/fixed_window.lua
var fixedWindowLua string

var (
	tokenBucketScript = redis.NewScript(tokenBucketLua)
	fixedWindowScript = redis.NewScript(fixedWindowLua)
)

// Redis stores limiter state in Redis so every instance shares one count.
type Redis struct {
	client redis.Scripter
}

func NewRedis(client redis.Scripter) *Redis { return &Redis{client: client} }

// Preload loads the scripts so the first real request uses EVALSHA.
func (r *Redis) Preload(ctx context.Context) error {
	if err := tokenBucketScript.Load(ctx, r.client).Err(); err != nil {
		return err
	}
	return fixedWindowScript.Load(ctx, r.client).Err()
}

func (r *Redis) Allow(ctx context.Context, rule Rule, subject string) (Decision, error) {
	var (
		res []int64
		err error
	)
	ruleID := strconv.FormatInt(rule.ID, 10)
	switch rule.Algorithm {
	case FixedWindow:
		// The window length is part of the key, so editing a rule's window
		// starts fresh counters instead of mixing window numbers of two sizes.
		winMs := strconv.FormatInt(rule.Window.Milliseconds(), 10)
		key := "rl:fw:" + ruleID + ":" + winMs + ":" + subject
		res, err = fixedWindowScript.Run(ctx, r.client, []string{key},
			rule.Window.Milliseconds(), rule.Limit).Int64Slice()
	default:
		key := "rl:tb:" + ruleID + ":" + subject
		res, err = tokenBucketScript.Run(ctx, r.client, []string{key},
			strconv.FormatFloat(rule.RatePerMs(), 'f', -1, 64), rule.Capacity()).Int64Slice()
	}
	if err != nil {
		return Decision{}, err
	}
	if len(res) != 4 {
		return Decision{}, fmt.Errorf("unexpected script reply %v", res)
	}
	return Decision{
		Allowed:    res[0] == 1,
		Limit:      rule.Limit,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
		Reset:      time.Duration(res[3]) * time.Millisecond,
		Source:     "redis",
	}, nil
}
