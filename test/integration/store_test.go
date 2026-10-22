//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// running migrations twice must be a no-op
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStoreKeysLifecycle(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	plan, err := s.PlanByName(ctx, "free")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := signer.Generate()
	info, err := s.CreateKey(ctx, raw, "integration", plan.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.Plan != "free" || info.Prefix != apikey.DisplayPrefix(raw) {
		t.Fatalf("bad key %q %+v", raw, info)
	}
	rec, found, err := s.LookupKey(ctx, apikey.Hash(raw))
	if err != nil || !found || rec.ID != info.ID || rec.PlanID != plan.ID {
		t.Fatalf("lookup: %+v %v %v", rec, found, err)
	}
	if err := s.RevokeKey(ctx, info.ID); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.LookupKey(ctx, apikey.Hash(raw)); found {
		t.Fatal("revoked key must not be found")
	}
	if err := s.RevokeKey(ctx, info.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double revoke: %v", err)
	}
}

func TestStoreSnapshotAndSettings(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if err := s.SetSetting(ctx, "store", "memory"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSetting(ctx, "store", "mongodb"); err == nil {
		t.Fatal("invalid store accepted")
	}
	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := s.PlanByName(ctx, "pro")
	rule, p, ok := snap.RuleForPlan(plan.ID)
	if !ok || p.Name != "pro" || rule.Limit < 1 || snap.Settings.Store != "memory" {
		t.Fatalf("snapshot: %+v %+v %v %+v", rule, p, ok, snap.Settings)
	}
	rule.Limit = 0
	if err := s.UpdateRule(ctx, rule); err == nil {
		t.Fatal("invalid rule accepted")
	}
	rule.Limit, rule.Algorithm = 100, limiter.FixedWindow
	if err := s.UpdateRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	rule.Algorithm = limiter.TokenBucket
	s.UpdateRule(ctx, rule)
	s.SetSetting(ctx, "store", "redis")
}

func TestStoreRunsAndShowcase(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	plan, _ := s.PlanByName(ctx, "free")
	id, err := s.CreateRun(ctx, store.Run{PlanName: "free", Clients: 2, RPS: 50, DurationS: 3, Pattern: "steady", Store: "redis", Algorithm: "token_bucket"})
	if err != nil {
		t.Fatal(err)
	}
	k1, _ := signer.Generate()
	k2, _ := signer.Generate()
	keys := []string{k1, k2}
	if err := s.CreateRunKeys(ctx, keys, plan.ID, id); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.LookupKey(ctx, apikey.Hash(keys[0])); !found {
		t.Fatal("run key should be active during the run")
	}
	if err := s.SetRunMarker(ctx, id, 2, "switched to redis"); err != nil {
		t.Fatal(err)
	}
	s.SetRunMarker(ctx, id, 3, "second marker ignored")
	p99, ratio, hitPct := 1500, 0.97, 99.8
	err = s.FinishRun(ctx, store.Run{ID: id, Status: "done", Sent: 150, Allowed: 30, Rejected: 120, P99us: &p99,
		LimitRatio: &ratio, CacheHitPct: &hitPct},
		[]store.RunSecond{
			{Second: 0, Instance: "api-1", Allowed: 5, Rejected: 10},
			{Second: 0, Instance: "api-2", Allowed: 5, Rejected: 10},
			{Second: 1, Instance: "api-1", Allowed: 10, Rejected: 40},
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeRunKeys(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.LookupKey(ctx, apikey.Hash(keys[1])); found {
		t.Fatal("run keys must be revoked after the run")
	}
	run, err := s.GetRun(ctx, id)
	if err != nil || run.Status != "done" || *run.P99us != 1500 || *run.MarkerSecond != 2 || run.MarkerLabel != "switched to redis" ||
		*run.LimitRatio != 0.97 || *run.CacheHitPct != 99.8 {
		t.Fatalf("run: %+v %v", run, err)
	}
	secs, _ := s.RunSeconds(ctx, id)
	if len(secs) != 3 {
		t.Fatalf("seconds: %d", len(secs))
	}
	if err := s.PinShowcase(ctx, id, "the 3x bug, then the fix", run.MarkerSecond, run.MarkerLabel); err != nil {
		t.Fatal(err)
	}
	sc, err := s.ShowcaseRun(ctx)
	if err != nil || sc.ID != id || !sc.IsShowcase {
		t.Fatalf("showcase: %+v %v", sc, err)
	}

	// a run left "running" by a crash is cleaned up at startup
	orphan, _ := s.CreateRun(ctx, store.Run{PlanName: "free", Clients: 1, RPS: 1, DurationS: 1, Pattern: "steady", Store: "redis", Algorithm: "token_bucket"})
	ok1, _ := signer.Generate()
	okeys := []string{ok1}
	s.CreateRunKeys(ctx, okeys, plan.ID, orphan)
	if _, err := s.CleanupOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.LookupKey(ctx, apikey.Hash(okeys[0])); found {
		t.Fatal("orphaned run key must be revoked")
	}
	if r, _ := s.GetRun(ctx, orphan); r.Status != "failed" {
		t.Fatalf("orphan status %s", r.Status)
	}
}

func TestEnsureKeyIsIdempotent(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	plan, err := s.PlanByName(ctx, "demo")
	if err != nil {
		t.Fatal("demo plan missing:", err)
	}
	k := signer.Derive(fmt.Sprintf("test-demo-%d", time.Now().UnixNano())) // fresh key on every run
	for i := 0; i < 2; i++ {
		if err := s.EnsureKey(ctx, k, "public-demo", plan.ID); err != nil {
			t.Fatal(err)
		}
	}
	rec, found, _ := s.LookupKey(ctx, apikey.Hash(k))
	if !found || rec.PlanID != plan.ID {
		t.Fatal("demo key not stored")
	}
	s.RevokeKey(ctx, rec.ID)
	s.EnsureKey(ctx, k, "public-demo", plan.ID)
	if _, found, _ := s.LookupKey(ctx, apikey.Hash(k)); found {
		t.Fatal("EnsureKey must not un-revoke a key an admin revoked")
	}
}
