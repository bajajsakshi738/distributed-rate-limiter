package keycache

import (
	"testing"
	"time"
)

func TestLRUEvictsOldest(t *testing.T) {
	c := New[string, int](2, time.Minute)
	c.Add("a", 1)
	c.Add("b", 2)
	c.Get("a") // a is now most recent
	c.Add("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("a should still be cached")
	}
	if c.Len() != 2 {
		t.Fatalf("len %d", c.Len())
	}
}

func TestLRUExpires(t *testing.T) {
	now := time.Now()
	c := New[string, int](10, time.Second)
	c.now = func() time.Time { return now }
	c.Add("a", 1)
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatal("entry should have expired")
	}
}

func TestLRUPurge(t *testing.T) {
	c := New[string, int](10, time.Minute)
	c.Add("a", 1)
	c.Purge()
	if _, ok := c.Get("a"); ok || c.Len() != 0 {
		t.Fatal("purge should empty the cache")
	}
}
