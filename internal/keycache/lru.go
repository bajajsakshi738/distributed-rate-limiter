// Package keycache is a small size-bounded LRU cache with a TTL, used for
// API key lookups (both "found" and "not found" results).
//
// The size bound matters: an attacker sending random keys could otherwise
// fill an unbounded "not found" cache until the process runs out of memory.
package keycache

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key     K
	value   V
	expires time.Time
}

type LRU[K comparable, V any] struct {
	mu  sync.Mutex
	ll  *list.List
	m   map[K]*list.Element
	cap int
	ttl time.Duration
	now func() time.Time
}

func New[K comparable, V any](capacity int, ttl time.Duration) *LRU[K, V] {
	return &LRU[K, V]{ll: list.New(), m: make(map[K]*list.Element, capacity), cap: capacity, ttl: ttl, now: time.Now}
}

func (c *LRU[K, V]) Get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.m[k]
	if !ok {
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if c.now().After(e.expires) {
		c.ll.Remove(el)
		delete(c.m, k)
		return zero, false
	}
	c.ll.MoveToFront(el)
	return e.value, true
}

func (c *LRU[K, V]) Add(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := c.now().Add(c.ttl)
	if el, ok := c.m[k]; ok {
		e := el.Value.(*entry[K, V])
		e.value, e.expires = v, exp
		c.ll.MoveToFront(el)
		return
	}
	c.m[k] = c.ll.PushFront(&entry[K, V]{key: k, value: v, expires: exp})
	if c.ll.Len() > c.cap {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.m, last.Value.(*entry[K, V]).key)
	}
}

func (c *LRU[K, V]) Purge() {
	c.mu.Lock()
	c.ll.Init()
	c.m = make(map[K]*list.Element, c.cap)
	c.mu.Unlock()
}

func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
