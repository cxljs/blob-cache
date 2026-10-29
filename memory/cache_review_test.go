package memory

import (
	"fmt"
	"sync"
	"testing"

	"blob-cache/common"
)

// TestCacheTouchDoesNotPin verifies that Touch does not pin entries in the
// eviction container (a regression guard: Touch used to call the LRU
// acquire hook without a matching release, permanently pinning entries).
func TestCacheTouchDoesNotPin(t *testing.T) {
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0})
	})
	for i := 0; i < 4; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}
	if !c.Touch("k0") {
		t.Fatal("expected k0 to be present")
	}
	for i := 4; i < 12; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}
	if c.Contains("k0") {
		t.Fatal("touched entry must remain evictable (Touch must not pin)")
	}
}

// TestCacheRemoveFiresEvent verifies that an explicit remove notifies the
// event listener, mirroring foyer.
func TestCacheRemoveFiresEvent(t *testing.T) {
	var mu sync.Mutex
	var events []string
	c := newTestCache(t, 10, func(b *Builder[string, int]) {
		b.WithEventListener(common.EventListenerFunc[string, int](func(reason common.Event, key string, _ int) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, fmt.Sprintf("%v:%s", reason, key))
		}))
	})
	c.Insert("a", 1)
	e := c.Remove("a")
	e.Release()
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, ev := range events {
		if ev == "Remove:a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a remove event, got %v", events)
	}
}
