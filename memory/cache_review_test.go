package memory

import (
	"fmt"
	"strings"
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

// TestCacheClearResetsUsageMetric verifies the MemoryUsage gauge returns
// to zero after Clear (regression: the gauge used to drift after clear).
func TestCacheClearResetsUsageMetric(t *testing.T) {
	c := newTestCache(t, 100)
	for i := 0; i < 10; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}
	if c.Metrics().MemoryUsage.Load() != 10 {
		t.Fatalf("usage gauge before clear = %d, want 10", c.Metrics().MemoryUsage.Load())
	}
	c.Clear()
	if got := c.Metrics().MemoryUsage.Load(); got != 0 {
		t.Fatalf("usage gauge after clear = %d, want 0", got)
	}
	if c.Metrics().MemoryEntries.Load() != 0 {
		t.Fatalf("entries gauge after clear = %d, want 0", c.Metrics().MemoryEntries.Load())
	}
}

// TestCacheUsageGaugeTracksEvictions verifies the MemoryUsage gauge stays
// consistent with the actual usage across eviction paths (regression: the
// gauge used to drift on Resize/EvictAll because evict() did not adjust
// it).
func TestCacheUsageGaugeTracksEvictions(t *testing.T) {
	c := NewBuilder[string, string](10).WithShards(1).
		WithWeighter(func(_, v string) int { return len(v) }).
		Build()
	for i := 0; i < 20; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), strings.Repeat("x", 5))
		e.Release()
	}
	if got, want := c.Metrics().MemoryUsage.Load(), int64(c.Usage()); got != want {
		t.Fatalf("usage gauge = %d, actual usage = %d", got, want)
	}
	c.EvictAll()
	if c.Usage() != 0 || c.Metrics().MemoryUsage.Load() != 0 {
		t.Fatalf("after evictAll: usage=%d gauge=%d", c.Usage(), c.Metrics().MemoryUsage.Load())
	}
	// Resize also evicts; the gauge must follow.
	for i := 0; i < 20; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), strings.Repeat("x", 5))
		e.Release()
	}
	if err := c.Resize(15); err != nil {
		t.Fatal(err)
	}
	if got, want := c.Metrics().MemoryUsage.Load(), int64(c.Usage()); got != want {
		t.Fatalf("after resize: usage gauge = %d, actual usage = %d", got, want)
	}
}
