package memory

import (
	"fmt"
	"sync"
	"testing"

	"blob-cache/common"
)

func newTestCache(t *testing.T, capacity int, opts ...func(*Builder[string, int])) *Cache[string, int] {
	t.Helper()
	b := NewBuilder[string, int](capacity).WithShards(1)
	for _, opt := range opts {
		opt(b)
	}
	return b.Build()
}

func TestCacheBasic(t *testing.T) {
	c := newTestCache(t, 10)

	e := c.Insert("a", 1)
	if e.IsNil() || e.Value() != 1 {
		t.Fatalf("insert/get entry failed: %+v", e)
	}
	if e.Refs() != 1 {
		t.Fatalf("refs = %d, want 1", e.Refs())
	}
	if got := c.Get("a"); got.Value() != 1 {
		t.Fatalf("get after insert = %v", got.Value())
	}
	if !c.Contains("a") || c.Contains("b") {
		t.Fatal("contains check failed")
	}
	e.Release()

	if r := c.Remove("a"); r.IsNil() || r.Value() != 1 {
		t.Fatal("remove should return the removed entry")
	}
	if c.Contains("a") {
		t.Fatal("entry should be removed")
	}
	if !c.Get("a").IsNil() {
		t.Fatal("get after remove should miss")
	}
}

func TestCacheLRUEviction(t *testing.T) {
	// High priority pool ratio 0 makes this a plain LRU.
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0})
	})

	for i := 0; i < 4; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	// Touch k0 so it becomes the most recently used.
	c.Touch("k0")
	c.Insert("k4", 4) // evicts k1

	for _, k := range []string{"k0", "k2", "k3", "k4"} {
		if !c.Contains(k) {
			t.Errorf("expected %q to be present", k)
		}
	}
	if c.Contains("k1") {
		t.Error("expected k1 to be evicted")
	}
	if c.Entries() != 4 || c.Usage() != 4 {
		t.Fatalf("entries=%d usage=%d, want 4/4", c.Entries(), c.Usage())
	}
}

func TestCacheLRUPriorityPool(t *testing.T) {
	// Pool ratio 0.5: pool capacity 2 of 4.
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0.5})
	})

	for i := 0; i < 6; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	// Pool capacity is 2; the first four high-priority inserts overflowed
	// k0, k1 and k2 into the low list. Inserting k5 evicts from the low
	// list first (k0).
	if c.Contains("k0") {
		t.Error("expected k0 to be evicted first (low priority pool)")
	}
	if !c.Contains("k5") {
		t.Error("expected k5 to be present")
	}

	// Low hints always go to the low priority pool.
	c2 := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0.5})
	})
	c2.InsertWithProperties("low", 1, common.Properties{}.WithHint(common.HintLow))
	c2.Insert("a", 2)
	c2.Insert("b", 3)
	// Now evict twice; the low-hinted entry must be evicted first.
	c2.Insert("c", 4)
	c2.Insert("d", 5)
	if c2.Contains("low") {
		t.Error("expected low hinted entry to be evicted first")
	}
}

func TestCachePinnedEntryNotEvicted(t *testing.T) {
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0})
	})

	ins := c.Insert("pinned", 100)
	// Acquiring via Get pins the record; the entry returned by Insert does
	// not pin (mirroring foyer).
	pinned := c.Get("pinned")
	if pinned.IsNil() {
		t.Fatal("expected hit")
	}
	ins.Release()
	for i := 0; i < 8; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	if !c.Contains("pinned") {
		t.Fatal("pinned entry must not be evicted while referenced")
	}

	// After release the entry becomes evictable again.
	pinned.Release()
	for i := 8; i < 12; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	if c.Contains("pinned") {
		t.Fatal("released entry should be evicted eventually")
	}
}

func TestCacheReplace(t *testing.T) {
	var leaves []string
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEventListener(common.EventListenerFunc[string, int](func(reason common.Event, key string, _ int) {
			leaves = append(leaves, fmt.Sprintf("%v:%s", reason, key))
		}))
	})

	c.Insert("a", 1)
	c.Insert("a", 2)
	if got := c.Get("a"); got.Value() != 2 {
		t.Fatalf("value after replace = %v, want 2", got.Value())
	}
	if c.Entries() != 1 {
		t.Fatalf("entries = %d, want 1", c.Entries())
	}
	found := false
	for _, l := range leaves {
		if l == "Replace:a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a replace event, got %v", leaves)
	}
}

func TestCacheWeighter(t *testing.T) {
	c := NewBuilder[string, string](10).
		WithShards(1).
		WithWeighter(func(key, value string) int { return len(value) }).
		Build()

	c.Insert("a", "aaaa") // weight 4
	c.Insert("b", "bb")   // weight 2
	if c.Usage() != 6 {
		t.Fatalf("usage = %d, want 6", c.Usage())
	}
	// Inserting c (weight 8) evicts until usage <= 10-8=2, so only `a` is
	// evicted and `b` survives; the cache ends up at capacity exactly.
	c.Insert("c", "cccccccc")
	if c.Usage() != 10 {
		t.Fatalf("usage = %d, want 10", c.Usage())
	}
	if c.Contains("a") {
		t.Error("expected a to be evicted")
	}
	if !c.Contains("b") || !c.Contains("c") {
		t.Error("expected b and c to be present")
	}
}

func TestCacheFilterPhantom(t *testing.T) {
	p := NewPipe[string, int]()
	var mu sync.Mutex
	var leaves []string
	c := newTestCache(t, 10, func(b *Builder[string, int]) {
		b.WithFilter(func(key string, _ int) bool { return key != "denied" })
		b.WithEventListener(common.EventListenerFunc[string, int](func(reason common.Event, key string, _ int) {
			mu.Lock()
			defer mu.Unlock()
			leaves = append(leaves, fmt.Sprintf("%v:%s", reason, key))
		}))
	})
	c.setPipe(p)

	e := c.Insert("denied", 42)
	if e.IsNil() || e.Value() != 42 {
		t.Fatal("phantom entry should still carry the value")
	}
	if c.Contains("denied") {
		t.Fatal("phantom entry must not be inserted")
	}
	if c.Entries() != 0 {
		t.Fatalf("entries = %d, want 0", c.Entries())
	}
	e.Release() // phantom entry release fires an evict event and pipes the piece
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, l := range leaves {
		if l == "Evict:denied" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected evict event for phantom entry, got %v", leaves)
	}
	if p.Len() != 1 {
		t.Fatalf("pipe len = %d, want 1", p.Len())
	}
	piece, ok := p.TryRecv()
	if !ok || piece.Key() != "denied" || piece.Value() != 42 {
		t.Fatal("piped phantom piece mismatch")
	}
}

func TestCachePipeOnEvict(t *testing.T) {
	p := NewPipe[string, int]()
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0})
	})
	c.setPipe(p)

	for i := 0; i < 6; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	if p.Len() != 2 {
		t.Fatalf("pipe len = %d, want 2 (two evictions)", p.Len())
	}
	piece, ok := p.TryRecv()
	if !ok || piece.Key() != "k0" {
		t.Fatalf("first evicted piece = %v", piece.Key())
	}
	if rest := p.PopAll(); len(rest) != 1 || rest[0].Key() != "k1" {
		t.Fatalf("second evicted piece mismatch: %v", rest)
	}

	// EvictAll offloads everything.
	c.EvictAll()
	if p.Len() != 4 {
		t.Fatalf("pipe len after evictAll = %d, want 4", p.Len())
	}
	if c.Entries() != 0 || c.Usage() != 0 {
		t.Fatalf("cache not empty after evictAll: entries=%d usage=%d", c.Entries(), c.Usage())
	}
}

func TestCacheIsOutdated(t *testing.T) {
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0})
	})
	e := c.Insert("k", 1)
	if e.IsOutdated() {
		t.Fatal("fresh entry should not be outdated")
	}
	for i := 0; i < 10; i++ {
		c.Insert(fmt.Sprintf("x%d", i), i)
	}
	if !e.IsOutdated() {
		t.Fatal("entry should be outdated after eviction")
	}
	// The value stays readable even when outdated.
	if e.Value() != 1 {
		t.Fatal("outdated entry should keep its value")
	}
	e.Release()
}

func TestCacheClear(t *testing.T) {
	var count int
	c := newTestCache(t, 10, func(b *Builder[string, int]) {
		b.WithEventListener(common.EventListenerFunc[string, int](func(reason common.Event, key string, _ int) {
			if reason == common.EventClear {
				count++
			}
		}))
	})
	for i := 0; i < 10; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	c.Clear()
	if c.Entries() != 0 || c.Usage() != 0 {
		t.Fatalf("cache not cleared: entries=%d usage=%d", c.Entries(), c.Usage())
	}
	if count != 10 {
		t.Fatalf("clear events = %d, want 10", count)
	}
	if !c.Get("k3").IsNil() {
		t.Fatal("get after clear should miss")
	}
}

func TestCacheCloneAndRefs(t *testing.T) {
	c := newTestCache(t, 4)
	e := c.Insert("a", 1)
	e2 := e.Clone()
	if e.Refs() != 2 {
		t.Fatalf("refs = %d, want 2", e.Refs())
	}
	e.Release()
	if e.Refs() != 1 {
		t.Fatalf("refs = %d, want 1", e.Refs())
	}
	e2.Release()
	// Pin semantics: an entry acquired via Get is pinned while any clone
	// of it is alive.
	c.Insert("b", 2)
	e3 := c.Get("b")
	e4 := e3.Clone()
	e3.Release()
	for i := 0; i < 8; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	if !c.Contains("b") {
		t.Fatal("entry pinned by clone should not be evicted")
	}
	e4.Release()
}

func TestCacheResize(t *testing.T) {
	c := newTestCache(t, 8)
	for i := 0; i < 8; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	if err := c.Resize(4); err != nil {
		t.Fatal(err)
	}
	if c.Capacity() != 4 {
		t.Fatalf("capacity = %d, want 4", c.Capacity())
	}
	if c.Entries() > 4 {
		t.Fatalf("entries = %d, want <= 4", c.Entries())
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewBuilder[int, int](512).WithShards(4).Build()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := (w*2000 + i) % 1000
				switch i % 4 {
				case 0:
					e := c.Insert(key, i)
					e.Release()
				case 1:
					e := c.Get(key)
					e.Release()
				case 2:
					e := c.Remove(key)
					e.Release()
				case 3:
					c.Contains(key)
					c.Touch(key)
				}
			}
		}(w)
	}
	wg.Wait()
	if c.Entries() < 0 {
		t.Fatal("negative entries")
	}
}

func TestFifoEviction(t *testing.T) {
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(FifoConfig{})
	})
	for i := 0; i < 4; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i)
	}
	// Touching must not affect FIFO order.
	c.Touch("k0")
	c.Get("k0").Release()
	c.Insert("k4", 4)
	if c.Contains("k0") {
		t.Error("FIFO must evict the earliest inserted entry regardless of access")
	}
	if !c.Contains("k4") {
		t.Error("expected k4 present")
	}
}
