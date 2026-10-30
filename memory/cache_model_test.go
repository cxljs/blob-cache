package memory

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// lruModel is the reference implementation for a single-shard, pure-LRU
// cache (HighPriorityPoolRatio 0, default weighter 1, no pins: every Get
// releases immediately). blob-cache's Cache must behave identically.
type lruModel struct {
	order []string // least-recently-used first
	vals  map[string]int
	cap   int
}

func newLRUModel(cap int) *lruModel {
	return &lruModel{vals: make(map[string]int), cap: cap}
}

func (m *lruModel) touch(k string) {
	for i, e := range m.order {
		if e == k {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.order = append(m.order, k)
}

func (m *lruModel) insert(k string, v int) {
	// Mirror foyer's emplace order (raw.rs:190): evict down to
	// (capacity - weight) BEFORE the replace frees the old weight. This
	// matches the real cache (and foyer): replacing a resident entry at
	// full capacity evicts one extra LRU entry, and the eviction may even
	// pick k itself, which is then re-inserted at the MRU position.
	for len(m.vals) > m.cap-1 {
		victim := m.order[0]
		m.order = m.order[1:]
		delete(m.vals, victim)
	}
	m.touch(k)
	m.vals[k] = v
}

func (m *lruModel) get(k string) (int, bool) {
	v, ok := m.vals[k]
	if ok {
		m.touch(k)
	}
	return v, ok
}

func (m *lruModel) remove(k string) {
	for i, e := range m.order {
		if e == k {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	delete(m.vals, k)
}

func (m *lruModel) contains(k string) bool {
	_, ok := m.vals[k]
	return ok
}

// TestCacheDifferentialLRU applies the same random operation sequence to
// the real cache and to a simple reference model and compares the full
// visible state after every operation.
func TestCacheDifferentialLRU(t *testing.T) {
	const (
		keys     = 32
		capacity = 8
		ops      = 5000
	)
	rng := rand.New(rand.NewSource(20260501))

	c := NewBuilder[string, int](capacity).
		WithShards(1).
		WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0}).
		Build()
	m := newLRUModel(capacity)

	verify := func(step int) {
		t.Helper()
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("k%d", i)
			want := m.contains(k)
			if got := c.Contains(k); got != want {
				t.Fatalf("step %d: Contains(%q) = %v, model says %v", step, k, got, want)
			}
			if want {
				if got := c.Get(k); got.IsNil() {
					t.Fatalf("step %d: Get(%q) miss but model has it", step, k)
				} else {
					if got.Value() != m.vals[k] {
						t.Fatalf("step %d: Get(%q) = %d, model says %d", step, k, got.Value(), m.vals[k])
					}
					got.Release()
				}
			} else if got := c.Get(k); !got.IsNil() {
				t.Fatalf("step %d: Get(%q) hit but model does not have it", step, k)
			}
			// The verify Get above touches the entry (acquire/release moves
			// it to the MRU position); keep the model in sync.
			m.get(k)
		}
		if c.Entries() != len(m.vals) {
			t.Fatalf("step %d: Entries = %d, model says %d", step, c.Entries(), len(m.vals))
		}
		if c.Usage() != len(m.vals) {
			t.Fatalf("step %d: Usage = %d, model says %d", step, c.Usage(), len(m.vals))
		}
	}

	for step := 0; step < ops; step++ {
		k := fmt.Sprintf("k%d", rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1, 2, 3: // insert / overwrite
			v := step
			c.Insert(k, v).Release()
			m.insert(k, v)
		case 4, 5, 6: // get (released immediately: no pins)
			if e := c.Get(k); !e.IsNil() {
				e.Release()
			}
			m.get(k)
		case 7: // remove
			e := c.Remove(k)
			if !e.IsNil() {
				e.Release()
			}
			m.remove(k)
		case 8: // contains / touch
			if c.Contains(k) != m.contains(k) {
				t.Fatalf("step %d: Contains(%q) mismatch", step, k)
			}
			if c.Touch(k) != m.contains(k) {
				t.Fatalf("step %d: Touch(%q) mismatch", step, k)
			}
		case 9: // clear occasionally
			if rng.Intn(50) == 0 {
				c.Clear()
				m = newLRUModel(capacity)
			}
		}
		if step%97 == 0 {
			verify(step)
		}
	}
	verify(ops)
}

// TestCacheDifferentialConcurrent runs the same model comparison with
// concurrent workers and shuffled key spaces, checking only aggregate
// invariants (gauge vs actual usage, capacity never exceeded after settle).
func TestCacheDifferentialConcurrent(t *testing.T) {
	const (
		workers  = 8
		keys     = 64
		capacity = 16
		ops      = 2000
	)
	c := NewBuilder[int, int](capacity).
		WithShards(2).
		WithEvictionConfig(LruConfig{HighPriorityPoolRatio: 0}).
		Build()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < ops; i++ {
				k := rng.Intn(keys)
				switch rng.Intn(4) {
				case 0:
					c.Insert(k, i).Release()
				case 1:
					if e := c.Get(k); !e.IsNil() {
						e.Release()
					}
				case 2:
					if e := c.Remove(k); !e.IsNil() {
						e.Release()
					}
				case 3:
					_ = c.Contains(k)
				}
			}
		}(int64(w))
	}
	wg.Wait()

	// Settled invariants: entries == usage (weight 1), never above capacity.
	if c.Entries() != c.Usage() {
		t.Fatalf("entries = %d, usage = %d (weight 1: must match)", c.Entries(), c.Usage())
	}
	if c.Entries() > capacity+workers {
		t.Fatalf("entries = %d exceeds capacity %d beyond in-flight tolerance", c.Entries(), capacity)
	}
	if got, want := c.Metrics().MemoryUsage.Load(), int64(c.Usage()); got != want {
		t.Fatalf("usage gauge = %d, actual usage = %d", got, want)
	}
	if got, want := c.Metrics().MemoryEntries.Load(), int64(c.Entries()); got != want {
		t.Fatalf("entries gauge = %d, actual entries = %d", got, want)
	}
}
