package memory

import (
	"fmt"
	"math/rand"
	"testing"

	"blob-cache/common"
)

// randomEvictionConfig picks a random eviction algorithm and config.
func randomEvictionConfig(r *rand.Rand) EvictionConfig {
	switch r.Intn(5) {
	case 0:
		return FifoConfig{}
	case 1:
		return LruConfig{HighPriorityPoolRatio: r.Float64()}
	case 2:
		return LfuConfig{
			WindowCapacityRatio:    0.1,
			ProtectedCapacityRatio: 0.8,
			CmsEps:                 0.01,
			CmsConfidence:          0.9,
		}
	case 3:
		return S3FifoConfig{
			SmallQueueCapacityRatio:  0.1 + r.Float64()*0.5,
			GhostQueueCapacityRatio:  1.0,
			SmallToMainFreqThreshold: 1,
		}
	default:
		return SieveConfig{}
	}
}

// TestMemoryDifferentialModel runs a randomized operation sequence against
// the cache and continuously checks policy-agnostic invariants that must
// hold for every eviction algorithm:
//
//  1. a hit always returns the most recently inserted value for the key
//     (never a stale version);
//  2. Contains(key) agrees with Get(key);
//  3. after Remove(key) the key is absent;
//  4. after Clear/EvictAll every key is absent and the gauges are zero;
//  5. the usage/entries gauges always equal Usage()/Entries();
//  6. with unit weights, Usage() and Entries() never exceed the capacity.
//
// The eviction *victim* choice is policy-specific and is NOT modeled.
func TestMemoryDifferentialModel(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4, 5, 6, 7, 8} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runMemoryModel(t, seed)
		})
	}
}

func runMemoryModel(t *testing.T, seed int64) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	c := NewBuilder[int, int](64).
		WithShards(1 + r.Intn(8)).
		WithEvictionConfig(randomEvictionConfig(r)).
		Build()

	// latest[k] = the value the most recent Insert/fetch stored for k.
	latest := make(map[int]int)
	nextValue := 0

	checkInvariants := func(where string) {
		t.Helper()
		if got, want := c.Metrics().MemoryUsage.Load(), int64(c.Usage()); got != want {
			t.Fatalf("%s: usage gauge %d != usage %d", where, got, want)
		}
		if got, want := c.Metrics().MemoryEntries.Load(), int64(c.Entries()); got != want {
			t.Fatalf("%s: entries gauge %d != entries %d", where, got, want)
		}
		if u, e := c.Usage(), c.Entries(); u > 64 || e > 64 {
			t.Fatalf("%s: usage %d / entries %d exceed capacity 64", where, u, e)
		}
	}

	for op := 0; op < 4000; op++ {
		k := r.Intn(96)
		switch r.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39:
			// Insert.
			nextValue++
			v := nextValue
			var props common.Properties
			if r.Intn(10) == 0 {
				props = props.WithHint(common.HintLow)
			}
			e := c.InsertWithProperties(k, v, props)
			if e.IsNil() || e.Value() != v {
				t.Fatalf("op %d: insert returned wrong entry", op)
			}
			e.Release()
			latest[k] = v
		case 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54:
			// Get: hits must return the latest value.
			e := c.Get(k)
			if !e.IsNil() {
				if v, ok := latest[k]; !ok || e.Value() != v {
					t.Fatalf("op %d: stale value for key %d: got %d, want %d", op, k, e.Value(), latest[k])
				}
				e.Release()
			}
			if e.IsNil() == c.Contains(k) {
				t.Fatalf("op %d: Contains/Get disagree for key %d", op, k)
			}
		case 55, 56, 57, 58, 59, 60, 61, 62, 63, 64:
			// Remove.
			e := c.Remove(k)
			if !e.IsNil() {
				if v, ok := latest[k]; !ok || e.Value() != v {
					t.Fatalf("op %d: removed stale value for key %d", op, k)
				}
				e.Release()
			}
			delete(latest, k)
			if c.Contains(k) || !c.Get(k).IsNil() {
				t.Fatalf("op %d: key %d still present after remove", op, k)
			}
		case 65, 66, 67, 68, 69:
			// GetOrFetch: fetch stores the latest value; hits must be fresh.
			nextValue++
			v := nextValue
			ran := false
			e, err := c.GetOrFetch(k, func() (int, error) {
				ran = true
				return v, nil
			})
			if err != nil {
				t.Fatalf("op %d: getOrFetch error: %v", op, err)
			}
			if ran {
				latest[k] = v
			}
			if !e.IsNil() {
				if w, ok := latest[k]; !ok || e.Value() != w {
					t.Fatalf("op %d: stale getOrFetch value for key %d", op, k)
				}
				e.Release()
			}
		case 70, 71, 72, 73, 74:
			// Clear.
			c.Clear()
			latest = make(map[int]int)
			if c.Entries() != 0 || c.Usage() != 0 {
				t.Fatalf("op %d: cache not empty after clear", op)
			}
			if c.Metrics().MemoryUsage.Load() != 0 || c.Metrics().MemoryEntries.Load() != 0 {
				t.Fatalf("op %d: gauges not zero after clear", op)
			}
		case 75, 76, 77, 78, 79:
			// EvictAll: everything leaves the cache.
			c.EvictAll()
			latest = make(map[int]int)
			if c.Entries() != 0 || c.Usage() != 0 {
				t.Fatalf("op %d: cache not empty after evictAll", op)
			}
		case 80, 81, 82, 83, 84:
			// Resize to a random capacity (>= 1 keeps the unit-weight
			// capacity invariant meaningful).
			if err := c.Resize(1 + r.Intn(64)); err != nil {
				t.Fatalf("op %d: resize: %v", op, err)
			}
		case 85, 86, 87, 88, 89:
			// Touch / Contains sanity.
			c.Touch(k)
			_ = c.Contains(k)
		default:
			// Clustered re-access of a hot key (drives the eviction
			// acquire/release hooks).
			hot := k % 8
			e := c.Get(hot)
			if !e.IsNil() {
				if v, ok := latest[hot]; !ok || e.Value() != v {
					t.Fatalf("op %d: stale hot value for key %d", op, hot)
				}
				e.Release()
			}
		}
		checkInvariants(fmt.Sprintf("op %d", op))
	}
}
