package common

import (
	"fmt"
	"sort"
	"sync/atomic"
)

// Counter is a monotonically-adjustable atomic counter used for cache
// metrics. It is a lightweight stand-in for foyer's prometheus-backed
// metrics registry.
type Counter struct {
	v    atomic.Int64
	name string
}

// Add adjusts the counter by delta.
func (c *Counter) Add(delta int64) { c.v.Add(delta) }

// Inc increases the counter by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Dec decreases the counter by one.
func (c *Counter) Dec() { c.v.Add(-1) }

// Load returns the current counter value.
func (c *Counter) Load() int64 { return c.v.Load() }

// Metrics holds all counters of a cache instance, grouped by layer. Field
// groups mirror foyer's metrics: `memory_*` for the in-memory cache,
// `storage_*` for the disk cache and `hybrid_*` for the hybrid cache.
type Metrics struct {
	Name string

	// In-memory cache metrics.
	MemoryInsert  Counter
	MemoryReplace Counter
	MemoryRemove  Counter
	MemoryEvict   Counter
	MemoryHit     Counter
	MemoryMiss    Counter
	MemoryUsage   Counter
	MemoryEntries Counter

	// Disk cache metrics.
	StorageEnqueue     Counter
	StorageEnqueueFail Counter
	StorageHit         Counter
	StorageMiss        Counter
	StorageThrottled   Counter
	StorageDelete      Counter
	StorageError       Counter
	StorageReinsert    Counter

	// Hybrid cache metrics.
	HybridHit   Counter
	HybridMiss  Counter
	HybridError Counter
}

// NewMetrics creates a metrics instance tagged with the given name.
func NewMetrics(name string) *Metrics {
	return &Metrics{Name: name}
}

// Snapshot returns a sorted name -> value map of all counters.
func (m *Metrics) Snapshot() map[string]int64 {
	pairs := []struct {
		name string
		c    *Counter
	}{
		{"memory_insert", &m.MemoryInsert},
		{"memory_replace", &m.MemoryReplace},
		{"memory_remove", &m.MemoryRemove},
		{"memory_evict", &m.MemoryEvict},
		{"memory_hit", &m.MemoryHit},
		{"memory_miss", &m.MemoryMiss},
		{"memory_usage", &m.MemoryUsage},
		{"memory_entries", &m.MemoryEntries},
		{"storage_enqueue", &m.StorageEnqueue},
		{"storage_enqueue_fail", &m.StorageEnqueueFail},
		{"storage_hit", &m.StorageHit},
		{"storage_miss", &m.StorageMiss},
		{"storage_throttled", &m.StorageThrottled},
		{"storage_delete", &m.StorageDelete},
		{"storage_error", &m.StorageError},
		{"storage_reinsert", &m.StorageReinsert},
		{"hybrid_hit", &m.HybridHit},
		{"hybrid_miss", &m.HybridMiss},
		{"hybrid_error", &m.HybridError},
	}
	out := make(map[string]int64, len(pairs))
	for _, p := range pairs {
		out[fmt.Sprintf("%s_%s", m.Name, p.name)] = p.c.Load()
	}
	return out
}

// SnapshotSorted returns the snapshot as sorted key-value pairs.
func (m *Metrics) SnapshotSorted() ([]string, []int64) {
	snap := m.Snapshot()
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]int64, len(keys))
	for i, k := range keys {
		values[i] = snap[k]
	}
	return keys, values
}
