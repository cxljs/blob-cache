// Package hybrid provides the hybrid cache of blob-cache: an in-memory
// cache backed by a disk (blob) cache behind a single API. It is the
// counterpart of the foyer crate's hybrid cache.
package hybrid

import (
	"context"

	"blob-cache/common"
	"blob-cache/memory"
	"blob-cache/storage"
)

// HybridCacheEntry is a referenced entry of the hybrid cache. It is the
// in-memory cache entry; the source reports where the value came from.
type HybridCacheEntry[K comparable, V any] = memory.CacheEntry[K, V]

// HybridCachePolicy controls when fetched/inserted entries are written to
// the disk cache. Only write-on-insertion is supported, mirroring foyer's
// default policy.
type HybridCachePolicy int

const (
	// PolicyWriteOnInsertion writes entries to the disk cache when they
	// are inserted or fetched.
	PolicyWriteOnInsertion HybridCachePolicy = iota
)

// HybridCache is the hybrid cache with an in-memory layer and an optional
// disk (blob) layer, mirroring foyer's `HybridCache`.
//
// When the disk layer is disabled, the hybrid cache behaves as an
// in-memory cache with hybrid-compatible APIs.
type HybridCache[K comparable, V any] struct {
	memory   *memory.Cache[K, V]
	store    *storage.Store[K, V] // nil when the disk layer is disabled
	pipe     *memory.Pipe[K, V]
	weighter memory.Weighter[K, V]
	metrics  *common.Metrics
	policy   HybridCachePolicy
	// flushOnClose offloads all in-memory entries into the disk cache on
	// Close.
	flushOnClose bool


	done    chan struct{}
}

// newHybridCache wires the layers together and starts the pipe pump that
// offloads evicted in-memory entries into the disk cache.
func newHybridCache[K comparable, V any](
	mc *memory.Cache[K, V],
	store *storage.Store[K, V],
	weighter memory.Weighter[K, V],
	metrics *common.Metrics,
	flushOnClose bool,
) *HybridCache[K, V] {
	c := &HybridCache[K, V]{
		memory:       mc,
		store:        store,
		weighter:     weighter,
		metrics:      metrics,
		policy:       PolicyWriteOnInsertion,
		flushOnClose: flushOnClose,

		done:         make(chan struct{}),
	}
	if store != nil {
		c.pipe = memory.NewPipe[K, V]()
		mc.SetPipe(c.pipe)
		go c.pipePump()
	}
	return c
}

// pipePump moves evicted in-memory pieces into the disk cache write queue.
func (c *HybridCache[K, V]) pipePump() {
	defer close(c.done)
	for {
		pieces := c.pipe.PopAllWait()
		for _, p := range pieces {
			c.store.Enqueue(p, false)
		}
		if c.pipe.IsClosed() {
			// Drain anything sent concurrently with the close.
			for _, p := range c.pipe.PopAll() {
				c.store.Enqueue(p, false)
			}
			return
		}
	}
}

// Memory returns the in-memory cache of the hybrid cache.
func (c *HybridCache[K, V]) Memory() *memory.Cache[K, V] { return c.memory }

// Storage returns the disk cache of the hybrid cache, or nil when the disk
// layer is disabled.
func (c *HybridCache[K, V]) Storage() *storage.Store[K, V] { return c.store }

// IsHybrid reports whether the disk layer is enabled.
func (c *HybridCache[K, V]) IsHybrid() bool { return c.store != nil }

// Metrics returns the metrics of the hybrid cache.
func (c *HybridCache[K, V]) Metrics() *common.Metrics { return c.metrics }

// Name returns the instance name.
func (c *HybridCache[K, V]) Name() string { return c.memory.Name() }

// Insert inserts an entry into the hybrid cache and, with the disk layer
// enabled, into the disk cache write queue.
func (c *HybridCache[K, V]) Insert(key K, value V) HybridCacheEntry[K, V] {
	entry := c.memory.Insert(key, value)
	c.insertToStorage(entry)
	return entry
}

// InsertWithProperties inserts an entry with per-entry properties. Entries
// located in-mem-only are not written to the disk cache; entries located
// on-disk-only are not cached in memory.
func (c *HybridCache[K, V]) InsertWithProperties(key K, value V, props common.Properties) HybridCacheEntry[K, V] {
	entry := c.memory.InsertWithProperties(key, value, props)
	c.insertToStorage(entry)
	return entry
}

func (c *HybridCache[K, V]) insertToStorage(entry memory.CacheEntry[K, V]) {
	if c.store == nil || c.policy != PolicyWriteOnInsertion {
		return
	}
	location := entry.Properties().Location
	if location == common.LocationInMem {
		return
	}
	c.store.Enqueue(entry.Piece(), location == common.LocationOnDisk)
}

// Get returns the cached entry with the given key, promoting disk cache
// hits into the in-memory cache.
func (c *HybridCache[K, V]) Get(ctx context.Context, key K) (HybridCacheEntry[K, V], error) {
	if entry := c.memory.Get(key); !entry.IsNil() {
		c.metrics.HybridHit.Inc()
		return entry, nil
	}
	if c.store != nil {
		load, err := c.store.Load(ctx, key)
		if err != nil {
			c.metrics.HybridError.Inc()
			return HybridCacheEntry[K, V]{}, err
		}
		if load.Kind == storage.LoadEntry {
			c.metrics.HybridHit.Inc()
			props := common.Properties{}.WithAge(load.Age)
			// Promote the disk entry into memory with the in-memory hash so
			// that it shards correctly.
			piece := memory.NewPiece[K, V](key, load.Value, props, c.memory.Hash(key), c.weight(key, load.Value))
			return c.memory.InsertPiece(piece), nil
		}
	}
	c.metrics.HybridMiss.Inc()
	return HybridCacheEntry[K, V]{}, nil
}

// GetOrFetch returns the cached entry with the given key, loading it from
// the disk cache or fetching it with the given function on a full miss.
// Concurrent calls for the same key are deduplicated.
//
// Fetched entries are inserted into the in-memory cache and written into
// the disk cache write queue.
func (c *HybridCache[K, V]) GetOrFetch(ctx context.Context, key K, fetch func() (V, error)) (HybridCacheEntry[K, V], error) {
	return c.GetOrFetchWithProperties(ctx, key, fetch, common.Properties{})
}

// GetOrFetchWithProperties is GetOrFetch with per-entry properties applied
// to the entry created by the fetch.
func (c *HybridCache[K, V]) GetOrFetchWithProperties(ctx context.Context, key K, fetch func() (V, error), props common.Properties) (HybridCacheEntry[K, V], error) {
	if entry := c.memory.Get(key); !entry.IsNil() {
		c.metrics.HybridHit.Inc()
		return entry, nil
	}

	fromDisk := false
	fetchClosure := func() (V, error) {
		if c.store != nil {
			load, err := c.store.Load(ctx, key)
			if err != nil {
				var zero V
				return zero, err
			}
			if load.Kind == storage.LoadEntry {
				fromDisk = true
				return load.Value, nil
			}
		}
		return fetch()
	}

	entry, err := c.memory.GetOrFetchWithProperties(key, fetchClosure, props)
	if err != nil {
		c.metrics.HybridError.Inc()
		return HybridCacheEntry[K, V]{}, err
	}

	// The in-memory cache handles the per-key deduplication, so the fetch
	// closure ran at most once: write the fetched value into the disk
	// cache from here.
	if c.store != nil && !fromDisk && props.Location != common.LocationInMem {
		piece := memory.NewPiece[K, V](key, entry.Value(), props.WithAge(common.AgeFresh), 0, c.weight(key, entry.Value()))
		c.store.Enqueue(piece, props.Location == common.LocationOnDisk)
	}
	c.metrics.HybridHit.Inc()
	return entry, nil
}

func (c *HybridCache[K, V]) weight(key K, value V) int {
	if c.weighter == nil {
		return 1
	}
	return c.weighter(key, value)
}

// Remove removes the cached entry with the given key from both layers.
func (c *HybridCache[K, V]) Remove(key K) {
	c.memory.Remove(key)
	if c.store != nil {
		c.store.Delete(key)
	}
}

// Contains reports whether the hybrid cache contains the given key,
// covering both layers. It may report false positives on hash collisions
// in the disk cache.
func (c *HybridCache[K, V]) Contains(key K) bool {
	if c.memory.Contains(key) {
		return true
	}
	if c.store != nil {
		return c.store.MayContains(key)
	}
	return false
}

// Clear removes all entries from the in-memory cache and destroys all
// disk cache data, mirroring foyer's clear.
func (c *HybridCache[K, V]) Clear(ctx context.Context) error {
	c.memory.Clear()
	if c.store != nil {
		return c.store.Destroy(ctx)
	}
	return nil
}

// Stats returns the I/O statistics of the disk cache layer, or nil when
// the disk layer is disabled.
func (c *HybridCache[K, V]) Stats() *storage.Statistics {
	if c.store == nil {
		return nil
	}
	return c.store.Stats()
}

// Wait waits until all entries queued for the disk cache are flushed.
// It is a no-op when the disk layer is disabled.
func (c *HybridCache[K, V]) Wait(ctx context.Context) {
	if c.store != nil {
		c.store.Wait(ctx)
	}
}

// Close gracefully closes the hybrid cache: with flush-on-close enabled,
// all in-memory entries are first offloaded into the disk cache through
// the pipe; then the pipe pump stops and all pending disk writes are
// flushed.
func (c *HybridCache[K, V]) Close(ctx context.Context) error {
	if c.pipe != nil {
		if c.flushOnClose {
			// Evicting everything routes all entries through the pipe into
			// the disk cache write queue.
			if err := c.memory.Resize(0); err != nil {
				return err
			}
		}
		c.pipe.Close()
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if c.store != nil {
		return c.store.Close(ctx)
	}
	return nil
}
