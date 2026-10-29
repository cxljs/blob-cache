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

	inflights []*memory.InflightManager[K, V]
	done      chan struct{}
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
		shards := mc.Shards()
		c.inflights = make([]*memory.InflightManager[K, V], shards)
		for i := range c.inflights {
			c.inflights[i] = memory.NewInflightManager[K, V]()
		}
		go c.pipePump()
	}
	return c
}

// pipePump moves evicted in-memory pieces into the disk cache write queue.
// Entries advised to stay in memory (LocationInMem) are dropped here,
// mirroring foyer's hybrid pipe.
func (c *HybridCache[K, V]) pipePump() {
	defer close(c.done)
	for {
		pieces := c.pipe.PopAllWait()
		c.enqueuePieces(pieces)
		if c.pipe.IsClosed() {
			// Drain anything sent concurrently with the close.
			c.enqueuePieces(c.pipe.PopAll())
			return
		}
	}
}

func (c *HybridCache[K, V]) enqueuePieces(pieces []memory.Piece[K, V]) {
	for _, p := range pieces {
		if p.Properties().Location == common.LocationInMem {
			continue
		}
		c.store.Enqueue(p, false)
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
	if entry.Properties().Location == common.LocationInMem {
		return
	}
	c.store.Enqueue(entry.Piece(), false)
}

// inflightFor returns the in-flight manager for the key.
func (c *HybridCache[K, V]) inflightFor(key K) *memory.InflightManager[K, V] {
	return c.inflights[c.memory.Hash(key)%uint64(len(c.inflights))]
}

// Get returns the cached entry with the given key, promoting disk cache
// hits into the in-memory cache. Concurrent misses for the same key are
// deduplicated through the in-flight manager, mirroring foyer's get.
func (c *HybridCache[K, V]) Get(ctx context.Context, key K) (HybridCacheEntry[K, V], error) {
	if entry := c.memory.Get(key); !entry.IsNil() {
		c.metrics.HybridHit.Inc()
		return entry, nil
	}
	if c.store == nil {
		c.metrics.HybridMiss.Inc()
		return HybridCacheEntry[K, V]{}, nil
	}

	m := c.inflightFor(key)
	call, isLeader := m.Register(key)
	if !isLeader {
		ch := call.Follow()
		<-call.Done()
		select {
		case entry := <-ch:
			c.metrics.HybridHit.Inc()
			return entry, nil
		default:
			c.metrics.HybridMiss.Inc()
			return HybridCacheEntry[K, V]{}, call.Err()
		}
	}

	entry, err, hit := c.loadFromDisk(ctx, key)
	call.Complete(entry, err)
	m.Unregister(key, call)
	switch {
	case err != nil:
		c.metrics.HybridError.Inc()
		return HybridCacheEntry[K, V]{}, err
	case hit:
		c.metrics.HybridHit.Inc()
		return entry, nil
	default:
		c.metrics.HybridMiss.Inc()
		return HybridCacheEntry[K, V]{}, nil
	}
}

// loadFromDisk loads the key from the disk cache and promotes a hit into
// the in-memory cache with SourceDisk and the stored age.
func (c *HybridCache[K, V]) loadFromDisk(ctx context.Context, key K) (entry HybridCacheEntry[K, V], err error, hit bool) {
	load, err := c.store.Load(ctx, key)
	if err != nil {
		return HybridCacheEntry[K, V]{}, err, false
	}
	if load.Kind != storage.LoadEntry {
		return HybridCacheEntry[K, V]{}, nil, false
	}
	props := common.Properties{}.WithAge(load.Age)
	piece := memory.NewPiece[K, V](key, load.Value, props, c.memory.Hash(key), c.weight(key, load.Value))
	return c.memory.InsertPieceWithSource(piece, common.SourceDisk), nil, true
}

// GetOrFetch returns the cached entry with the given key, loading it from
// the disk cache or fetching it with the given function on a full miss.
// Concurrent calls for the same key are deduplicated.
//
// Disk cache load errors are transparently discarded (they fall through to
// the fetch function), mirroring foyer: only fetch errors are returned.
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
	if c.store == nil {
		// Memory-only mode: dedup and filtering are handled by the memory
		// cache itself.
		entry, err := c.memory.GetOrFetchWithProperties(key, fetch, props)
		if err != nil {
			c.metrics.HybridError.Inc()
			return HybridCacheEntry[K, V]{}, err
		}
		c.metrics.HybridHit.Inc()
		return entry, nil
	}

	m := c.inflightFor(key)
	call, isLeader := m.Register(key)

	var leaderEntry HybridCacheEntry[K, V]
	var leaderErr error
	if isLeader {
		leaderEntry, leaderErr = c.getOrFetchLeader(ctx, key, fetch, props, call)
		m.Unregister(key, call)
		if leaderErr != nil {
			c.metrics.HybridError.Inc()
			return HybridCacheEntry[K, V]{}, leaderErr
		}
		if leaderEntry.IsNil() {
			c.metrics.HybridError.Inc()
			return HybridCacheEntry[K, V]{}, call.Err()
		}
		c.metrics.HybridHit.Inc()
		return leaderEntry, nil
	}

	// Follower: wait for the leader's result.
	ch := call.Follow()
	<-call.Done()
	select {
	case entry := <-ch:
		c.metrics.HybridHit.Inc()
		return entry, nil
	default:
		c.metrics.HybridError.Inc()
		return HybridCacheEntry[K, V]{}, call.Err()
	}
}

// getOrFetchLeader runs the disk-load/fetch chain for the leader of an
// in-flight call and completes the call for the followers.
func (c *HybridCache[K, V]) getOrFetchLeader(ctx context.Context, key K, fetch func() (V, error), props common.Properties, call *memory.InflightCall[K, V]) (HybridCacheEntry[K, V], error) {
	defer func() {
		if p := recover(); p != nil {
			call.Complete(HybridCacheEntry[K, V]{}, context.Canceled)
			panic(p)
		}
	}()

	// 1. Disk cache load. Load errors are discarded: they fall through to
	// the user fetch, mirroring foyer's optional-fetch error handling.
	if c.store != nil {
		load, loadErr := c.store.Load(ctx, key)
		if loadErr == nil && load.Kind == storage.LoadEntry {
			diskProps := common.Properties{}.WithAge(load.Age)
			piece := memory.NewPiece[K, V](key, load.Value, diskProps, c.memory.Hash(key), c.weight(key, load.Value))
			entry := c.memory.InsertPieceWithSource(piece, common.SourceDisk)
			call.Complete(entry, nil)
			return entry, nil
		}
	}

	// 2. User fetch.
	value, err := fetch()
	if err != nil {
		call.Complete(HybridCacheEntry[K, V]{}, err)
		return HybridCacheEntry[K, V]{}, err
	}

	// 3. Insert into the in-memory cache and the disk cache write queue.
	// The in-flight manager deduplicates concurrent fetches, so this runs
	// once per key. The memory layer applies its own filter and the
	// LocationOnDisk phantom handling.
	entry := c.memory.InsertWithProperties(key, value, props.WithAge(common.AgeFresh))
	if c.store != nil && props.Location != common.LocationInMem {
		piece := memory.NewPiece[K, V](key, value, props.WithAge(common.AgeFresh), 0, c.weight(key, value))
		c.store.Enqueue(piece, false)
	}
	call.Complete(entry, nil)
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
