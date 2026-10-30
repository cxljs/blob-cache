package memory

import (
	"hash/maphash"
	"runtime"
	"sync"
	"sync/atomic"

	"blob-cache/common"
)

// Weighter computes the weight of an entry, the counterpart of foyer's
// `Weighter` trait. The default weighter returns 1 for every entry, i.e.
// capacity is measured in entry count.
type Weighter[K comparable, V any] func(key K, value V) int

// Filter decides whether an entry should be admitted into the in-memory
// cache. Rejected entries become phantom entries: they are not inserted,
// but the returned cache entry still carries the value for one-shot use.
type Filter[K comparable, V any] func(key K, value V) bool

func defaultWeighter[K comparable, V any](K, V) int { return 1 }

func defaultFilter[K comparable, V any](K, V) bool { return true }

type cacheInner[K comparable, V any] struct {
	name      string
	capacity  int
	shards    int
	shardList []*shard[K, V]
	hashSeed  maphash.Seed

	weighter      Weighter[K, V]
	filter        Filter[K, V]
	eventListener common.EventListener[K, V]
	pipe          atomic.Pointer[Pipe[K, V]]
	inflights     []*InflightManager[K, V]
	// entryFinalizers attaches runtime finalizers to cache entries so
	// that forgotten entries are released lazily by the GC. It costs
	// extra allocations and GC pressure on the Get path; disabling it
	// makes Release mandatory for every acquired entry.
	entryFinalizers bool

	metrics *common.Metrics
}

// Cache is the in-memory cache, the counterpart of foyer's `Cache`.
//
// A Cache is safe for concurrent use. CacheEntry values hold reference
// counted entries: an entry pinned by its holder cannot be evicted until
// the last reference is released (explicitly via CacheEntry.Release, or
// lazily when the entry becomes unreachable).
type Cache[K comparable, V any] struct {
	inner *cacheInner[K, V]
}

type shard[K comparable, V any] struct {
	mu       sync.RWMutex
	capacity int
	usage    int
	entries  int

	indexer map[K]*record[K, V]
	ev      eviction[K, V]

	metrics *common.Metrics
}

// shardCapacityFor distributes the total capacity over shards, giving the
// first `total % shards` shards one extra unit, mirroring foyer.
func shardCapacityFor(total, shards, index int) int {
	base := total / shards
	if index < total%shards {
		base++
	}
	return base
}

func newShard[K comparable, V any](capacity int, cfg EvictionConfig, metrics *common.Metrics) *shard[K, V] {
	s := &shard[K, V]{
		capacity: capacity,
		indexer:  make(map[K]*record[K, V]),
		metrics:  metrics,
	}
	s.ev = newEviction[K, V](capacity, cfg)
	return s
}

// newEviction builds the eviction container for the given config. The
// default is w-TinyLFU (LFU), mirroring foyer.
func newEviction[K comparable, V any](capacity int, cfg EvictionConfig) eviction[K, V] {
	switch c := cfg.(type) {
	case FifoConfig:
		return newFifo[K, V](capacity, c)
	case LruConfig:
		return newLru[K, V](capacity, c)
	case LfuConfig:
		return newLfu[K, V](capacity, c)
	case S3FifoConfig:
		return newS3fifo[K, V](capacity, c)
	case SieveConfig:
		return newSieve[K, V](capacity, c)
	case nil:
		return newLfu[K, V](capacity, DefaultLfuConfig())
	default:
		return newLfu[K, V](capacity, DefaultLfuConfig())
	}
}

// Builder builds a Cache, mirroring foyer's `CacheBuilder`.
type Builder[K comparable, V any] struct {
	capacity        int
	shards          int
	evictionConfig  EvictionConfig
	weighter        Weighter[K, V]
	filter          Filter[K, V]
	eventListener   common.EventListener[K, V]
	name            string
	metrics         *common.Metrics
	entryFinalizers bool
}

// NewBuilder creates a cache builder with the given total capacity.
func NewBuilder[K comparable, V any](capacity int) *Builder[K, V] {
	return &Builder[K, V]{
		capacity:        capacity,
		name:            "blob-cache",
		entryFinalizers: true,
	}
}

// WithName sets the instance name, used as the metrics prefix.
func (b *Builder[K, V]) WithName(name string) *Builder[K, V] {
	b.name = name
	return b
}

// WithShards sets the shard count. Entries are distributed to shards by
// hash; operations on different shards are parallelized. Defaults to the
// CPU count clamped to [1, 32].
func (b *Builder[K, V]) WithShards(shards int) *Builder[K, V] {
	b.shards = shards
	return b
}

// WithEvictionConfig sets the eviction algorithm config. Defaults to LRU
// with the default config.
func (b *Builder[K, V]) WithEvictionConfig(cfg EvictionConfig) *Builder[K, V] {
	b.evictionConfig = cfg
	return b
}

// WithWeighter sets the entry weighter.
func (b *Builder[K, V]) WithWeighter(w Weighter[K, V]) *Builder[K, V] {
	b.weighter = w
	return b
}

// WithFilter sets the admission filter.
func (b *Builder[K, V]) WithFilter(f Filter[K, V]) *Builder[K, V] {
	b.filter = f
	return b
}

// WithEventListener installs an event listener observing entries leaving
// the cache.
func (b *Builder[K, V]) WithEventListener(l common.EventListener[K, V]) *Builder[K, V] {
	b.eventListener = l
	return b
}

// WithMetrics installs a shared metrics instance (used by the hybrid cache).
func (b *Builder[K, V]) WithMetrics(m *common.Metrics) *Builder[K, V] {
	b.metrics = m
	return b
}

// WithEntryFinalizers sets whether cache entries get runtime finalizers as
// a safety net for callers that forget to release them. Default: true.
// Disabling it removes the finalizer cost from the Get path (~15% with
// GC pressure) but makes CacheEntry.Release mandatory.
func (b *Builder[K, V]) WithEntryFinalizers(enabled bool) *Builder[K, V] {
	b.entryFinalizers = enabled
	return b
}

// Build creates the cache.
func (b *Builder[K, V]) Build() *Cache[K, V] {
	if b.capacity < 0 {
		panic("blob-cache: capacity must be >= 0")
	}
	shards := b.shards
	if shards <= 0 {
		shards = runtime.NumCPU()
		if shards > 32 {
			shards = 32
		}
		if shards < 1 {
			shards = 1
		}
	}
	metrics := b.metrics
	if metrics == nil {
		metrics = common.NewMetrics(b.name)
	}
	weighter := b.weighter
	if weighter == nil {
		weighter = defaultWeighter[K, V]
	}
	filter := b.filter
	if filter == nil {
		filter = defaultFilter[K, V]
	}

	c := &Cache[K, V]{inner: &cacheInner[K, V]{
		name:            b.name,
		capacity:        b.capacity,
		shards:          shards,
		weighter:        weighter,
		filter:          filter,
		eventListener:   b.eventListener,
		metrics:         metrics,
		entryFinalizers: b.entryFinalizers,
	}}
	c.inner.hashSeed = maphash.MakeSeed()

	shardList := make([]*shard[K, V], shards)
	for i := range shardList {
		shardList[i] = newShard[K, V](shardCapacityFor(b.capacity, shards, i), b.evictionConfig, metrics)
	}
	c.inner.shardList = shardList

	inflights := make([]*InflightManager[K, V], shards)
	for i := range inflights {
		inflights[i] = NewInflightManager[K, V]()
	}
	c.inner.inflights = inflights
	return c
}

// Name returns the instance name.
func (c *Cache[K, V]) Name() string { return c.inner.name }

// Shards returns the shard count.
func (c *Cache[K, V]) Shards() int { return c.inner.shards }

// Metrics returns the metrics of the cache.
func (c *Cache[K, V]) Metrics() *common.Metrics { return c.inner.metrics }

// Capacity returns the total capacity in weight units.
func (c *Cache[K, V]) Capacity() int { return c.inner.capacity }

func (c *Cache[K, V]) shardFor(hash uint64) *shard[K, V] {
	return c.inner.shardList[hash%uint64(c.inner.shards)]
}

// setPipe attaches the offload pipe used by the hybrid cache.
func (c *Cache[K, V]) SetPipe(p *Pipe[K, V]) { c.inner.pipe.Store(p) }

// Pipe returns the attached pipe, or nil.
func (c *Cache[K, V]) Pipe() *Pipe[K, V] { return c.inner.pipe.Load() }

func (c *Cache[K, V]) piped() bool { return c.inner.pipe.Load() != nil }

// Hash returns the in-memory hash of the key. It uses a per-process seed
// (hash/maphash) and is only meaningful within this cache instance, e.g.
// to build pieces for InsertPiece.
func (c *Cache[K, V]) Hash(key K) uint64 {
	return maphash.Comparable(c.inner.hashSeed, key)
}

func (c *Cache[K, V]) hash(key K) uint64 {
	return maphash.Comparable(c.inner.hashSeed, key)
}

// resize updates the total capacity and redistributes it over shards,
// evicting overflow entries like foyer's resize.
func (c *Cache[K, V]) Resize(capacity int) error {
	c.inner.capacity = capacity
	var garbages []garbage[K, V]
	for i, s := range c.inner.shardList {
		shardCap := shardCapacityFor(capacity, c.inner.shards, i)
		s.mu.Lock()
		s.capacity = shardCap
		err := s.ev.update(shardCap, nil)
		if err == nil {
			s.evict(shardCap, &garbages)
		}
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	c.fireGarbages(garbages)
	return nil
}

// Insert inserts an entry into the cache, returning a referenced cache
// entry.
func (c *Cache[K, V]) Insert(key K, value V) CacheEntry[K, V] {
	return c.InsertWithProperties(key, value, common.Properties{})
}

// InsertWithProperties inserts an entry with per-entry properties.
func (c *Cache[K, V]) InsertWithProperties(key K, value V, props common.Properties) CacheEntry[K, V] {
	hash := c.hash(key)
	weight := c.inner.weighter(key, value)
	if !c.inner.filter(key, value) {
		props.Phantom = true
	}
	if props.Location == common.LocationOnDisk {
		props.Phantom = true
	}
	rec := newRecord(key, value, props, hash, weight)
	return c.insertInner(rec, common.SourceOuter)
}

// InsertPiece inserts a piece (e.g. loaded from the disk cache) into the
// in-memory cache, reusing its record.
// InsertPiece inserts a piece (e.g. loaded from the disk cache) into the
// in-memory cache, reusing its record. The entry source is SourceMemory.
func (c *Cache[K, V]) InsertPiece(piece Piece[K, V]) CacheEntry[K, V] {
	return c.insertInner(piece.rec, common.SourceMemory)
}

// InsertPieceWithSource is InsertPiece with an explicit entry source, used
// by the hybrid cache to report disk-loaded entries as SourceDisk.
func (c *Cache[K, V]) InsertPieceWithSource(piece Piece[K, V], source common.Source) CacheEntry[K, V] {
	return c.insertInner(piece.rec, source)
}

// insertInner runs the emplace flow on the owning shard and fires leave
// events for the garbage records outside the lock, mirroring foyer's
// `insert_inner`.
func (c *Cache[K, V]) insertInner(rec *record[K, V], source common.Source) CacheEntry[K, V] {
	s := c.shardFor(rec.hash)

	s.mu.Lock()
	var garbages []garbage[K, V]
	s.emplace(rec, &garbages)
	s.mu.Unlock()

	c.fireGarbages(garbages)

	return newCacheEntry[K, V](c, rec, source)
}

func (c *Cache[K, V]) fireGarbages(garbages []garbage[K, V]) {
	if len(garbages) == 0 {
		return
	}
	listener := c.inner.eventListener
	p := c.Pipe()
	piped := p != nil
	for _, g := range garbages {
		if listener != nil {
			listener.OnLeave(g.event, g.record.key, g.record.value)
		}
		if piped && g.event == common.EventEvict {
			p.Send(Piece[K, V]{rec: g.record})
		}
	}
}

// emplace inserts the record into the shard, evicting overflow records.
// Must be called with the shard lock held exclusively.
func (s *shard[K, V]) emplace(rec *record[K, V], garbages *[]garbage[K, V]) {
	// Concurrent fetches can produce a record that is already resident
	// (e.g. inserted from the disk keeper): reuse it without unlinking and
	// reinserting, mirroring foyer's emplace fast path.
	if rec.isInIndexer() {
		if resident := s.indexer[rec.key]; resident == rec {
			rec.incRefs(1)
			return
		}
	}

	if rec.isPhantom() {
		if old := s.removeNoEvict(rec.key); old != nil {
			if old.isInEviction() {
				s.ev.remove(old)
			}
			s.usage -= old.weight
			s.metrics.MemoryUsage.Add(int64(-old.weight))
			s.entries--
			s.metrics.MemoryEntries.Dec()
			*garbages = append(*garbages, garbage[K, V]{event: common.EventReplace, record: old})
		}
		rec.incRefs(1)
		*garbages = append(*garbages, garbage[K, V]{event: common.EventRemove, record: rec})
		s.metrics.MemoryInsert.Inc()
		return
	}

	// Evict overflow records to make room for the new one.
	target := s.capacity - rec.weight
	if target < 0 {
		target = 0
	}
	s.evict(target, garbages)

	if old := s.indexerInsert(rec); old != nil {
		s.metrics.MemoryReplace.Inc()
		if old.isInEviction() {
			s.ev.remove(old)
		}
		s.usage -= old.weight
		s.metrics.MemoryUsage.Add(int64(-old.weight))
		*garbages = append(*garbages, garbage[K, V]{event: common.EventReplace, record: old})
	} else {
		s.metrics.MemoryInsert.Inc()
		s.entries++
		s.metrics.MemoryEntries.Inc()
	}

	s.ev.push(rec)
	s.usage += rec.weight
	s.metrics.MemoryUsage.Add(int64(rec.weight))
	rec.incRefs(1)
}

// indexerInsert puts the record into the indexer, returning the replaced
// record if any.
func (s *shard[K, V]) indexerInsert(rec *record[K, V]) *record[K, V] {
	old := s.indexer[rec.key]
	s.indexer[rec.key] = rec
	rec.setInIndexer(true)
	if old != nil {
		old.setInIndexer(false)
	}
	return old
}

// removeNoEvict removes a record from the indexer without touching the
// eviction container. Must be called with the shard lock held exclusively.
func (s *shard[K, V]) removeNoEvict(key K) *record[K, V] {
	rec := s.indexer[key]
	if rec == nil {
		return nil
	}
	delete(s.indexer, key)
	rec.setInIndexer(false)
	return rec
}

// evict evicts records until usage fits the target. Must be called with
// the shard lock held exclusively.
func (s *shard[K, V]) evict(target int, garbages *[]garbage[K, V]) {
	for s.usage > target {
		evicted := s.ev.pop()
		if evicted == nil {
			break
		}
		s.metrics.MemoryEvict.Inc()

		if old := s.removeNoEvict(evicted.key); old != evicted {
			panic("blob-cache: indexer/eviction inconsistent")
		}
		s.usage -= evicted.weight
		s.metrics.MemoryUsage.Add(int64(-evicted.weight))
		s.entries--
		s.metrics.MemoryEntries.Dec()

		*garbages = append(*garbages, garbage[K, V]{event: common.EventEvict, record: evicted})
	}
}

// Get returns the cached entry with the given key, or a zero CacheEntry if
// the key is absent.
func (c *Cache[K, V]) Get(key K) CacheEntry[K, V] {
	hash := c.hash(key)
	s := c.shardFor(hash)

	if s.ev.acquireKind() == opMutable {
		s.mu.Lock()
	} else {
		s.mu.RLock()
	}
	rec := s.indexer[key]
	if rec != nil {
		c.inner.metrics.MemoryHit.Inc()
		rec.incRefs(1)
		if s.ev.acquireKind() != opNoop {
			s.ev.acquire(rec)
		}
	}
	if s.ev.acquireKind() == opMutable {
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}
	if rec == nil {
		c.inner.metrics.MemoryMiss.Inc()
		return CacheEntry[K, V]{}
	}
	return newCacheEntry[K, V](c, rec, common.SourceMemory)
}

// Contains reports whether the cache holds an entry with the given key.
func (c *Cache[K, V]) Contains(key K) bool {
	hash := c.hash(key)
	s := c.shardFor(hash)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.indexer[key] != nil
}

// Touch reports whether the entry with the given key exists, mirroring
// foyer's touch (a get_noop): it does not pin the entry, reorder it in the
// eviction container, or bump algorithm-specific access stats.
func (c *Cache[K, V]) Touch(key K) bool {
	hash := c.hash(key)
	s := c.shardFor(hash)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.indexer[key] != nil
}

// Remove removes the entry with the given key from the cache. The removed
// entry (if any) is returned with a reference held.
func (c *Cache[K, V]) Remove(key K) CacheEntry[K, V] {
	hash := c.hash(key)
	s := c.shardFor(hash)

	s.mu.Lock()
	rec := s.removeNoEvict(key)
	if rec != nil {
		if rec.isInEviction() {
			s.ev.remove(rec)
		}
		s.usage -= rec.weight
		s.entries--
		s.metrics.MemoryRemove.Inc()
		s.metrics.MemoryUsage.Add(int64(-rec.weight))
		s.metrics.MemoryEntries.Dec()
		rec.incRefs(1)
	}
	s.mu.Unlock()

	if rec == nil {
		return CacheEntry[K, V]{}
	}
	// Notify the listener out of the lock critical section, mirroring
	// foyer's remove.
	if listener := c.inner.eventListener; listener != nil {
		listener.OnLeave(common.EventRemove, rec.key, rec.value)
	}
	return newCacheEntry[K, V](c, rec, common.SourceMemory)
}

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear() {
	var garbages []garbage[K, V]
	for _, s := range c.inner.shardList {
		s.mu.Lock()
		records := make([]*record[K, V], 0, len(s.indexer))
		for _, rec := range s.indexer {
			records = append(records, rec)
		}
		oldUsage := s.usage
		s.indexer = make(map[K]*record[K, V])
		s.ev.clear()
		s.usage = 0
		s.entries = 0
		s.mu.Unlock()

		for _, rec := range records {
			rec.setInIndexer(false)
			rec.setInEviction(false)
			garbages = append(garbages, garbage[K, V]{event: common.EventClear, record: rec})
		}
		if len(records) > 0 {
			s.metrics.MemoryRemove.Add(int64(len(records)))
			s.metrics.MemoryEntries.Add(-int64(len(records)))
		}
		s.metrics.MemoryUsage.Add(-int64(oldUsage))
	}
	c.fireGarbages(garbages)
}

// Usage returns the total weight of the cached entries.
func (c *Cache[K, V]) Usage() int {
	total := 0
	for _, s := range c.inner.shardList {
		s.mu.RLock()
		total += s.usage
		s.mu.RUnlock()
	}
	return total
}

// Entries returns the number of cached entries.
func (c *Cache[K, V]) Entries() int {
	total := 0
	for _, s := range c.inner.shardList {
		s.mu.RLock()
		total += s.entries
		s.mu.RUnlock()
	}
	return total
}

// EvictAll evicts all entries and, when a pipe is attached, offloads them
// into the disk cache.
func (c *Cache[K, V]) EvictAll() {
	var garbages []garbage[K, V]
	for _, s := range c.inner.shardList {
		s.mu.Lock()
		s.evict(0, &garbages)
		s.mu.Unlock()
	}
	c.fireGarbages(garbages)
}

// CacheEntry is a reference to a cached entry, the counterpart of foyer's
// `CacheEntry`. The underlying record cannot be evicted until the last
// reference is released, either explicitly via Release or lazily when the
// entry becomes unreachable.
type CacheEntry[K comparable, V any] struct {
	h *entryHandle[K, V]
}

type entryHandle[K comparable, V any] struct {
	c        *Cache[K, V]
	rec      *record[K, V]
	source   common.Source
	released atomic.Bool
}

func newCacheEntry[K comparable, V any](c *Cache[K, V], rec *record[K, V], source common.Source) CacheEntry[K, V] {
	h := &entryHandle[K, V]{c: c, rec: rec, source: source}
	if c.inner.entryFinalizers {
		runtime.SetFinalizer(h, (*entryHandle[K, V]).release)
	}
	return CacheEntry[K, V]{h: h}
}

// IsNil reports whether the entry is empty (cache miss result).
func (e CacheEntry[K, V]) IsNil() bool { return e.h == nil }

// Hash returns the entry hash.
func (e CacheEntry[K, V]) Hash() uint64 { return e.h.rec.hash }

// Key returns the entry key.
func (e CacheEntry[K, V]) Key() K { return e.h.rec.key }

// Value returns the entry value.
func (e CacheEntry[K, V]) Value() V { return e.h.rec.value }

// Properties returns the entry properties.
func (e CacheEntry[K, V]) Properties() common.Properties { return e.h.rec.props }

// Weight returns the entry weight.
func (e CacheEntry[K, V]) Weight() int { return e.h.rec.weight }

// Refs returns the current external reference count of the entry.
func (e CacheEntry[K, V]) Refs() int { return int(e.h.rec.refs.Load()) }

// IsOutdated reports whether the entry was evicted or replaced while still
// referenced.
func (e CacheEntry[K, V]) IsOutdated() bool { return !e.h.rec.isInIndexer() }

// Source returns where the entry value comes from.
func (e CacheEntry[K, V]) Source() common.Source { return e.h.source }

// Clone returns a new cache entry referencing the same record, increasing
// the reference count by one.
func (e CacheEntry[K, V]) Clone() CacheEntry[K, V] {
	if e.IsNil() {
		return CacheEntry[K, V]{}
	}
	e.h.rec.incRefs(1)
	h := &entryHandle[K, V]{c: e.h.c, rec: e.h.rec, source: e.h.source}
	if e.h.c.inner.entryFinalizers {
		runtime.SetFinalizer(h, (*entryHandle[K, V]).release)
	}
	return CacheEntry[K, V]{h: h}
}

// Piece returns a piece view of the entry for the storage layer.
func (e CacheEntry[K, V]) Piece() Piece[K, V] { return Piece[K, V]{rec: e.h.rec} }

// Release drops the reference held by this entry. Releasing is idempotent;
// unreleased entries are released lazily by the garbage collector.
func (e CacheEntry[K, V]) Release() {
	if e.IsNil() {
		return
	}
	e.h.release()
}

func (h *entryHandle[K, V]) release() {
	if h.released.Swap(true) {
		return
	}
	runtime.SetFinalizer(h, nil)

	rec := h.rec
	if rec.decRefs(1) != 0 {
		return
	}

	if rec.isPhantom() {
		// Phantom entries were never inserted; treat their last release as
		// an eviction, mirroring foyer.
		if listener := h.c.inner.eventListener; listener != nil {
			listener.OnLeave(common.EventEvict, rec.key, rec.value)
		}
		if p := h.c.Pipe(); p != nil {
			p.Send(Piece[K, V]{rec: rec})
		}
		return
	}

	s := h.c.shardFor(rec.hash)
	switch s.ev.releaseKind() {
	case opNoop:
	case opImmutable:
		s.mu.RLock()
		s.ev.release(rec)
		s.mu.RUnlock()
	case opMutable:
		s.mu.Lock()
		s.ev.release(rec)
		s.mu.Unlock()
	}
}
