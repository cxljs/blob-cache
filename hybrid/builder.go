package hybrid

import (
	"blob-cache/common"
	"blob-cache/memory"
	"blob-cache/storage"
)

// Builder builds a HybridCache, mirroring foyer's `HybridCacheBuilder`.
type Builder[K comparable, V any] struct {
	name         string
	capacity     int
	weighter     memory.Weighter[K, V]
	eventListene common.EventListener[K, V]
	memoryOpts   []func(*memory.Builder[K, V])

	storageEnabled bool
	dir            string
	storeOpts      []func(*storage.StoreBuilder[K, V])

	flushOnClose bool
}

// NewBuilder creates a hybrid cache builder with the given in-memory
// capacity (in weight units).
func NewBuilder[K comparable, V any](capacity int) *Builder[K, V] {
	return &Builder[K, V]{
		name:         "blob-cache",
		capacity:     capacity,
		flushOnClose: true,
	}
}

// WithName sets the instance name, used as the metrics prefix.
func (b *Builder[K, V]) WithName(name string) *Builder[K, V] {
	b.name = name
	return b
}

// WithWeighter sets the entry weighter shared by both layers.
func (b *Builder[K, V]) WithWeighter(w memory.Weighter[K, V]) *Builder[K, V] {
	b.weighter = w
	return b
}

// WithEventListener installs an event listener observing entries leaving
// the in-memory cache.
func (b *Builder[K, V]) WithEventListener(l common.EventListener[K, V]) *Builder[K, V] {
	b.eventListene = l
	return b
}

// WithFlushOnClose sets whether all in-memory cached entries are flushed
// into the disk cache on close. Default: true.
func (b *Builder[K, V]) WithFlushOnClose(flushOnClose bool) *Builder[K, V] {
	b.flushOnClose = flushOnClose
	return b
}

// WithMemoryOptions applies extra options to the in-memory cache builder
// (shards, eviction config, ...).
func (b *Builder[K, V]) WithMemoryOptions(opts ...func(*memory.Builder[K, V])) *Builder[K, V] {
	b.memoryOpts = append(b.memoryOpts, opts...)
	return b
}

// WithStorage enables the disk (blob) cache layer in the given directory
// and applies extra options to the store builder (codec, blob file size,
// capacity, filter, ...).
func (b *Builder[K, V]) WithStorage(dir string, opts ...func(*storage.StoreBuilder[K, V])) *Builder[K, V] {
	b.storageEnabled = true
	b.dir = dir
	b.storeOpts = append(b.storeOpts, opts...)
	return b
}

// Build creates the hybrid cache.
func (b *Builder[K, V]) Build() (*HybridCache[K, V], error) {
	metrics := common.NewMetrics(b.name)

	mb := memory.NewBuilder[K, V](b.capacity).
		WithName(b.name).
		WithMetrics(metrics)
	if b.weighter != nil {
		mb = mb.WithWeighter(b.weighter)
	}
	if b.eventListene != nil {
		mb = mb.WithEventListener(b.eventListene)
	}
	for _, opt := range b.memoryOpts {
		opt(mb)
	}
	mc := mb.Build()

	var store *storage.Store[K, V]
	if b.storageEnabled {
		sb := storage.NewStoreBuilder[K, V](b.dir).WithMetrics(metrics)
		for _, opt := range b.storeOpts {
			opt(sb)
		}
		var err error
		store, err = sb.Build()
		if err != nil {
			return nil, err
		}
	}

	return newHybridCache[K, V](mc, store, b.weighter, metrics, b.flushOnClose), nil
}
