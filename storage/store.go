package storage

import (
	"context"

	"blob-cache/common"
	"blob-cache/memory"
)

// StoreConfig configures the disk cache Store.
type StoreConfig[K comparable, V any] struct {
	// Engine is the disk cache engine. Required.
	Engine Engine[K, V]
	// Codec serializes keys and values. Required.
	Codec *Codec[K, V]
	// Hasher computes stable entry hashes over encoded key bytes. Defaults
	// to common.DefaultHashBuilder (xxHash64).
	Hasher common.Hasher64
	// Filter is the admission filter applied by the store. The engine may
	// apply additional limits (e.g. the blob size). Nil admits all.
	Filter StorageFilter
	// Metrics receives store-level counters. Optional.
	Metrics *common.Metrics
}

// Store is the disk cache that serves as the storage backend of the hybrid
// cache. It is the counterpart of foyer-storage's `Store`: it hashes keys
// stably, applies admission filters, routes pieces through the keeper into
// the engine's write queue, and loads entries back.
type Store[K comparable, V any] struct {
	engine  Engine[K, V]
	codec   *Codec[K, V]
	hasher  common.Hasher64
	filter  StorageFilter
	metrics *common.Metrics
}

// NewStore creates a store over the given engine.
func NewStore[K comparable, V any](cfg StoreConfig[K, V]) (*Store[K, V], error) {
	if cfg.Engine == nil {
		return nil, common.NewError(common.ErrorKindConfig, "build store failed").
			WithContext("reason", "engine must not be nil")
	}
	if cfg.Codec == nil {
		return nil, common.NewError(common.ErrorKindConfig, "build store failed").
			WithContext("reason", "codec must not be nil")
	}
	hasher := cfg.Hasher
	if hasher == nil {
		hasher = common.DefaultHashBuilder
	}
	return &Store[K, V]{
		engine:  cfg.Engine,
		codec:   cfg.Codec,
		hasher:  hasher,
		filter:  cfg.Filter,
		metrics: cfg.Metrics,
	}, nil
}

// hashOf encodes the key and hashes it stably.
func (s *Store[K, V]) hashOf(key K) (uint64, []byte, error) {
	kb, err := s.codec.EncodeKey(key)
	if err != nil {
		return 0, nil, err
	}
	return s.hasher.Hash64(kb), kb, nil
}

// Close closes the disk cache gracefully, waiting for all ongoing flush
// and reclaim tasks to finish.
func (s *Store[K, V]) Close(ctx context.Context) error { return s.engine.Close(ctx) }

// Destroy closes the disk cache and removes all data.
func (s *Store[K, V]) Destroy(ctx context.Context) error { return s.engine.Destroy(ctx) }

// Wait waits until all queued submissions are flushed.
func (s *Store[K, V]) Wait(ctx context.Context) { s.engine.Wait(ctx) }

// Stats returns the I/O statistics of the disk cache.
func (s *Store[K, V]) Stats() *Statistics { return s.engine.Stats() }

// Filter reports whether an entry with the given hash and estimated size
// can be admitted into the disk cache.
func (s *Store[K, V]) Filter(hash uint64, estimatedSize int) StorageFilterResult {
	return s.engine.Filter(hash, estimatedSize)
}

// Enqueue pushes an in-memory cache piece into the disk cache write queue.
//
// The piece hash is recomputed from the encoded key: the in-memory cache
// hashes keys with a per-process seed, while the disk cache requires a
// process-stable hash.
func (s *Store[K, V]) Enqueue(piece memory.Piece[K, V], force bool) {
	if s.metrics != nil {
		s.metrics.StorageEnqueue.Inc()
	}

	key, value := piece.Key(), piece.Value()
	hash, kb, err := s.hashOf(key)
	if err != nil {
		if s.metrics != nil {
			s.metrics.StorageEnqueueFail.Inc()
		}
		return
	}
	vb, err := s.codec.EncodeValue(value)
	if err != nil {
		if s.metrics != nil {
			s.metrics.StorageEnqueueFail.Inc()
		}
		return
	}
	estimated := entryLen(len(kb), len(vb))

	if !force && !s.engine.Filter(hash, estimated).IsAdmitted() {
		s.deleteHash(hash)
		return
	}

	// Rebuild the piece with the stable hash.
	p := memory.NewPiece[K, V](key, value, piece.Properties(), hash, piece.Weight())
	s.engine.Enqueue(p, estimated)
}

// Load loads the cached entry with the given key from the disk cache.
func (s *Store[K, V]) Load(ctx context.Context, key K) (Load[K, V], error) {
	hash, _, err := s.hashOf(key)
	if err != nil {
		return Load[K, V]{}, err
	}
	load, err := s.engine.Load(ctx, hash)
	if err != nil {
		if s.metrics != nil {
			s.metrics.StorageError.Inc()
		}
		return Load[K, V]{}, err
	}
	switch {
	case load.Kind == LoadEntry && load.Key == key:
		if s.metrics != nil {
			s.metrics.StorageHit.Inc()
		}
		return load, nil
	case load.Kind == LoadEntry:
		// Hash collision: the stored key differs from the requested one.
		if s.metrics != nil {
			s.metrics.StorageMiss.Inc()
		}
		return Load[K, V]{Kind: LoadMiss}, nil
	case load.Kind == LoadThrottled:
		if s.metrics != nil {
			s.metrics.StorageThrottled.Inc()
		}
		return load, nil
	default:
		if s.metrics != nil {
			s.metrics.StorageMiss.Inc()
		}
		return Load[K, V]{Kind: LoadMiss}, nil
	}
}

// Delete removes the cached entry with the given key from the disk cache.
func (s *Store[K, V]) Delete(key K) {
	hash, _, err := s.hashOf(key)
	if err != nil {
		return
	}
	s.deleteHash(hash)
}

func (s *Store[K, V]) deleteHash(hash uint64) {
	if s.metrics != nil {
		s.metrics.StorageDelete.Inc()
	}
	s.engine.Delete(hash)
}

// MayContains reports whether the disk cache may contain an entry with the
// given key. Entries recently enqueued but not yet flushed also count.
// It may report false positives on hash collisions.
func (s *Store[K, V]) MayContains(key K) bool {
	hash, _, err := s.hashOf(key)
	if err != nil {
		return false
	}
	return s.engine.MayContains(hash)
}

// StoreBuilder builds a Store with a filesystem-backed blob engine,
// mirroring foyer's `StoreBuilder`.
type StoreBuilder[K comparable, V any] struct {
	dir           string
	blobFileSize  int64
	totalCapacity int64
	codec         *Codec[K, V]
	filter        StorageFilter
	picker        BlobPicker
	recoverMode   RecoverMode
	indexerShards int
	keeperShards  int
	hasher        common.Hasher64
	metrics       *common.Metrics
}

// NewStoreBuilder creates a store builder writing into dir.
func NewStoreBuilder[K comparable, V any](dir string) *StoreBuilder[K, V] {
	return &StoreBuilder[K, V]{dir: dir}
}

// WithBlobFileSize sets the capacity of each blob file. Defaults to 64 MiB.
func (b *StoreBuilder[K, V]) WithBlobFileSize(size int64) *StoreBuilder[K, V] {
	b.blobFileSize = size
	return b
}

// WithTotalCapacity sets the total device capacity in bytes.
func (b *StoreBuilder[K, V]) WithTotalCapacity(capacity int64) *StoreBuilder[K, V] {
	b.totalCapacity = capacity
	return b
}

// WithCodec sets the key/value codec. Defaults to GobCodec.
func (b *StoreBuilder[K, V]) WithCodec(codec *Codec[K, V]) *StoreBuilder[K, V] {
	b.codec = codec
	return b
}

// WithFilter sets the admission filter.
func (b *StoreBuilder[K, V]) WithFilter(filter StorageFilter) *StoreBuilder[K, V] {
	b.filter = filter
	return b
}

// WithPicker sets the reclaim blob picker. Defaults to FifoPicker.
func (b *StoreBuilder[K, V]) WithPicker(picker BlobPicker) *StoreBuilder[K, V] {
	b.picker = picker
	return b
}

// WithRecoverMode sets the recovery mode. Defaults to RecoverWary.
func (b *StoreBuilder[K, V]) WithRecoverMode(mode RecoverMode) *StoreBuilder[K, V] {
	b.recoverMode = mode
	return b
}

// WithHasher sets the stable entry hasher. Defaults to xxHash64.
func (b *StoreBuilder[K, V]) WithHasher(hasher common.Hasher64) *StoreBuilder[K, V] {
	b.hasher = hasher
	return b
}

// WithMetrics installs a shared metrics instance (used by the hybrid cache).
func (b *StoreBuilder[K, V]) WithMetrics(m *common.Metrics) *StoreBuilder[K, V] {
	b.metrics = m
	return b
}

// Build opens the device, starts the engine and returns the store.
func (b *StoreBuilder[K, V]) Build() (*Store[K, V], error) {
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: b.dir, BlobFileSize: b.blobFileSize})
	if err != nil {
		return nil, err
	}
	codec := b.codec
	if codec == nil {
		codec = GobCodec[K, V]()
	}
	engine, err := NewBlobEngine(BlobEngineConfig[K, V]{
		Device:        dev,
		Codec:         codec,
		TotalCapacity: b.totalCapacity,
		Filter:        b.filter,
		Picker:        b.picker,
		RecoverMode:   b.recoverMode,
		IndexerShards: b.indexerShards,
		KeeperShards:  b.keeperShards,
		Metrics:       b.metrics,
	})
	if err != nil {
		_ = dev.Close()
		return nil, err
	}
	return NewStore[K, V](StoreConfig[K, V]{
		Engine:  engine,
		Codec:   codec,
		Hasher:  b.hasher,
		Filter:  b.filter,
		Metrics: b.metrics,
	})
}
