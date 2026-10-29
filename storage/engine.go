package storage

import (
	"context"

	"blob-cache/common"
	"blob-cache/memory"
)

// LoadKind classifies the result of a disk cache load.
type LoadKind int

const (
	// LoadMiss means the entry is not in the disk cache.
	LoadMiss LoadKind = iota
	// LoadEntry means an entry was loaded from the disk cache.
	LoadEntry
	// LoadThrottled means the load was throttled by the io throttle.
	LoadThrottled
)

// Load is the result of a disk cache load, mirroring foyer's `Load` enum.
type Load[K comparable, V any] struct {
	Kind LoadKind

	// Key and Value are valid when Kind is LoadEntry.
	Key   K
	Value V
	// Age is the age of the loaded entry, used by the hybrid cache.
	Age common.Age
}

// IsMiss reports whether the load missed.
func (l Load[K, V]) IsMiss() bool { return l.Kind == LoadMiss }

// IsThrottled reports whether the load was throttled.
func (l Load[K, V]) IsThrottled() bool { return l.Kind == LoadThrottled }

// RecoverMode controls how the engine recovers from corrupted blob files.
type RecoverMode int

const (
	// RecoverWary recovers best-effort: scanning stops at the first
	// corruption of each blob, valid entries are kept. This is the
	// default (zero) mode.
	RecoverWary RecoverMode = iota
	// RecoverStrict fails the open if any corruption is detected.
	RecoverStrict
	// RecoverQuiet recovers best-effort and skips broken files silently.
	RecoverQuiet
)

// StorageFilterResult is the admission decision of a storage filter.
type StorageFilterResult int

const (
	// FilterAdmitted means the entry is admitted into the disk cache.
	FilterAdmitted StorageFilterResult = iota
	// FilterRejected means the entry is rejected.
	FilterRejected
)

// IsAdmitted reports whether the entry was admitted.
func (r StorageFilterResult) IsAdmitted() bool { return r == FilterAdmitted }

// StorageFilter decides whether an entry is admitted into the disk cache,
// mirroring foyer's `StorageFilter`.
type StorageFilter interface {
	Filter(hash uint64, estimatedSize int) StorageFilterResult
}

// AdmitAll admits every entry.
type AdmitAll struct{}

// Filter implements StorageFilter.
func (AdmitAll) Filter(uint64, int) StorageFilterResult { return FilterAdmitted }

// RejectAll rejects every entry.
type RejectAll struct{}

// Filter implements StorageFilter.
func (RejectAll) Filter(uint64, int) StorageFilterResult { return FilterRejected }

// EstimatedSize rejects entries larger than the given size.
type EstimatedSize struct {
	Max int
}

// Filter implements StorageFilter.
func (f EstimatedSize) Filter(_ uint64, estimatedSize int) StorageFilterResult {
	if estimatedSize > f.Max {
		return FilterRejected
	}
	return FilterAdmitted
}

// Engine is the disk cache engine abstraction, mirroring foyer's `Engine`
// trait.
type Engine[K comparable, V any] interface {
	// Filter reports whether the entry with the given hash and estimated
	// size can be admitted.
	Filter(hash uint64, estimatedSize int) StorageFilterResult
	// Enqueue pushes a piece into the write queue. The piece is flushed
	// asynchronously.
	Enqueue(piece memory.Piece[K, V], estimatedSize int)
	// Load loads the cached entry with the given hash.
	Load(ctx context.Context, hash uint64) (Load[K, V], error)
	// Delete removes the cached entry with the given hash from the index.
	// The on-disk space is reclaimed later by the reclaim loop.
	Delete(hash uint64)
	// MayContains reports whether the engine may contain an entry with the
	// given hash. It may report false positives on hash collisions.
	MayContains(hash uint64) bool
	// Wait waits until all queued submissions are flushed.
	Wait(ctx context.Context)
	// Close flushes everything and stops the engine gracefully.
	Close(ctx context.Context) error
	// Destroy closes the engine and removes all data.
	Destroy(ctx context.Context) error
	// Stats returns the I/O statistics of the engine.
	Stats() *Statistics
}
