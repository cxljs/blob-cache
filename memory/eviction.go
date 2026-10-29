package memory

import "errors"

// EvictionConfig is the sealed configuration interface implemented by all
// eviction algorithm configs. It is the counterpart of foyer's
// `EvictionConfig` enum.
type EvictionConfig interface {
	isEvictionConfig()
}

// FifoConfig is the config of the FIFO eviction algorithm.
type FifoConfig struct{}

func (FifoConfig) isEvictionConfig() {}

// LruConfig is the config of the LRU eviction algorithm.
type LruConfig struct {
	// HighPriorityPoolRatio is the ratio of the high priority pool
	// occupied. LRU guarantees the high priority weight is always as large
	// as possible but no larger than capacity * ratio.
	//
	// Must be in [0.0, 1.0].
	HighPriorityPoolRatio float64
}

// DefaultLruConfig returns the default LRU config, mirroring foyer.
func DefaultLruConfig() LruConfig { return LruConfig{HighPriorityPoolRatio: 0.9} }

func (LruConfig) isEvictionConfig() {}

var errInvalidConfig = errors.New("invalid eviction config")

// opKind classifies how an eviction algorithm performs its acquire/release
// hooks, mirroring foyer's `Op` enum. Immutable and noop hooks may run under
// a shared (read) lock; mutable hooks require an exclusive lock.
type opKind int

const (
	opNoop opKind = iota
	opImmutable
	opMutable
)

// eviction is the cache eviction algorithm abstraction, the counterpart of
// foyer's `Eviction` trait. All methods are called with the owning shard's
// lock held exclusively, except when acquireKind/releaseKind permit shared
// locking.
type eviction[K comparable, V any] interface {
	// update updates the capacity (and possibly config) of the algorithm.
	update(capacity int, config EvictionConfig) error
	// push puts a record into the container. The caller guarantees the
	// record is not held by the container yet. The container must set the
	// record's IN_EVICTION flag.
	push(r *record[K, V])
	// pop removes and returns the victim record. The container must clear
	// the record's IN_EVICTION flag.
	pop() *record[K, V]
	// remove removes a specific record held by the container. The caller
	// guarantees the record is held by the container. The container must
	// clear the record's IN_EVICTION flag.
	remove(r *record[K, V])
	// clear removes all records held by the container.
	clear()
	// acquire is called when an external caller acquires a cache entry.
	// The record may or may not be held by the container.
	acquire(r *record[K, V])
	// release is called when the last external reference of a record is
	// dropped. The record may or may not be held by the container.
	release(r *record[K, V])
	// acquireKind reports the op kind of acquire.
	acquireKind() opKind
	// releaseKind reports the op kind of release.
	releaseKind() opKind
}
