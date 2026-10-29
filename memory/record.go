package memory

import (
	"sync/atomic"

	"blob-cache/common"
)

// Record flags, mirroring foyer's `Flags` bitflags.
const (
	flagInIndexer  uint32 = 1 << 0
	flagInEviction uint32 = 1 << 1
)

// record holds the information of one cached entry, the counterpart of
// foyer's `Record`. It is shared between the indexer, the eviction
// container and all outstanding cache entries (via reference counting).
type record[K comparable, V any] struct {
	key    K
	value  V
	props  common.Properties
	hash   uint64
	weight int

	// state is owned by the eviction container. Each eviction algorithm
	// stores its per-entry bookkeeping here (list links, frequency bits...).
	state any

	refs  atomic.Int64
	flags atomic.Uint32
}

func newRecord[K comparable, V any](key K, value V, props common.Properties, hash uint64, weight int) *record[K, V] {
	return &record[K, V]{key: key, value: value, props: props, hash: hash, weight: weight}
}

func (r *record[K, V]) keyOf() K                      { return r.key }
func (r *record[K, V]) valueOf() V                    { return r.value }
func (r *record[K, V]) properties() common.Properties { return r.props }
func (r *record[K, V]) hashOf() uint64                { return r.hash }
func (r *record[K, V]) weightOf() int                 { return r.weight }

func (r *record[K, V]) incRefs(n int64) { r.refs.Add(n) }

// decRefs decreases the reference count and returns the value after the
// decrement. A result of zero means the caller released the last external
// reference.
func (r *record[K, V]) decRefs(n int64) int64 { return r.refs.Add(-n) }

func (r *record[K, V]) setFlag(flag uint32, on bool) {
	for {
		old := r.flags.Load()
		var next uint32
		if on {
			next = old | flag
		} else {
			next = old &^ flag
		}
		if old == next || r.flags.CompareAndSwap(old, next) {
			return
		}
	}
}

func (r *record[K, V]) isInIndexer() bool  { return r.flags.Load()&flagInIndexer != 0 }
func (r *record[K, V]) isInEviction() bool { return r.flags.Load()&flagInEviction != 0 }

func (r *record[K, V]) setInIndexer(on bool)  { r.setFlag(flagInIndexer, on) }
func (r *record[K, V]) setInEviction(on bool) { r.setFlag(flagInEviction, on) }

func (r *record[K, V]) isPhantom() bool { return r.props.Phantom }

// garbage is a record that left the cache together with the reason.
type garbage[K comparable, V any] struct {
	event  common.Event
	record *record[K, V]
}
