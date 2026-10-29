package storage

import (
	"sync"
	"sync/atomic"

	"blob-cache/memory"
)

// PieceRef is a reference to an in-memory cache piece sitting in the disk
// cache write queue, mirroring foyer's `PieceRef`. It tracks whether it is
// still the current keeper registration for its key, so that superseded
// pieces do not remove newer ones when flushed.
type PieceRef[K comparable, V any] struct {
	Piece memory.Piece[K, V]

	current *atomic.Bool
}

// IsCurrent reports whether this reference owns the current keeper
// registration.
func (r *PieceRef[K, V]) IsCurrent() bool { return r.current.Load() }

// keeperShard is one shard of the keeper.
type keeperShard[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*pendingEntry[K, V]
}

// pendingEntry is one registration of a pending piece.
type pendingEntry[K comparable, V any] struct {
	current *atomic.Bool
	piece   memory.Piece[K, V]
}

// Keeper holds the pieces recently enqueued into the disk cache write
// queue, so that loads can hit them before they are flushed. It is the
// counterpart of foyer's `Keeper`.
type Keeper[K comparable, V any] struct {
	shards []*keeperShard[K, V]
}

// NewKeeper creates a keeper with the given shard count.
func NewKeeper[K comparable, V any](shards int) *Keeper[K, V] {
	if shards <= 0 {
		shards = 8
	}
	k := &Keeper[K, V]{shards: make([]*keeperShard[K, V], shards)}
	for i := range k.shards {
		k.shards[i] = &keeperShard[K, V]{m: make(map[K]*pendingEntry[K, V])}
	}
	return k
}

func (k *Keeper[K, V]) shard(hash uint64) *keeperShard[K, V] {
	return k.shards[hash%uint64(len(k.shards))]
}

// Insert registers a piece, superseding any previous registration for the
// same key, and returns the piece reference handed to the engine.
func (k *Keeper[K, V]) Insert(piece memory.Piece[K, V]) *PieceRef[K, V] {
	sh := k.shard(piece.Hash())
	current := new(atomic.Bool)
	current.Store(true)

	sh.mu.Lock()
	if old, ok := sh.m[piece.Key()]; ok {
		old.current.Store(false)
	}
	sh.m[piece.Key()] = &pendingEntry[K, V]{current: current, piece: piece}
	sh.mu.Unlock()

	return &PieceRef[K, V]{Piece: piece, current: current}
}

// Get returns the pending piece for the key, if any.
func (k *Keeper[K, V]) Get(hash uint64, key K) (memory.Piece[K, V], bool) {
	sh := k.shard(hash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	p, ok := sh.m[key]
	if !ok {
		return memory.Piece[K, V]{}, false
	}
	return p.piece, true
}

// Contains reports whether the keeper holds a pending piece for the key.
func (k *Keeper[K, V]) Contains(hash uint64, key K) bool {
	sh := k.shard(hash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, ok := sh.m[key]
	return ok
}

// Remove drops the registration owned by the given piece reference.
func (k *Keeper[K, V]) Remove(ref *PieceRef[K, V]) {
	if !ref.current.Load() {
		return
	}
	sh := k.shard(ref.Piece.Hash())
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if p, ok := sh.m[ref.Piece.Key()]; ok && p.current == ref.current {
		delete(sh.m, ref.Piece.Key())
	}
}
