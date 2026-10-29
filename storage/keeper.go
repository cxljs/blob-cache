package storage

import (
	"sync"

	"blob-cache/memory"
)

// PieceRef is a reference to an in-memory cache piece sitting in the disk
// cache write queue, mirroring foyer's `PieceRef`.
//
// The keeper holds exactly one PieceRef per pending key: a newer
// registration for the same key replaces the older one in the shard map,
// so pointer identity decides whether a flushed piece may remove the
// registration (a superseded reference can no longer be the map entry).
type PieceRef[K comparable, V any] struct {
	Piece memory.Piece[K, V]

	keeper *Keeper[K, V]
}

// IsCurrent reports whether this reference is still the current keeper
// registration for its key.
func (r *PieceRef[K, V]) IsCurrent() bool {
	sh := r.keeper.shard(r.Piece.Hash())
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[r.Piece.Key()] == r
}

// Remove drops the keeper registration owned by this reference. It is
// called by the engine after the piece has been flushed to the device.
func (r *PieceRef[K, V]) Remove() {
	sh := r.keeper.shard(r.Piece.Hash())
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.m[r.Piece.Key()] == r {
		delete(sh.m, r.Piece.Key())
	}
}

// keeperShard is one shard of the keeper.
type keeperShard[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*PieceRef[K, V]
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
		k.shards[i] = &keeperShard[K, V]{m: make(map[K]*PieceRef[K, V])}
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

	sh.mu.Lock()
	ref := &PieceRef[K, V]{Piece: piece, keeper: k}
	sh.m[piece.Key()] = ref
	sh.mu.Unlock()

	return ref
}

// Get returns the pending piece for the key, if any.
func (k *Keeper[K, V]) Get(hash uint64, key K) (memory.Piece[K, V], bool) {
	sh := k.shard(hash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	ref, ok := sh.m[key]
	if !ok {
		return memory.Piece[K, V]{}, false
	}
	return ref.Piece, true
}

// Contains reports whether the keeper holds a pending piece for the key.
func (k *Keeper[K, V]) Contains(hash uint64, key K) bool {
	sh := k.shard(hash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, ok := sh.m[key]
	return ok
}
