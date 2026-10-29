package memory

import "blob-cache/common"

// Piece is a ref-counted view of a key-value entry flowing from the
// in-memory cache to the disk cache, the counterpart of foyer's `Piece`.
//
// Unlike foyer, the Go rewrite does not need explicit reference counting on
// pieces: the garbage collector keeps the underlying record alive as long
// as any piece or cache entry references it.
type Piece[K comparable, V any] struct {
	rec *record[K, V]
}

// NewPiece creates a piece from a record. It is used by the storage layer
// and the hybrid cache; external callers normally receive pieces from
// pipes or eviction callbacks.
func NewPiece[K comparable, V any](key K, value V, props common.Properties, hash uint64, weight int) Piece[K, V] {
	return Piece[K, V]{rec: newRecord(key, value, props, hash, weight)}
}

func (p Piece[K, V]) valid() bool { return p.rec != nil }

// Key returns the piece key.
func (p Piece[K, V]) Key() K { return p.rec.key }

// Value returns the piece value.
func (p Piece[K, V]) Value() V { return p.rec.value }

// Properties returns the piece properties.
func (p Piece[K, V]) Properties() common.Properties { return p.rec.props }

// Hash returns the piece hash.
func (p Piece[K, V]) Hash() uint64 { return p.rec.hash }

// Weight returns the piece weight.
func (p Piece[K, V]) Weight() int { return p.rec.weight }
