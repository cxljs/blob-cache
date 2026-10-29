package common

// Hasher64 is a stateless 64-bit hash function over byte slices.
//
// The implementation must be deterministic across processes: the disk cache
// engine persists hashes in its on-disk index.
type Hasher64 interface {
	Hash64(data []byte) uint64
}

// HashBuilder creates hashers. The stateless method is the one-shot form of
// the streaming hashers it creates.
type HashBuilder interface {
	Hasher64
	New() Hasher64Streaming
}

// xxHashBuilder is the default HashBuilder using XXH64 with seed 0.
type xxHashBuilder struct{}

// DefaultHashBuilder is the default hash builder (xxHash64, seed 0), the
// counterpart of foyer's `DefaultHasher` (XxHash64).
var DefaultHashBuilder HashBuilder = xxHashBuilder{}

func (xxHashBuilder) Hash64(data []byte) uint64 { return Sum64XXH64(data, 0) }
func (xxHashBuilder) New() Hasher64Streaming    { return NewXXH64(0) }
