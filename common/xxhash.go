// Package common provides shared primitives used by all blob-cache packages:
// errors, cache entry events and properties, and the default hasher.
package common

import "math/bits"

// Hasher64Streaming is the streaming counterpart of Hasher64: a hash whose
// state advances with every Write and can be finalized with Sum64. It is
// compatible with hash.Hash64.
type Hasher64Streaming interface {
	Write(p []byte) (int, error)
	Sum64() uint64
	Reset()
}

// XXH64 is a pure-Go implementation of the xxHash64 algorithm.
//
// It is used as the default hasher because it is fast and, unlike
// hash/maphash, stable across processes. The disk cache engine persists
// hashes in its on-disk index, so a process-stable hash is required.
type XXH64 struct {
	v1, v2, v3, v4 uint64
	total          uint64
	mem            [32]byte
	n              int
	seed           uint64
}

// Primes are package-level vars (not consts) on purpose: the seed==0
// initializer needs mod-2^64 wrapping arithmetic which Go constant
// expressions do not allow.
var (
	prime64_1 = uint64(0x9E3779B185EBCA87)
	prime64_2 = uint64(0xC2B2AE3D27D4EB4F)
	prime64_3 = uint64(0x165667B19E3779F9)
	prime64_4 = uint64(0x85EBCA77C2B2AE63)
	prime64_5 = uint64(0x27D4EB2F165667C5)
)

// NewXXH64 returns a XXH64 hasher seeded with seed.
func NewXXH64(seed uint64) *XXH64 {
	h := &XXH64{seed: seed}
	if seed != 0 {
		h.v1 = seed + prime64_1 + prime64_2
		h.v2 = seed + prime64_2
		h.v3 = seed
		h.v4 = seed - prime64_1
	} else {
		h.v1 = prime64_1 + prime64_2
		h.v2 = prime64_2
		h.v3 = 0
		h.v4 = uint64(0) - prime64_1
	}
	return h
}

// Write implements io.Writer.
func (h *XXH64) Write(p []byte) (int, error) {
	n := len(p)
	h.total += uint64(n)

	if h.n+len(p) < 32 {
		h.n += copy(h.mem[h.n:], p)
		return n, nil
	}

	if h.n > 0 {
		// Fill the internal buffer first.
		c := copy(h.mem[h.n:], p)
		h.n = 0
		p = p[c:]
		h.v1 = round64(h.v1, le64(h.mem[0:8]))
		h.v2 = round64(h.v2, le64(h.mem[8:16]))
		h.v3 = round64(h.v3, le64(h.mem[16:24]))
		h.v4 = round64(h.v4, le64(h.mem[24:32]))
	}

	for len(p) >= 32 {
		h.v1 = round64(h.v1, le64(p[0:8]))
		h.v2 = round64(h.v2, le64(p[8:16]))
		h.v3 = round64(h.v3, le64(p[16:24]))
		h.v4 = round64(h.v4, le64(p[24:32]))
		p = p[32:]
	}

	h.n = copy(h.mem[:], p)
	return n, nil
}

// Sum64 finalizes the hash and returns the 64-bit digest. The hasher can
// keep accumulating input afterwards.
func (h *XXH64) Sum64() uint64 {
	var digest uint64
	if h.total >= 32 {
		digest = bits.RotateLeft64(h.v1, 1) + bits.RotateLeft64(h.v2, 7) +
			bits.RotateLeft64(h.v3, 12) + bits.RotateLeft64(h.v4, 18)
		digest = mergeRound64(digest, h.v1)
		digest = mergeRound64(digest, h.v2)
		digest = mergeRound64(digest, h.v3)
		digest = mergeRound64(digest, h.v4)
	} else {
		digest = h.seed + prime64_5
	}

	digest += h.total

	p := h.mem[:h.n]
	for len(p) >= 8 {
		digest ^= round64(0, le64(p[:8]))
		digest = bits.RotateLeft64(digest, 27)*prime64_1 + prime64_4
		p = p[8:]
	}
	if len(p) >= 4 {
		digest ^= uint64(le32(p)) * prime64_1
		digest = bits.RotateLeft64(digest, 23)*prime64_2 + prime64_3
		p = p[4:]
	}
	for _, b := range p {
		digest ^= uint64(b) * prime64_5
		digest = bits.RotateLeft64(digest, 11) * prime64_1
	}

	digest ^= digest >> 33
	digest *= prime64_2
	digest ^= digest >> 29
	digest *= prime64_3
	digest ^= digest >> 32
	return digest
}

// Reset restores the hasher to its initial state.
func (h *XXH64) Reset() {
	*h = *NewXXH64(h.seed)
}

func round64(acc, input uint64) uint64 {
	acc += input * prime64_2
	acc = bits.RotateLeft64(acc, 31)
	acc *= prime64_1
	return acc
}

func mergeRound64(acc, val uint64) uint64 {
	val = round64(0, val)
	acc ^= val
	acc = acc*prime64_1 + prime64_4
	return acc
}

func le64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func le32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// Sum64XXH64 is the one-shot xxHash64 of data with the given seed.
func Sum64XXH64(data []byte, seed uint64) uint64 {
	h := NewXXH64(seed)
	h.Write(data)
	return h.Sum64()
}
