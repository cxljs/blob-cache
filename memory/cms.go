package memory

import (
	"math"
)

// countMinSketch is a compact frequency estimator, the counterpart of the
// count-min sketch used by foyer's w-TinyLFU implementation. Values are
// uint16 counters; frequencies saturate instead of overflowing.
type countMinSketch struct {
	numHashes  int
	numBuckets int
	buckets    []uint16 // numHashes * numBuckets
}

// suggestNumBuckets returns the sketch width for the given error bound
// (standard count-min sketch: w = ceil(2 / eps)).
func suggestNumBuckets(eps float64) int {
	if eps <= 0 {
		panic("blob-cache: cms eps must be > 0")
	}
	return int(math.Ceil(2.0 / eps))
}

// suggestNumHashes returns the sketch depth for the given confidence
// (d = ceil(ln(1 / (1 - confidence)))).
func suggestNumHashes(confidence float64) int {
	if confidence <= 0 || confidence >= 1 {
		panic("blob-cache: cms confidence must be in (0, 1)")
	}
	return int(math.Ceil(math.Log(1.0 / (1.0 - confidence))))
}

func newCountMinSketch(numHashes, numBuckets int) *countMinSketch {
	return &countMinSketch{
		numHashes:  numHashes,
		numBuckets: numBuckets,
		buckets:    make([]uint16, numHashes*numBuckets),
	}
}

// hashAt computes the bucket index of key in row i.
//
// The key is already a 64-bit hash from the cache hasher; row indices are
// derived with the splitmix64 finalizer, which is far cheaper than hashing
// the key again per row.
func (c *countMinSketch) hashAt(key uint64, i int) int {
	return int(mix64(key+uint64(i)*0x9E3779B97F4A7C15) % uint64(c.numBuckets))
}

func mix64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// update increments the estimated frequency of the key by one.
func (c *countMinSketch) update(key uint64) {
	for i := 0; i < c.numHashes; i++ {
		idx := i*c.numBuckets + c.hashAt(key, i)
		if v := &c.buckets[idx]; *v < math.MaxUint16 {
			*v++
		}
	}
}

// estimate returns the estimated frequency of the key.
func (c *countMinSketch) estimate(key uint64) uint16 {
	minV := uint16(math.MaxUint16)
	for i := 0; i < c.numHashes; i++ {
		v := c.buckets[i*c.numBuckets+c.hashAt(key, i)]
		if v < minV {
			minV = v
		}
	}
	return minV
}

// halve halves all counters, aging out old frequencies.
func (c *countMinSketch) halve() {
	for i := range c.buckets {
		c.buckets[i] >>= 1
	}
}
