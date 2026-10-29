package memory

import (
	"math"

	"blob-cache/common"
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
func (c *countMinSketch) hashAt(key uint64, i int) int {
	h := common.Sum64XXH64([]byte{
		byte(key), byte(key >> 8), byte(key >> 16), byte(key >> 24),
		byte(key >> 32), byte(key >> 40), byte(key >> 48), byte(key >> 56),
	}, uint64(i)+1)
	return int(h % uint64(c.numBuckets))
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
