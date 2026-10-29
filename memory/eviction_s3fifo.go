package memory

import (
	"container/list"
	"sync/atomic"

	"blob-cache/common"
)

// S3FifoConfig is the config of the S3-FIFO eviction algorithm.
type S3FifoConfig struct {
	// SmallQueueCapacityRatio is the capacity ratio of the small queue.
	// Must be in (0, 1).
	SmallQueueCapacityRatio float64
	// GhostQueueCapacityRatio is the capacity ratio of the ghost queue
	// (tracked by hash and weight).
	GhostQueueCapacityRatio float64
	// SmallToMainFreqThreshold is the minimum access count for a small
	// queue entry to be promoted into the main queue.
	SmallToMainFreqThreshold uint8
}

// DefaultS3FifoConfig returns the default S3-FIFO config, mirroring foyer.
func DefaultS3FifoConfig() S3FifoConfig {
	return S3FifoConfig{
		SmallQueueCapacityRatio:  0.1,
		GhostQueueCapacityRatio:  1.0,
		SmallToMainFreqThreshold: 1,
	}
}

func (S3FifoConfig) isEvictionConfig() {}

const s3fifoMaxFrequency = 3

type s3fifoQueue int

const (
	s3fifoQueueNone s3fifoQueue = iota
	s3fifoQueueSmall
	s3fifoQueueMain
)

// s3fifoState is the per-entry state of the S3-FIFO algorithm.
type s3fifoState[K comparable, V any] struct {
	elem *list.Element
	// queue tells which queue the record is in; only valid while the
	// record is in the container.
	queue s3fifoQueue
	// frequency is a 2-bit saturated counter updated by acquire from
	// multiple readers, hence atomic.
	frequency atomic.Uint32
}

// s3fifo is the S3-FIFO eviction algorithm (see the S3-FIFO paper,
// https://dl.acm.org/doi/10.1145/3600006.3613147), mirroring foyer's
// implementation:
//
//   - Fresh entries are inserted into the small queue, or into the main
//     queue directly if their hash was recently evicted (tracked by the
//     ghost queue).
//   - When evicting, the small queue is scanned first if it is over its
//     weight capacity: entries with enough frequency are promoted to the
//     main queue, others are evicted (and recorded in the ghost queue).
//   - The main queue is scanned in FIFO order, decrementing frequencies
//     until an entry with zero frequency is found.
type s3fifo[K comparable, V any] struct {
	ghost *ghostQueue
	small *list.List
	main  *list.List

	smallWeight     int
	mainWeight      int
	smallWeightCap  int
	freqThreshold   uint8
	config          S3FifoConfig
}

func newS3fifo[K comparable, V any](capacity int, cfg EvictionConfig) *s3fifo[K, V] {
	if cfg == nil {
		cfg = DefaultS3FifoConfig()
	}
	c, ok := cfg.(S3FifoConfig)
	if !ok {
		c = DefaultS3FifoConfig()
	}
	if err := validateS3FifoConfig(c); err != nil {
		panic(err)
	}
	s := &s3fifo[K, V]{
		ghost:        newGhostQueue(int(float64(capacity) * c.GhostQueueCapacityRatio)),
		small:        list.New(),
		main:         list.New(),
		freqThreshold: min8(c.SmallToMainFreqThreshold, s3fifoMaxFrequency),
		config:       c,
	}
	s.smallWeightCap = int(float64(capacity) * c.SmallQueueCapacityRatio)
	return s
}

func validateS3FifoConfig(c S3FifoConfig) error {
	if c.SmallQueueCapacityRatio <= 0.0 || c.SmallQueueCapacityRatio >= 1.0 {
		return common.NewError(common.ErrorKindConfig, "invalid S3-FIFO config").
			WithContext("reason", "small_queue_capacity_ratio must be in (0, 1)")
	}
	return nil
}

func min8(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

func (s *s3fifo[K, V]) update(capacity int, cfg EvictionConfig) error {
	if cfg != nil {
		c, ok := cfg.(S3FifoConfig)
		if !ok {
			return common.NewError(common.ErrorKindConfig, "update S3-FIFO config failed").
				WithContext("reason", "config type mismatch for S3-FIFO")
		}
		if err := validateS3FifoConfig(c); err != nil {
			return err
		}
		s.config = c
		s.freqThreshold = min8(s.config.SmallToMainFreqThreshold, s3fifoMaxFrequency)
	}
	s.smallWeightCap = int(float64(capacity) * s.config.SmallQueueCapacityRatio)
	s.ghost.update(int(float64(capacity) * s.config.GhostQueueCapacityRatio))
	return nil
}

func (s *s3fifo[K, V]) push(r *record[K, V]) {
	st := &s3fifoState[K, V]{}
	r.state = st
	r.setInEviction(true)

	if s.ghost.contains(r.hash) {
		st.queue = s3fifoQueueMain
		s.mainWeight += r.weight
		st.elem = s.main.PushBack(r)
	} else {
		st.queue = s3fifoQueueSmall
		s.smallWeight += r.weight
		st.elem = s.small.PushBack(r)
	}
}

func (s *s3fifo[K, V]) pop() *record[K, V] {
	r := s.evict()
	if r == nil {
		return nil
	}
	r.setInEviction(false)
	return r
}

func (s *s3fifo[K, V]) evict() *record[K, V] {
	if s.smallWeight > s.smallWeightCap {
		if r := s.evictSmall(); r != nil {
			return r
		}
	}
	if r := s.evictMain(); r != nil {
		return r
	}
	return s.evictSmallForce()
}

// evictSmallForce evicts the front of the small queue unconditionally.
func (s *s3fifo[K, V]) evictSmallForce() *record[K, V] {
	e := s.small.Front()
	if e == nil {
		return nil
	}
	s.small.Remove(e)
	r := e.Value.(*record[K, V])
	st := r.state.(*s3fifoState[K, V])
	st.queue = s3fifoQueueNone
	st.frequency.Store(0)
	s.smallWeight -= r.weight
	st.elem = nil
	r.state = nil
	return r
}

// evictSmall scans the small queue from the front, promoting entries with
// enough frequency into the main queue, and evicts the first entry that
// does not.
func (s *s3fifo[K, V]) evictSmall() *record[K, V] {
	for {
		e := s.small.Front()
		if e == nil {
			return nil
		}
		s.small.Remove(e)
		r := e.Value.(*record[K, V])
		st := r.state.(*s3fifoState[K, V])
		if st.frequency.Load() >= uint32(s.freqThreshold) {
			st.queue = s3fifoQueueMain
			s.smallWeight -= r.weight
			s.mainWeight += r.weight
			st.elem = s.main.PushBack(r)
		} else {
			st.queue = s3fifoQueueNone
			st.frequency.Store(0)
			s.smallWeight -= r.weight
			s.ghost.push(r.hash, r.weight)
			st.elem = nil
			r.state = nil
			return r
		}
	}
}

// evictMain scans the main queue from the front, decrementing frequencies,
// and evicts the first entry that reaches zero frequency.
func (s *s3fifo[K, V]) evictMain() *record[K, V] {
	for {
		e := s.main.Front()
		if e == nil {
			return nil
		}
		s.main.Remove(e)
		r := e.Value.(*record[K, V])
		st := r.state.(*s3fifoState[K, V])
		if s.decFrequency(st) > 0 {
			st.elem = s.main.PushBack(r)
		} else {
			st.queue = s3fifoQueueNone
			s.mainWeight -= r.weight
			st.elem = nil
			r.state = nil
			return r
		}
	}
}

func (s *s3fifo[K, V]) decFrequency(st *s3fifoState[K, V]) uint32 {
	for {
		v := st.frequency.Load()
		if v == 0 {
			return 0
		}
		if st.frequency.CompareAndSwap(v, v-1) {
			return v - 1
		}
	}
}

func (s *s3fifo[K, V]) remove(r *record[K, V]) {
	st := r.state.(*s3fifoState[K, V])
	switch st.queue {
	case s3fifoQueueMain:
		s.main.Remove(st.elem)
		s.mainWeight -= r.weight
	case s3fifoQueueSmall:
		s.small.Remove(st.elem)
		s.smallWeight -= r.weight
	case s3fifoQueueNone:
		panic("blob-cache: s3fifo removal of a record not in the container")
	}
	st.queue = s3fifoQueueNone
	st.frequency.Store(0)
	st.elem = nil
	r.state = nil
	r.setInEviction(false)
}

func (s *s3fifo[K, V]) clear() {
	for s.pop() != nil {
	}
}

func (s *s3fifo[K, V]) acquire(r *record[K, V]) {
	st := r.state.(*s3fifoState[K, V])
	for {
		v := st.frequency.Load()
		if v >= s3fifoMaxFrequency {
			return
		}
		if st.frequency.CompareAndSwap(v, v+1) {
			return
		}
	}
}

func (s *s3fifo[K, V]) release(_ *record[K, V]) {}

func (s *s3fifo[K, V]) acquireKind() opKind { return opImmutable }
func (s *s3fifo[K, V]) releaseKind() opKind { return opNoop }

// ghostQueue tracks recently evicted hashes with a weight capacity, used
// to route re-inserted entries directly into the main queue.
type ghostQueue struct {
	counts   map[uint64]struct{}
	queue    []ghostEntry
	capacity int
	weight   int
}

type ghostEntry struct {
	hash   uint64
	weight int
}

func newGhostQueue(capacity int) *ghostQueue {
	return &ghostQueue{
		counts:   make(map[uint64]struct{}),
		queue:    nil,
		capacity: capacity,
	}
}

func (g *ghostQueue) update(capacity int) {
	g.capacity = capacity
	if g.capacity == 0 {
		return
	}
	for g.weight > g.capacity && g.weight > 0 {
		g.pop()
	}
}

func (g *ghostQueue) push(hash uint64, weight int) {
	if g.capacity == 0 {
		return
	}
	for g.weight+weight > g.capacity && g.weight > 0 {
		g.pop()
	}
	g.queue = append(g.queue, ghostEntry{hash, weight})
	g.counts[hash] = struct{}{}
	g.weight += weight
}

func (g *ghostQueue) pop() {
	if len(g.queue) == 0 {
		return
	}
	e := g.queue[0]
	g.queue = g.queue[1:]
	g.weight -= e.weight
	delete(g.counts, e.hash)
}

func (g *ghostQueue) contains(hash uint64) bool {
	_, ok := g.counts[hash]
	return ok
}
