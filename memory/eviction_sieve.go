package memory

import (
	"container/list"
	"sync/atomic"
)

// SieveConfig is the config of the Sieve eviction algorithm.
type SieveConfig struct{}

func (SieveConfig) isEvictionConfig() {}

// sieveState is the per-entry state of the Sieve algorithm.
type sieveState[K comparable, V any] struct {
	elem *list.Element
	// visited is updated by acquire from multiple readers, hence atomic.
	visited atomic.Bool
}

// sieve is the Sieve eviction algorithm (see "SIEVE is Simpler than LRU:
// an Efficient Turn-Key Eviction Algorithm for Web Caches",
// https://junchengyang.com/publication/nsdi24-SIEVE.pdf), mirroring foyer's
// implementation.
//
// Sieve scans a FIFO queue with a hand pointer: entries marked visited get
// a second chance, unvisited entries are evicted. It is best suited for
// web workloads; it lacks scan resistance.
type sieve[K comparable, V any] struct {
	queue *list.List
	// hand points to the next candidate to examine; nil means start from
	// the queue front.
	hand *record[K, V]
}

func newSieve[K comparable, V any](_ int, _ EvictionConfig) *sieve[K, V] {
	return &sieve[K, V]{queue: list.New()}
}

func (s *sieve[K, V]) update(_ int, _ EvictionConfig) error { return nil }

func (s *sieve[K, V]) push(r *record[K, V]) {
	r.setInEviction(true)
	st := &sieveState[K, V]{}
	r.state = st
	st.elem = s.queue.PushBack(r)
}

func (s *sieve[K, V]) pop() *record[K, V] {
	var e *list.Element
	if s.hand != nil {
		e = s.hand.state.(*sieveState[K, V]).elem
	} else {
		e = s.queue.Front()
	}
	if e == nil {
		return nil
	}

	for {
		r := e.Value.(*record[K, V])
		st := r.state.(*sieveState[K, V])
		if !st.visited.Load() {
			// Evict this entry; the hand moves to its next position.
			next := e.Next()
			if next == nil {
				s.hand = nil
			} else {
				s.hand = next.Value.(*record[K, V])
			}
			s.queue.Remove(e)
			st.elem = nil
			r.state = nil
			r.setInEviction(false)
			return r
		}
		st.visited.Store(false)
		next := e.Next()
		if next == nil {
			e = s.queue.Front()
		} else {
			e = next
		}
		if e == nil {
			return nil
		}
	}
}

func (s *sieve[K, V]) remove(r *record[K, V]) {
	// Reset the hand if we are removing its current position.
	if s.hand == r {
		s.hand = nil
	}
	st := r.state.(*sieveState[K, V])
	s.queue.Remove(st.elem)
	st.elem = nil
	r.state = nil
	r.setInEviction(false)
}

func (s *sieve[K, V]) clear() {
	for s.pop() != nil {
	}
	// Any remaining pinned records are not held by the queue; reset the
	// hand to be safe.
	s.hand = nil
}

func (s *sieve[K, V]) acquire(r *record[K, V]) {
	st := r.state.(*sieveState[K, V])
	st.visited.Store(true)
}

func (s *sieve[K, V]) release(_ *record[K, V]) {}

func (s *sieve[K, V]) acquireKind() opKind { return opImmutable }
func (s *sieve[K, V]) releaseKind() opKind { return opNoop }
