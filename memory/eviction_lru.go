package memory

import (
	"container/list"
	"math"

	"blob-cache/common"
)

// lruState is the per-entry state of the LRU algorithm.
type lruState[K comparable, V any] struct {
	// elem is the membership element in exactly one of the three lists.
	elem *list.Element
	// pinElem is the membership element in the pin list while pinned.
	pinElem *list.Element
	// inHighPriorityPool reports whether the record belongs to the high
	// priority pool.
	inHighPriorityPool bool
	// isPinned reports whether the record is pinned by an external
	// reference.
	isPinned bool
}

// lru is the LRU eviction algorithm with a high priority pool and a pin
// list, mirroring foyer's LRU:
//
//   - Records are inserted into the high priority pool or the low priority
//     list based on their hint.
//   - The high priority pool overflows its oldest records into the low
//     priority list when its weight exceeds the pool capacity.
//   - Pinned records (held by external references) are moved into the pin
//     list and cannot be evicted until released.
type lru[K comparable, V any] struct {
	highPriorityList *list.List
	lowList          *list.List
	pinList          *list.List

	// highPriorityWeight is the weight of the unpinned records currently
	// in the high priority pool.
	highPriorityWeight         int
	highPriorityWeightCapacity int

	config LruConfig
}

func newLru[K comparable, V any](capacity int, cfg EvictionConfig) *lru[K, V] {
	if cfg == nil {
		cfg = DefaultLruConfig()
	}
	c, ok := cfg.(LruConfig)
	if !ok {
		c = DefaultLruConfig()
	}
	if err := validateLruConfig(c); err != nil {
		panic(err)
	}
	l := &lru[K, V]{
		highPriorityList: list.New(),
		lowList:          list.New(),
		pinList:          list.New(),
		config:           c,
	}
	l.highPriorityWeightCapacity = l.poolCapacity(capacity)
	return l
}

func validateLruConfig(c LruConfig) error {
	if c.HighPriorityPoolRatio < 0.0 || c.HighPriorityPoolRatio > 1.0 || math.IsNaN(c.HighPriorityPoolRatio) {
		return common.NewError(common.ErrorKindConfig, "update LRU config failed").
			WithContext("reason", "high_priority_pool_ratio must be in [0.0, 1.0]")
	}
	return nil
}

func (l *lru[K, V]) poolCapacity(capacity int) int {
	return int(float64(capacity) * l.config.HighPriorityPoolRatio)
}

func (l *lru[K, V]) update(capacity int, cfg EvictionConfig) error {
	if cfg != nil {
		c, ok := cfg.(LruConfig)
		if !ok {
			return common.NewError(common.ErrorKindConfig, "update LRU config failed").
				WithContext("reason", "config type mismatch for LRU")
		}
		if err := validateLruConfig(c); err != nil {
			return err
		}
		l.config = c
	}
	l.highPriorityWeightCapacity = l.poolCapacity(capacity)
	l.mayOverflowHighPriorityPool()
	return nil
}

// mayOverflowHighPriorityPool moves the oldest high priority records into
// the low priority list while the pool weight exceeds its capacity.
func (l *lru[K, V]) mayOverflowHighPriorityPool() {
	for l.highPriorityWeight > l.highPriorityWeightCapacity {
		e := l.highPriorityList.Front()
		if e == nil {
			return
		}
		l.highPriorityList.Remove(e)
		r := e.Value.(*record[K, V])
		st := r.state.(*lruState[K, V])
		st.inHighPriorityPool = false
		st.elem = l.lowList.PushBack(r)
		l.highPriorityWeight -= r.weight
	}
}

func (l *lru[K, V]) push(r *record[K, V]) {
	r.setInEviction(true)
	st := &lruState[K, V]{}
	r.state = st

	switch r.props.Hint {
	case common.HintNormal:
		st.inHighPriorityPool = true
		l.highPriorityWeight += r.weight
		st.elem = l.highPriorityList.PushBack(r)
		l.mayOverflowHighPriorityPool()
	case common.HintLow:
		st.inHighPriorityPool = false
		st.elem = l.lowList.PushBack(r)
	}
}

func (l *lru[K, V]) pop() *record[K, V] {
	var e *list.Element
	if l.lowList.Len() > 0 {
		e = l.lowList.Front()
	} else {
		e = l.highPriorityList.Front()
	}
	if e == nil {
		return nil
	}

	if l.lowList.Len() > 0 {
		l.lowList.Remove(e)
	} else {
		l.highPriorityList.Remove(e)
	}

	r := e.Value.(*record[K, V])
	st := r.state.(*lruState[K, V])
	if st.inHighPriorityPool {
		l.highPriorityWeight -= r.weight
		st.inHighPriorityPool = false
	}
	st.elem = nil
	r.state = nil
	r.setInEviction(false)
	return r
}

func (l *lru[K, V]) remove(r *record[K, V]) {
	st := r.state.(*lruState[K, V])

	switch {
	case st.isPinned:
		l.pinList.Remove(st.pinElem)
		st.pinElem = nil
		if st.inHighPriorityPool {
			st.inHighPriorityPool = false
		}
	case st.inHighPriorityPool:
		l.highPriorityList.Remove(st.elem)
		l.highPriorityWeight -= r.weight
		st.inHighPriorityPool = false
	default:
		l.lowList.Remove(st.elem)
	}

	st.elem = nil
	st.isPinned = false
	r.state = nil
	r.setInEviction(false)
}

func (l *lru[K, V]) clear() {
	for l.pop() != nil {
	}
	// Clear the pin list to release all pinned records as well.
	for {
		e := l.pinList.Front()
		if e == nil {
			break
		}
		l.pinList.Remove(e)
		r := e.Value.(*record[K, V])
		st := r.state.(*lruState[K, V])
		if st.inHighPriorityPool {
			st.inHighPriorityPool = false
		}
		st.pinElem = nil
		st.isPinned = false
		st.elem = nil
		r.state = nil
		r.setInEviction(false)
	}
}

func (l *lru[K, V]) acquire(r *record[K, V]) {
	if !r.isInEviction() {
		return
	}
	st := r.state.(*lruState[K, V])
	if st.isPinned {
		return
	}
	// Pin the record by moving it into the pin list.
	switch {
	case st.inHighPriorityPool:
		l.highPriorityList.Remove(st.elem)
		l.highPriorityWeight -= r.weight
	default:
		l.lowList.Remove(st.elem)
	}
	st.pinElem = l.pinList.PushBack(r)
	st.isPinned = true
}

func (l *lru[K, V]) release(r *record[K, V]) {
	if !r.isInEviction() {
		return
	}
	st := r.state.(*lruState[K, V])
	if !st.isPinned {
		return
	}
	// Unpin the record by moving it back to its pool.
	l.pinList.Remove(st.pinElem)
	st.pinElem = nil
	if st.inHighPriorityPool {
		l.highPriorityWeight += r.weight
		st.elem = l.highPriorityList.PushBack(r)
		l.mayOverflowHighPriorityPool()
	} else {
		st.elem = l.lowList.PushBack(r)
	}
	st.isPinned = false
}

func (l *lru[K, V]) acquireKind() opKind { return opMutable }
func (l *lru[K, V]) releaseKind() opKind { return opMutable }
