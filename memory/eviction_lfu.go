package memory

import (
	"container/list"

	"blob-cache/common"
)

// LfuConfig is the config of the w-TinyLFU eviction algorithm.
type LfuConfig struct {
	// WindowCapacityRatio is the `window` capacity ratio of the total cache
	// capacity. Must be in (0, 1).
	WindowCapacityRatio float64
	// ProtectedCapacityRatio is the `protected` capacity ratio of the total
	// cache capacity. Must be in (0, 1).
	//
	// Must guarantee WindowCapacityRatio + ProtectedCapacityRatio < 1.
	ProtectedCapacityRatio float64
	// CmsEps is the error of the count-min sketch.
	CmsEps float64
	// CmsConfidence is the confidence of the count-min sketch.
	CmsConfidence float64
}

// DefaultLfuConfig returns the default w-TinyLFU config, mirroring foyer.
func DefaultLfuConfig() LfuConfig {
	return LfuConfig{
		WindowCapacityRatio:    0.1,
		ProtectedCapacityRatio: 0.8,
		CmsEps:                 0.001,
		CmsConfidence:          0.9,
	}
}

func (LfuConfig) isEvictionConfig() {}

func validateLfuConfig(c LfuConfig) error {
	if c.WindowCapacityRatio <= 0.0 || c.WindowCapacityRatio >= 1.0 {
		return common.NewError(common.ErrorKindConfig, "invalid LFU config").
			WithContext("reason", "window_capacity_ratio must be in (0, 1)")
	}
	if c.ProtectedCapacityRatio <= 0.0 || c.ProtectedCapacityRatio >= 1.0 {
		return common.NewError(common.ErrorKindConfig, "invalid LFU config").
			WithContext("reason", "protected_capacity_ratio must be in (0, 1)")
	}
	if c.WindowCapacityRatio+c.ProtectedCapacityRatio >= 1.0 {
		return common.NewError(common.ErrorKindConfig, "invalid LFU config").
			WithContext("reason", "must guarantee: window_capacity_ratio + protected_capacity_ratio < 1")
	}
	return nil
}

type lfuQueue int

const (
	lfuQueueNone lfuQueue = iota
	lfuQueueWindow
	lfuQueueProbation
	lfuQueueProtected
)

// lfuState is the per-entry state of the w-TinyLFU algorithm.
type lfuState[K comparable, V any] struct {
	elem  *list.Element
	queue lfuQueue
}

// lfu is the w-TinyLFU eviction algorithm, mirroring foyer's LFU
// implementation (inspired by Caffeine):
//
//   - A new and hot entry is kept in `window`.
//   - When `window` is full, entries from it overflow to `probation`.
//   - When an entry in `probation` is accessed, it is promoted to
//     `protected`.
//   - When `protected` is full, entries from it overflow to `probation`.
//   - When evicting, the entry with a lower estimated frequency from
//     `window` or `probation` is evicted first, then from `protected`.
type lfu[K comparable, V any] struct {
	window    *list.List
	probation *list.List
	protected *list.List

	windowWeight     int
	probationWeight  int
	protectedWeight  int
	windowCap        int
	protectedCap     int

	frequencies *countMinSketch
	step        int
	decay       int

	config LfuConfig
}

func newLfu[K comparable, V any](capacity int, cfg EvictionConfig) *lfu[K, V] {
	if cfg == nil {
		cfg = DefaultLfuConfig()
	}
	c, ok := cfg.(LfuConfig)
	if !ok {
		c = DefaultLfuConfig()
	}
	if err := validateLfuConfig(c); err != nil {
		panic(err)
	}
	f := &lfu[K, V]{
		window:    list.New(),
		probation: list.New(),
		protected: list.New(),
		config:    c,
	}
	numHashes := suggestNumHashes(c.CmsConfidence)
	numBuckets := suggestNumBuckets(c.CmsEps)
	f.frequencies = newCountMinSketch(numHashes, numBuckets)
	f.decay = numBuckets
	f.applyCapacity(capacity)
	return f
}

func (f *lfu[K, V]) applyCapacity(capacity int) {
	f.windowCap = int(float64(capacity) * f.config.WindowCapacityRatio)
	f.protectedCap = int(float64(capacity) * f.config.ProtectedCapacityRatio)
}

func (f *lfu[K, V]) update(capacity int, cfg EvictionConfig) error {
	if cfg != nil {
		c, ok := cfg.(LfuConfig)
		if !ok {
			return common.NewError(common.ErrorKindConfig, "update LFU config failed").
				WithContext("reason", "config type mismatch for LFU")
		}
		if err := validateLfuConfig(c); err != nil {
			return err
		}
		f.config = c
	}
	f.applyCapacity(capacity)
	return nil
}

func (f *lfu[K, V]) updateFrequencies(hash uint64) {
	f.frequencies.update(hash)
	f.step++
	if f.step >= f.decay {
		f.step >>= 1
		f.frequencies.halve()
	}
}

func (f *lfu[K, V]) push(r *record[K, V]) {
	r.setInEviction(true)
	st := &lfuState[K, V]{queue: lfuQueueWindow}
	r.state = st

	f.windowWeight += r.weight
	f.updateFrequencies(r.hash)
	st.elem = f.window.PushBack(r)

	// Overflow the window to the probation queue if needed.
	for f.windowWeight > f.windowCap {
		e := f.window.Front()
		if e == nil {
			return
		}
		f.window.Remove(e)
		w := e.Value.(*record[K, V])
		ws := w.state.(*lfuState[K, V])
		f.windowWeight -= w.weight
		ws.queue = lfuQueueProbation
		f.probationWeight += w.weight
		ws.elem = f.probation.PushBack(w)
	}
}

func (f *lfu[K, V]) pop() *record[K, V] {
	// Compare the frequency of the front of `window` and `probation` and
	// evict the lower one. If both are empty, evict from `protected`.
	var victim *record[K, V]
	fw := f.window.Front()
	fp := f.probation.Front()
	switch {
	case fw == nil && fp == nil:
		if f.protected.Len() == 0 {
			return nil
		}
		e := f.protected.Front()
		f.protected.Remove(e)
		victim = e.Value.(*record[K, V])
	case fw == nil:
		f.probation.Remove(fp)
		victim = fp.Value.(*record[K, V])
	case fp == nil:
		f.window.Remove(fw)
		victim = fw.Value.(*record[K, V])
	default:
		w := fw.Value.(*record[K, V])
		p := fp.Value.(*record[K, V])
		if f.frequencies.estimate(w.hash) < f.frequencies.estimate(p.hash) {
			f.window.Remove(fw)
			victim = w
		} else {
			f.probation.Remove(fp)
			victim = p
		}
	}

	st := victim.state.(*lfuState[K, V])
	f.removeQueueWeight(st.queue, victim.weight)
	st.queue = lfuQueueNone
	st.elem = nil
	victim.state = nil
	victim.setInEviction(false)
	return victim
}

func (f *lfu[K, V]) remove(r *record[K, V]) {
	st := r.state.(*lfuState[K, V])
	switch st.queue {
	case lfuQueueWindow:
		f.window.Remove(st.elem)
	case lfuQueueProbation:
		f.probation.Remove(st.elem)
	case lfuQueueProtected:
		f.protected.Remove(st.elem)
	}
	f.removeQueueWeight(st.queue, r.weight)
	st.queue = lfuQueueNone
	st.elem = nil
	r.state = nil
	r.setInEviction(false)
}

func (f *lfu[K, V]) clear() {
	for f.pop() != nil {
	}
}

func (f *lfu[K, V]) removeQueueWeight(q lfuQueue, weight int) {
	switch q {
	case lfuQueueWindow:
		f.windowWeight -= weight
	case lfuQueueProbation:
		f.probationWeight -= weight
	case lfuQueueProtected:
		f.protectedWeight -= weight
	}
}

func (f *lfu[K, V]) acquire(r *record[K, V]) {
	// Update frequency by access.
	f.updateFrequencies(r.hash)

	if !r.isInEviction() {
		return
	}
	st := r.state.(*lfuState[K, V])

	switch st.queue {
	case lfuQueueWindow:
		// Move to the MRU position of the window.
		f.window.Remove(st.elem)
		st.elem = f.window.PushBack(r)
	case lfuQueueProbation:
		// Promote to the MRU position of the protected queue.
		f.probation.Remove(st.elem)
		f.probationWeight -= r.weight
		st.queue = lfuQueueProtected
		f.protectedWeight += r.weight
		st.elem = f.protected.PushBack(r)

		// Overflow the protected queue to the probation queue if needed.
		for f.protectedWeight > f.protectedCap {
			e := f.protected.Front()
			if e == nil {
				return
			}
			f.protected.Remove(e)
			w := e.Value.(*record[K, V])
			ws := w.state.(*lfuState[K, V])
			f.protectedWeight -= w.weight
			ws.queue = lfuQueueProbation
			f.probationWeight += w.weight
			ws.elem = f.probation.PushBack(w)
		}
	case lfuQueueProtected:
		// Move to the MRU position of the protected queue.
		f.protected.Remove(st.elem)
		st.elem = f.protected.PushBack(r)
	}
}

func (f *lfu[K, V]) release(_ *record[K, V]) {}

func (f *lfu[K, V]) acquireKind() opKind { return opMutable }
func (f *lfu[K, V]) releaseKind() opKind { return opNoop }
