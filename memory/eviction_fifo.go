package memory

import "container/list"

// fifo is the FIFO eviction algorithm. It evicts the earliest inserted
// entry regardless of access patterns. Acquire and release are noops, so
// reads only need a shared lock.
type fifo[K comparable, V any] struct {
	q *list.List // of *record[K, V]
}

func newFifo[K comparable, V any](_ int, _ EvictionConfig) *fifo[K, V] {
	return &fifo[K, V]{q: list.New()}
}

func (f *fifo[K, V]) update(_ int, _ EvictionConfig) error { return nil }

func (f *fifo[K, V]) push(r *record[K, V]) {
	r.setInEviction(true)
	r.state = f.q.PushBack(r)
}

func (f *fifo[K, V]) pop() *record[K, V] {
	e := f.q.Front()
	if e == nil {
		return nil
	}
	f.q.Remove(e)
	r := e.Value.(*record[K, V])
	r.state = nil
	r.setInEviction(false)
	return r
}

func (f *fifo[K, V]) remove(r *record[K, V]) {
	e := r.state.(*list.Element)
	f.q.Remove(e)
	r.state = nil
	r.setInEviction(false)
}

func (f *fifo[K, V]) clear() {
	for f.pop() != nil {
	}
}

func (f *fifo[K, V]) acquire(_ *record[K, V]) {}

func (f *fifo[K, V]) release(_ *record[K, V]) {}

func (f *fifo[K, V]) acquireKind() opKind { return opNoop }
func (f *fifo[K, V]) releaseKind() opKind { return opNoop }
