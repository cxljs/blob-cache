package memory

import "sync"

// Pipe is the unbounded queue used to offload evicted in-memory entries to
// the disk cache. It is the counterpart of foyer's `ArcPipe` (built on an
// unbounded async queue): Send never blocks, so eviction is never stalled
// by a slow disk cache.
type Pipe[K comparable, V any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []Piece[K, V]
	closed bool
}

// NewPipe creates an enabled pipe.
func NewPipe[K comparable, V any]() *Pipe[K, V] {
	p := &Pipe[K, V]{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Send appends a piece to the pipe. It never blocks.
func (p *Pipe[K, V]) Send(piece Piece[K, V]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.queue = append(p.queue, piece)
	p.cond.Broadcast()
}

// TryRecv pops one piece from the pipe, if any.
func (p *Pipe[K, V]) TryRecv() (Piece[K, V], bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return Piece[K, V]{}, false
	}
	piece := p.queue[0]
	p.queue = p.queue[1:]
	return piece, true
}

// PopAll drains all queued pieces and returns them. The returned slice is
// owned by the caller.
func (p *Pipe[K, V]) PopAll() []Piece[K, V] {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return nil
	}
	out := p.queue
	p.queue = nil
	return out
}

// Len returns the number of queued pieces.
func (p *Pipe[K, V]) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue)
}

// Close marks the pipe as closed. Queued pieces remain readable.
func (p *Pipe[K, V]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
}

// IsClosed reports whether the pipe is closed.
func (p *Pipe[K, V]) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
