package memory

import (
	"fmt"
	"sync"

	"blob-cache/common"
)

// inflightCall is one in-progress fetch for a key, the counterpart of
// foyer's `Inflight`. The leader runs the fetch and inserts the record;
// followers receive a clone of the resulting entry (or the error).
type inflightCall[K comparable, V any] struct {
	done chan struct{}

	mu        sync.Mutex
	completed bool
	waiters   []chan CacheEntry[K, V]
	entry     CacheEntry[K, V]
	err       error
}

func newInflightCall[K comparable, V any]() *inflightCall[K, V] {
	return &inflightCall[K, V]{done: make(chan struct{})}
}

// follow registers the caller as a follower of the call and returns the
// channel delivering the entry. If the call already completed, the result
// is delivered into the channel immediately, so a follower that lost the
// race against complete() still receives it.
func (c *inflightCall[K, V]) follow() chan CacheEntry[K, V] {
	ch := make(chan CacheEntry[K, V], 1)
	c.mu.Lock()
	switch {
	case c.completed && c.err == nil:
		ch <- c.entry.Clone()
	case !c.completed:
		c.waiters = append(c.waiters, ch)
	}
	c.mu.Unlock()
	return ch
}

// complete delivers the entry to all followers and unblocks them. It must
// be called exactly once.
func (c *inflightCall[K, V]) complete(entry CacheEntry[K, V], err error) {
	c.mu.Lock()
	c.completed = true
	c.entry = entry
	c.err = err
	for _, ch := range c.waiters {
		if err == nil {
			ch <- entry.Clone()
		}
	}
	c.waiters = nil
	c.mu.Unlock()
	close(c.done)
}

// inflightManager dedups concurrent fetches for the same key within one
// shard, mirroring foyer's `InflightManager`.
type inflightManager[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*inflightCall[K, V]
}

func newInflightManager[K comparable, V any]() *inflightManager[K, V] {
	return &inflightManager[K, V]{m: make(map[K]*inflightCall[K, V])}
}

// register returns the call for the key. isLeader reports whether the
// caller must run the fetch and complete the call.
func (m *inflightManager[K, V]) register(key K) (call *inflightCall[K, V], isLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if call, ok := m.m[key]; ok {
		return call, false
	}
	call = newInflightCall[K, V]()
	m.m[key] = call
	return call, true
}

func (m *inflightManager[K, V]) unregister(key K, call *inflightCall[K, V]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m[key] == call {
		delete(m.m, key)
	}
}

// GetOrFetch returns the cached entry for the key, fetching it with the
// given function on a miss. Concurrent GetOrFetch calls for the same key
// are deduplicated: only one fetch runs and all callers receive the same
// entry, mirroring foyer's `GetOrFetch` with in-flight deduplication.
func (c *Cache[K, V]) GetOrFetch(key K, fetch func() (V, error)) (CacheEntry[K, V], error) {
	return c.getOrFetchInner(key, fetch, common.Properties{}, common.SourceOuter)
}

// GetOrFetchWithProperties is GetOrFetch with per-entry properties applied
// to the entry inserted on a fetch.
func (c *Cache[K, V]) GetOrFetchWithProperties(key K, fetch func() (V, error), props common.Properties) (CacheEntry[K, V], error) {
	return c.getOrFetchInner(key, fetch, props, common.SourceOuter)
}

func (c *Cache[K, V]) getOrFetchInner(key K, fetch func() (V, error), props common.Properties, source common.Source) (CacheEntry[K, V], error) {
	if entry := c.Get(key); !entry.IsNil() {
		return entry, nil
	}

	hash := c.hash(key)
	m := c.managerFor(hash)
	call, isLeader := m.register(key)
	if !isLeader {
		ch := call.follow()
		<-call.done
		select {
		case entry := <-ch:
			return entry, nil
		default:
			return CacheEntry[K, V]{}, call.err
		}
	}

	var entry CacheEntry[K, V]
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				call.complete(CacheEntry[K, V]{}, fmt.Errorf("blob-cache: fetch panicked: %v", p))
				m.unregister(key, call)
				panic(p)
			}
		}()
		var value V
		value, err = fetch()
		if err != nil {
			call.complete(CacheEntry[K, V]{}, err)
			return
		}
		if !c.inner.filter(key, value) {
			props.Phantom = true
		}
		if props.Location == common.LocationOnDisk {
			props.Phantom = true
		}
		rec := newRecord(key, value, props, hash, c.inner.weighter(key, value))
		entry = c.insertInner(rec, source)
		call.complete(entry, nil)
	}()

	m.unregister(key, call)
	return entry, err
}

func (c *Cache[K, V]) managerFor(hash uint64) *inflightManager[K, V] {
	return c.inner.inflights[hash%uint64(len(c.inner.inflights))]
}
