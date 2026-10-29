package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrFetch(t *testing.T) {
	c := newTestCache(t, 10)
	e, err := c.GetOrFetch("a", func() (int, error) { return 42, nil })
	if err != nil || e.Value() != 42 {
		t.Fatalf("getOrFetch = %v, %v", e.Value(), err)
	}
	// Second call must hit the cache without running the fetch.
	e2, err := c.GetOrFetch("a", func() (int, error) {
		t.Error("fetch should not run on cache hit")
		return 0, nil
	})
	if err != nil || e2.Value() != 42 {
		t.Fatalf("second getOrFetch = %v, %v", e2.Value(), err)
	}
}

func TestGetOrFetchError(t *testing.T) {
	c := newTestCache(t, 10)
	_, err := c.GetOrFetch("a", func() (int, error) { return 0, fmt.Errorf("boom") })
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v, want boom", err)
	}
	if c.Contains("a") {
		t.Fatal("failed fetch must not insert")
	}
	// Retry after failure works.
	e, err := c.GetOrFetch("a", func() (int, error) { return 7, nil })
	if err != nil || e.Value() != 7 {
		t.Fatalf("retry = %v, %v", e.Value(), err)
	}
}

func TestGetOrFetchDedup(t *testing.T) {
	c := newTestCache(t, 10)
	var runs atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			e, err := c.GetOrFetch("k", func() (int, error) {
				runs.Add(1)
				time.Sleep(5 * time.Millisecond)
				return 99, nil
			})
			if err != nil || e.Value() != 99 {
				t.Errorf("getOrFetch = %v, %v", e.Value(), err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := runs.Load(); got != 1 {
		t.Fatalf("fetch ran %d times, want 1", got)
	}
}

// TestInflightLateFollower verifies that a follower registering after the
// leader completed (but before the call was unregistered) still receives
// the entry instead of an empty result.
func TestInflightLateFollower(t *testing.T) {
	c := newTestCache(t, 10)
	m := newInflightManager[string, int]()
	call, isLeader := m.register("k")
	if !isLeader {
		t.Fatal("expected to be the leader")
	}

	entry := c.Insert("k", 42)
	call.complete(entry, nil)

	// A follower that arrives after completion.
	ch := call.follow()
	select {
	case e := <-ch:
		if e.Value() != 42 {
			t.Fatalf("late follower value = %d, want 42", e.Value())
		}
		e.Release()
	default:
		t.Fatal("late follower received nothing")
	}

	// A follower that arrived before completion with an error gets the
	// error, not an entry.
	call2, isLeader := m.register("e")
	if !isLeader {
		t.Fatal("expected to be the leader")
	}
	ch2 := call2.follow()
	call2.complete(CacheEntry[string, int]{}, fmt.Errorf("boom"))
	<-call2.done
	select {
	case e := <-ch2:
		t.Fatalf("error call delivered an entry: %v", e.Value())
	default:
	}
	m.unregister("k", call)
	m.unregister("e", call2)
	entry.Release()
}

func TestGetOrFetchPhantom(t *testing.T) {
	p := NewPipe[string, int]()
	c := newTestCache(t, 10, func(b *Builder[string, int]) {
		b.WithFilter(func(key string, _ int) bool { return key != "denied" })
	})
	c.SetPipe(p)

	e, err := c.GetOrFetch("denied", func() (int, error) { return 5, nil })
	if err != nil || e.Value() != 5 {
		t.Fatalf("phantom fetch = %v, %v", e.Value(), err)
	}
	if c.Contains("denied") {
		t.Fatal("filtered value must not be inserted")
	}
	// The followers of the fetch receive the phantom entry too.
	e.Release()
}
