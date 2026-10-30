package hybrid

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"blob-cache/storage"
)

// TestHybridGetOrFetchPanicDoesNotLeakInflight verifies that a panicking
// user fetch propagates the panic but does not leak the in-flight entry:
// later GetOrFetch calls for the same key must work again.
func TestHybridGetOrFetchPanicDoesNotLeakInflight(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](1<<20).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec())
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	func() {
		defer func() { _ = recover() }() // the panic must propagate to the caller
		_, _ = c.GetOrFetch(ctx, "k", func() ([]byte, error) {
			panic("boom")
		})
		t.Fatal("panic should propagate to the caller")
	}()

	// The in-flight entry must have been unregistered: a healthy fetch
	// works afterwards.
	e, err := c.GetOrFetch(ctx, "k", func() ([]byte, error) {
		return []byte("ok"), nil
	})
	if err != nil {
		t.Fatalf("getOrFetch after panic = %v", err)
	}
	if string(e.Value()) != "ok" {
		t.Fatalf("value = %q, want ok", e.Value())
	}
}

// TestHybridGetPanicDoesNotLeakInflight verifies that a panicking codec /
// weighter / listener during a Get (disk-hit promotion) does not leak the
// in-flight entry: later Gets of the same key must work again.
func TestHybridGetPanicDoesNotLeakInflight(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	var panicOn atomic.Bool
	c, err := NewBuilder[string, []byte](128).WithWeighter(func(key string, value []byte) int {
		if panicOn.Load() {
			panic("weighter boom")
		}
		return 1
	}).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec())
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	// Insert "k" and evict it to disk (before arming the panic).
	c.Insert("k", []byte("v"))
	for i := 0; i < 500; i++ {
		e := c.Insert(fmt.Sprintf("fill-%d", i), make([]byte, 512))
		e.Release()
	}
	waitForDisk(t, c, "k", true)
	c.Wait(ctx)

	// A Get that panics inside the promotion path must propagate the panic
	// without leaking the in-flight entry.
	panicOn.Store(true)
	func() {
		defer func() { _ = recover() }()
		_, _ = c.Get(ctx, "k")
	}()
	panicOn.Store(false)

	// The key must be gettable again (no hang, no error).
	e, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get after panic = %v", err)
	}
	if e.IsNil() || string(e.Value()) != "v" {
		t.Fatalf("get after panic = %v", e)
	}
}
