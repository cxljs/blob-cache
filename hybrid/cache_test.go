package hybrid

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"blob-cache/common"
	"blob-cache/storage"
)

func TestHybridMemoryOnly(t *testing.T) {
	ctx := context.Background()
	c, err := NewBuilder[string, []byte](16).Build()
	if err != nil {
		t.Fatal(err)
	}
	if c.IsHybrid() {
		t.Fatal("expected memory-only hybrid cache")
	}
	c.Insert("k", []byte("v"))
	e, err := c.Get(ctx, "k")
	if err != nil || string(e.Value()) != "v" {
		t.Fatalf("get = %v, %v", e, err)
	}
	e, err = c.GetOrFetch(ctx, "missing", func() ([]byte, error) { return []byte("fetched"), nil })
	if err != nil || string(e.Value()) != "fetched" {
		t.Fatalf("getOrFetch = %v, %v", e, err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHybridInsertGet(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](16).
		WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if !c.IsHybrid() {
		t.Fatal("expected hybrid cache with disk layer")
	}

	c.Insert("k1", []byte("v1"))
	// The entry is in memory right away.
	e, err := c.Get(ctx, "k1")
	if err != nil || string(e.Value()) != "v1" {
		t.Fatalf("get k1 = %v, %v", e, err)
	}

	// Contains covers both layers.
	if !c.Contains("k1") {
		t.Fatal("expected k1 to be contained")
	}

	// Wait for the disk flush, then check the disk layer directly.
	time.Sleep(100 * time.Millisecond)
	if !c.Storage().MayContains("k1") {
		t.Fatal("expected k1 to reach the disk layer")
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHybridDiskHitPromotion(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")

	// Cache 1: insert and flush to disk.
	c1, err := NewBuilder[string, []byte](1<<30 /* big memory: nothing evicted */).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	c1.Insert("k", []byte("survives-restart"))
	time.Sleep(100 * time.Millisecond)
	if err := c1.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Cache 2: reopen the same directory; the value must come back from
	// disk and be promoted into memory.
	c2, err := NewBuilder[string, []byte](1<<30).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(ctx)

	e, err := c2.Get(ctx, "k")
	if err != nil || string(e.Value()) != "survives-restart" {
		t.Fatalf("get k after reopen = %v, %v", e, err)
	}
	if e.Source() != common.SourceDisk {
		t.Fatalf("promoted entry source = %v, want Disk", e.Source())
	}
	// After promotion the entry lives in memory.
	if !c2.Memory().Contains("k") {
		t.Fatal("expected disk hit to be promoted into memory")
	}
}

func TestHybridGetOrFetch(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](1<<20).
		WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	var runs int
	e, err := c.GetOrFetch(ctx, "k", func() ([]byte, error) {
		runs++
		return []byte("fetched"), nil
	})
	if err != nil || string(e.Value()) != "fetched" || runs != 1 {
		t.Fatalf("getOrFetch = %v, %v, runs=%d", e.Value(), err, runs)
	}

	// Second call hits the memory cache.
	e, err = c.GetOrFetch(ctx, "k", func() ([]byte, error) {
		t.Error("fetch should not run again")
		return nil, nil
	})
	if err != nil || string(e.Value()) != "fetched" {
		t.Fatalf("second getOrFetch = %v, %v", e.Value(), err)
	}

	// After the disk flush, a fresh cache over the same dir loads from
	// disk via GetOrFetch.
	time.Sleep(100 * time.Millisecond)
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}

	c2, err := NewBuilder[string, []byte](1<<20).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(ctx)
	e, err = c2.GetOrFetch(ctx, "k", func() ([]byte, error) {
		t.Error("fetch should not run when the disk cache has the entry")
		return nil, nil
	})
	if err != nil || string(e.Value()) != "fetched" {
		t.Fatalf("getOrFetch after reopen = %v, %v", e.Value(), err)
	}
}

func TestHybridGetOrFetchDedup(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](1<<20).
		WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec())
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	var runs int
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			e, err := c.GetOrFetch(ctx, "k", func() ([]byte, error) {
				mu.Lock()
				runs++
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				return []byte("once"), nil
			})
			if err != nil || string(e.Value()) != "once" {
				t.Errorf("getOrFetch = %v, %v", e.Value(), err)
			}
		}()
	}
	close(start)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if runs != 1 {
		t.Fatalf("fetch ran %d times, want 1", runs)
	}
}

func TestHybridEvictionOffload(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](4). // tiny memory: entries evict quickly
							WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	for i := 0; i < 10; i++ {
		c.Insert(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}
	time.Sleep(200 * time.Millisecond) // let the pipe pump offload evictions

	// Evicted entries are served from the disk cache.
	e, err := c.Get(ctx, "k0")
	if err != nil {
		t.Fatal(err)
	}
	if e.IsNil() {
		t.Fatal("expected k0 to be served from the disk layer")
	}
	if string(e.Value()) != "v0" {
		t.Fatalf("k0 value = %q", e.Value())
	}
}

func TestHybridRemoveAndClear(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](1<<20).
		WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(64 << 10)
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	c.Insert("k", []byte("v"))
	c.Wait(ctx)
	c.Remove("k")
	c.Wait(ctx)
	if c.Contains("k") {
		t.Fatal("expected k to be removed from both layers")
	}
	e, err := c.Get(ctx, "k")
	if err != nil || !e.IsNil() {
		t.Fatalf("get k after remove = %v, %v", e, err)
	}

	c.Insert("k2", []byte("v2"))
	c.Wait(ctx)
	if err := c.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Contains("k2") {
		t.Fatal("expected k2 to be cleared from both layers")
	}
}
