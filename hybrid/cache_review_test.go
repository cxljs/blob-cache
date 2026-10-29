package hybrid

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"blob-cache/common"
	"blob-cache/storage"
)

// TestHybridInMemNotPersisted verifies that entries advised to stay in
// memory are never written to the disk layer, neither on insert nor via
// the eviction pipe.
func TestHybridInMemNotPersisted(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](128).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).
			WithBlobFileSize(64 << 10).
			WithTotalCapacity(64 << 20)
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	c.InsertWithProperties("mem-only", []byte("v"), common.Properties{}.WithLocation(common.LocationInMem))
	waitForDisk(t, c, "mem-only", false)

	// Force the entry out of the in-memory cache: it must not reach the
	// disk layer through the eviction pipe either.
	for i := 0; i < 1000; i++ {
		e := c.Insert(fmt.Sprintf("fill-%d", i), make([]byte, 512))
		e.Release()
	}
	c.Wait(ctx)
	time.Sleep(100 * time.Millisecond)
	if c.Storage().MayContains("mem-only") {
		t.Fatal("InMem entry must not be persisted through the eviction pipe")
	}
}

// waitForDisk polls until the disk layer's containment of the key matches
// want. The pipe pump forwards evictions asynchronously, so containment
// can lag the Wait of the engine queue.
func waitForDisk(t *testing.T, c *HybridCache[string, []byte], key string, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.Storage().MayContains(key) != want && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if c.Storage().MayContains(key) != want {
		t.Fatalf("disk containment of %q = %v, want %v", key, !want, want)
	}
}

// TestHybridGetOrFetchFallsBackOnDiskError verifies that a disk cache load
// error (e.g. a corrupted entry) does not fail GetOrFetch: it falls
// through to the user fetch, mirroring foyer.
func TestHybridGetOrFetchFallsBackOnDiskError(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](128).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).
			WithBlobFileSize(64 << 10).
			WithTotalCapacity(64 << 20) // no reclaim: blob 0 keeps the entry
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	// 128-entry capacity: fill so that "k" is evicted from memory (and
	// offloaded to disk).
	c.Insert("k", []byte("corrupted-value"))
	for i := 0; i < 1000; i++ {
		e := c.Insert(fmt.Sprintf("fill-%d", i), make([]byte, 512))
		e.Release()
	}
	waitForDisk(t, c, "k", true)
	c.Wait(ctx)

	// Corrupt the LAST record of key "k" across all blob files (the
	// indexer points at the last write: the insert writes once and the
	// eviction pipes a second copy, possibly after a blob rotation) so
	// that its CRC no longer matches.
	lastKPath, lastKOff := "", -1
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		blobPath := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(blobPath)
		if err != nil {
			t.Fatal(err)
		}
		off := 8 // blob header
		for off+28 < len(data) {
			keyLen := int(uint32(data[off+12]) | uint32(data[off+13])<<8 | uint32(data[off+14])<<16 | uint32(data[off+15])<<24)
			valLen := int(uint32(data[off+16]) | uint32(data[off+17])<<8 | uint32(data[off+18])<<16 | uint32(data[off+19])<<24)
			keyStart := off + 28
			if keyStart+keyLen+valLen > len(data) {
				break
			}
			if string(data[keyStart:keyStart+keyLen]) == "k" {
				lastKPath, lastKOff = blobPath, keyStart
			}
			off = keyStart + keyLen + valLen
		}
	}
	if lastKPath == "" {
		t.Fatal("failed to locate the record of key \"k\"")
	}
	data, err := os.ReadFile(lastKPath)
	if err != nil {
		t.Fatal(err)
	}
	data[lastKOff] ^= 0xFF
	if err := os.WriteFile(lastKPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// The disk load now fails; GetOrFetch must still succeed through the
	// fetch fallback.
	e, err := c.GetOrFetch(ctx, "k", func() ([]byte, error) {
		return []byte("fetched-fallback"), nil
	})
	if err != nil {
		t.Fatalf("getOrFetch after disk corruption = %v", err)
	}
	if string(e.Value()) != "fetched-fallback" {
		t.Fatalf("getOrFetch value = %q, want fetched-fallback", e.Value())
	}
	if e.Source() != common.SourceOuter {
		t.Fatalf("fallback entry source = %v, want Outer", e.Source())
	}
}
