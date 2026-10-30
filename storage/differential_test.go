package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

// TestStorageDifferentialModel runs a randomized enqueue/delete/overwrite
// sequence through the Store (with random flush-then-check and random
// full restarts) and verifies the core disk-cache invariant:
//
//	Load(key) always returns the most recently enqueued value for the key,
//	except for keys whose most recent operation was a Delete (deletes are
//	asynchronous but become observable after Wait; they intentionally do
//	not survive a restart without a tombstone log).
//
// Reclaim is disabled (large TotalCapacity) so eviction cannot remove
// entries; blob rotation IS exercised.
func TestStorageDifferentialModel(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4, 5, 6, 7, 8} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runStorageModel(t, seed)
		})
	}
}

func runStorageModel(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed))
	dir := t.TempDir()

	store, err := NewStoreBuilder[string, []byte](dir).
		WithCodec(BytesCodec()).
		WithBlobFileSize(8 << 10). // 8 KiB blobs: frequent rotation
		WithTotalCapacity(64 << 20).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	latest := make(map[string][]byte)  // last enqueued value per key
	deleted := make(map[string]bool)   // last op was a delete (pre-restart)
	keys := make([]string, 0, 64)      // key universe in insertion order
	inUniverse := make(map[string]bool)

	enque := func(k, v string) {
		piece := memory.NewPiece[string, []byte](k, []byte(v), common.Properties{}, 0, 1)
		store.Enqueue(piece, false)
		if !inUniverse[k] {
			keys = append(keys, k)
			inUniverse[k] = true
		}
		latest[k] = []byte(v)
		deleted[k] = false
	}
	verifyAll := func(where string) {
		t.Helper()
		store.Wait(ctx)
		for _, k := range keys {
			load, err := store.Load(ctx, k)
			if deleted[k] {
				if err != nil || !load.IsMiss() {
					t.Fatalf("%s: key %q should be absent, got %+v err=%v", where, k, load, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: load %q error: %v", where, k, err)
			}
			if load.Kind != LoadEntry || string(load.Value) != string(latest[k]) {
				t.Fatalf("%s: STALE value for key %q: got %q, want %q", where, k, load.Value, latest[k])
			}
		}
	}

	restart := func() {
		if err := store.Close(ctx); err != nil {
			t.Fatalf("close: %v", err)
		}
		store2, err := NewStoreBuilder[string, []byte](dir).
			WithCodec(BytesCodec()).
			WithBlobFileSize(8 << 10).
			WithTotalCapacity(64 << 20).
			Build()
		if err != nil {
			t.Fatal(err)
		}
		*store = *store2
		// Deletes do not survive a restart (no tombstone log).
		for k := range deleted {
			deleted[k] = false
		}
	}

	next := 0
	for op := 0; op < 1200; op++ {
		k := fmt.Sprintf("key-%02d", r.Intn(64))
		switch r.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58:
			// Enqueue (overwrite or insert).
			next++
			enque(k, fmt.Sprintf("value-%d-%s", next, k))
		case 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73:
			// Delete.
			store.Delete(k)
			deleted[k] = true
		case 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89:
			// Flush and verify a random key.
			store.Wait(ctx)
			load, err := store.Load(ctx, k)
			if v, ok := latest[k]; ok && !deleted[k] {
				if err != nil || load.Kind != LoadEntry || string(load.Value) != string(v) {
					t.Fatalf("op %d: STALE value for key %q: got %q, want %q (err=%v)", op, k, load.Value, v, err)
				}
			} else if err != nil || !load.IsMiss() {
				t.Fatalf("op %d: key %q should be absent, got %+v err=%v", op, k, load, err)
			}
		case 90, 91, 92, 93, 94:
			// Flush and verify everything.
			verifyAll(fmt.Sprintf("op %d", op))
		case 95, 96, 97:
			// Full restart (recovery path).
			verifyAll(fmt.Sprintf("op %d before restart", op))
			restart()
		default:
			// Verify without flushing: hits may come from the keeper, and
			// pending deletes make absence checks racy, so only assert on
			// keys that were enqueued and not deleted, and only that a hit
			// is never stale.
			if v, ok := latest[k]; ok && !deleted[k] {
				load, err := store.Load(ctx, k)
				if err == nil && load.Kind == LoadEntry && string(load.Value) != string(v) {
					t.Fatalf("op %d: STALE value for key %q: got %q, want %q", op, k, load.Value, v)
				}
			}
		}
	}
	verifyAll("end")
	restart()
	verifyAll("after final restart")
}
