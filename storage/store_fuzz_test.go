package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

// TestStoreRoundtripFuzz applies a random operation stream (insert with
// random-size values, overwrite, delete) to a store and reopens it. Two
// configurations are exercised:
//
//   - Large capacity (reclaim disabled): the strongest recovery invariant
//     holds — every live entry (key whose last op was an enqueue) must be
//     present with its newest value after the restart; deleted entries may
//     reappear (no tombstone log) but only with the value they had when
//     deleted.
//   - Small capacity (aggressive reclaim): entries may legitimately be
//     evicted, so only the no-stale/no-foreign-value invariant is asserted.
func TestStoreRoundtripFuzz(t *testing.T) {
	// Multiple seeds per configuration: the operation stream is randomized
	// per seed to shake out ordering/state bugs.
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("noreclaim/seed%d", seed), func(t *testing.T) {
			runStoreFuzz(t, seed, 64<<20, true)
		})
		t.Run(fmt.Sprintf("reclaim/seed%d", seed), func(t *testing.T) {
			runStoreFuzz(t, seed, 16<<10, false)
		})
	}
}

func runStoreFuzz(t *testing.T, seed int64, totalCapacity int64, requireAll bool) {
	t.Helper()
	ctx := context.Background()
	const (
		keys     = 40
		ops      = 400
		maxValue = 400 // crosses several 1 KiB blobs
	)
	dir := t.TempDir()

	build := func() *Store[string, []byte] {
		t.Helper()
		store, err := NewStoreBuilder[string, []byte](dir).
			WithCodec(BytesCodec()).
			WithBlobFileSize(1024).
			WithTotalCapacity(totalCapacity).
			Build()
		if err != nil {
			t.Fatal(err)
		}
		return store
	}

	rng := rand.New(rand.NewSource(seed*1000003))
	store := build()

	live := make(map[string][]byte)  // key -> newest value
	dead := make(map[string][]byte)  // key -> value at deletion time
	values := make([][]byte, 0, 64)  // value pool for verification
	enqueue := func(key string, value []byte) {
		piece := memory.NewPiece[string, []byte](key, value, common.Properties{}, 0, 1)
		store.Enqueue(piece, false)
	}

	for step := 0; step < ops; step++ {
		key := fmt.Sprintf("key-%d", rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4: // insert / overwrite
			v := make([]byte, rng.Intn(maxValue))
			for i := range v {
				v[i] = byte(step)
			}
			enqueue(key, v)
			live[key] = v
			delete(dead, key)
		case 5, 6, 7:
			// Serve from the pool to exercise overwrite of identical sizes.
			if len(values) > 0 {
				v := values[rng.Intn(len(values))]
				enqueue(key, v)
				live[key] = v
				delete(dead, key)
			}
		case 8: // delete
			if v, ok := live[key]; ok {
				store.Delete(key)
				dead[key] = v
				delete(live, key)
			}
		case 9:
			if step%50 == 0 {
				store.Wait(ctx)
			}
		}
		if step%10 == 0 {
			v := make([]byte, rng.Intn(maxValue))
			for i := range v {
				v[i] = byte(step + 1)
			}
			values = append(values, v)
		}
	}
	store.Wait(ctx)

	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen: the indexer is rebuilt from the blob files.
	store2 := build()
	defer store2.Close(ctx)

	for key, want := range live {
		load, err := store2.Load(ctx, key)
		if err != nil {
			t.Fatalf("load live key %q: %v", key, err)
		}
		if load.IsMiss() {
			if requireAll {
				t.Fatalf("live key %q lost after recovery (reclaim is disabled)", key)
			}
			// Aggressive reclaim may legitimately evict it.
			continue
		}
		if string(load.Value) != string(want) {
			t.Fatalf("live key %q came back stale: got %d bytes, want %d bytes", key, len(load.Value), len(want))
		}
	}

	for key, want := range dead {
		load, err := store2.Load(ctx, key)
		if err != nil {
			t.Fatalf("load deleted key %q: %v", key, err)
		}
		if !load.IsMiss() && string(load.Value) != string(want) {
			t.Fatalf("deleted key %q resurrected with a wrong value", key)
		}
	}
}
