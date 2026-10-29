// Example: hybrid cache with an in-memory layer and a disk (blob) layer.
//
// Run: go run ./examples/hybrid
package main

import (
	"context"
	"fmt"
	"time"

	"blob-cache/common"
	"blob-cache/hybrid"
	"blob-cache/storage"
)

func main() {
	ctx := context.Background()

	cache, err := hybrid.NewBuilder[string, []byte](4*1024*1024). // 4 MiB in-memory capacity
									WithName("example").
									WithStorage("./cache-data", func(sb *storage.StoreBuilder[string, []byte]) {
										sb.WithCodec(storage.BytesCodec()).
											WithBlobFileSize(16 << 20).   // 16 MiB blob files
											WithTotalCapacity(64 << 20)   // 64 MiB disk cache
									}).
									WithEventListener(common.EventListenerFunc[string, []byte](
										func(reason common.Event, key string, _ []byte) {
											fmt.Printf("[event] entry %q left the memory cache: %s\n", key, reason)
										})).
									Build()
	if err != nil {
		panic(err)
	}

	// Insert entries: they land in memory and are queued for the disk.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		cache.Insert(key, []byte(fmt.Sprintf("value-%03d", i)))
	}

	// Get: hits come from memory.
	entry, err := cache.Get(ctx, "key-000")
	if err != nil {
		panic(err)
	}
	fmt.Printf("get key-000: %s\n", entry.Value())

	// Give the background flusher a moment, then check the disk layer.
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("disk may contain key-000: %v\n", cache.Storage().MayContains("key-000"))

	// GetOrFetch: loads from the disk cache, or fetches on a full miss
	// with per-key deduplication.
	entry, err = cache.GetOrFetch(ctx, "key-000", func() ([]byte, error) {
		return nil, fmt.Errorf("should not be fetched: the entry is cached")
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("getOrFetch key-000: %s\n", entry.Value())

	entry, err = cache.GetOrFetch(ctx, "fetched-key", func() ([]byte, error) {
		fmt.Println("fetching fetched-key from the source...")
		return []byte("fresh-value"), nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("getOrFetch fetched-key: %s\n", entry.Value())

	// Remove from both layers.
	cache.Remove("key-001")

	// Metrics snapshot.
	keys, values := cache.Metrics().SnapshotSorted()
	fmt.Println("--- metrics ---")
	for i, k := range keys {
		fmt.Printf("%s = %d\n", k, values[i])
	}

	// Close: offloads remaining in-memory entries to the disk cache and
	// waits for all pending writes.
	if err := cache.Close(ctx); err != nil {
		panic(err)
	}
	fmt.Println("cache closed")
}
