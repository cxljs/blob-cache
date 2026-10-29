// Example: in-memory cache with pluggable eviction algorithms.
//
// Run: go run ./examples/memory
package main

import (
	"fmt"

	"blob-cache/common"
	"blob-cache/memory"
)

func main() {
	// w-TinyLFU is the default eviction algorithm. Others: memory.LruConfig,
	// memory.FifoConfig, memory.S3FifoConfig, memory.SieveConfig.
	cache := memory.NewBuilder[string, string](1024).
		WithName("example").
		WithShards(4).
		WithEvictionConfig(memory.DefaultLfuConfig()).
		WithEventListener(common.EventListenerFunc[string, string](
			func(reason common.Event, key string, _ string) {
				fmt.Printf("[event] %q: %s\n", key, reason)
			})).
		Build()

	// Insert returns a reference-counted entry; pinned entries are not
	// evicted until released.
	entry := cache.Insert("greeting", "hello, blob-cache")
	fmt.Printf("inserted %q = %q (refs=%d)\n", entry.Key(), entry.Value(), entry.Refs())
	entry.Release()

	// Get pins the entry while it is referenced.
	got := cache.Get("greeting")
	fmt.Printf("get %q = %q (outdated=%v)\n", got.Key(), got.Value(), got.IsOutdated())
	got.Release()

	// GetOrFetch deduplicates concurrent fetches per key.
	e, err := cache.GetOrFetch("computed", func() (string, error) {
		fmt.Println("computing value for \"computed\"...")
		return "computed-value", nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("getOrFetch %q = %q\n", e.Key(), e.Value())
	e.Release()

	// Stats.
	fmt.Printf("entries=%d usage=%d capacity=%d\n", cache.Entries(), cache.Usage(), cache.Capacity())
	keys, values := cache.Metrics().SnapshotSorted()
	for i, k := range keys {
		fmt.Printf("%s = %d\n", k, values[i])
	}
}
