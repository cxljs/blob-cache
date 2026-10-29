package hybrid

import (
	"context"
	"testing"

	"blob-cache/storage"
)

func benchHybrid(b *testing.B) (*HybridCache[string, []byte], context.Context) {
	b.Helper()
	return benchHybridWithCapacity(b, 1<<26)
}

func benchHybridWithCapacity(b *testing.B, capacity int) (*HybridCache[string, []byte], context.Context) {
	b.Helper()
	cache, err := NewBuilder[string, []byte](capacity).
		WithStorage(b.TempDir(), func(sb *storage.StoreBuilder[string, []byte]) {
			sb.WithCodec(storage.BytesCodec())
		}).
		Build()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cache.Close(context.Background()) })
	return cache, context.Background()
}

// BenchmarkHybridGetMemoryHit measures the hot path: a pure in-memory hit.
func BenchmarkHybridGetMemoryHit(b *testing.B) {
	cache, ctx := benchHybrid(b)
	for i := 0; i < 65536; i++ {
		cache.Insert("key-"+string(rune('a'+i%26)), make([]byte, 128))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := cache.Get(ctx, "key-"+string(rune('a'+i%26)))
		if err != nil {
			b.Fatal(err)
		}
		e.Release()
	}
}

// BenchmarkHybridGetDiskHit measures the path where the entry was evicted
// from memory and must be loaded from the disk layer.
func BenchmarkHybridGetDiskHit(b *testing.B) {
	// 128 entries of 512B fit in memory; the rest live on disk.
	cache, ctx := benchHybridWithCapacity(b, 128*512)
	for i := 0; i < 20000; i++ {
		cache.Insert("key-"+string(rune('a'+i%26))+string(rune('0'+i/26%10)), make([]byte, 512))
	}
	cache.Wait(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := cache.Get(ctx, "key-"+string(rune('a'+i%26))+string(rune('0'+i/26%10)))
		if err != nil {
			b.Fatal(err)
		}
		if e.IsNil() {
			b.Fatal("expected a hit")
		}
		e.Release()
	}
}

// BenchmarkHybridGetOrFetchHit measures GetOrFetch on an in-memory hit.
func BenchmarkHybridGetOrFetchHit(b *testing.B) {
	cache, ctx := benchHybrid(b)
	for i := 0; i < 65536; i++ {
		cache.Insert("key-"+string(rune('a'+i%26)), make([]byte, 128))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := cache.GetOrFetch(ctx, "key-"+string(rune('a'+i%26)), func() ([]byte, error) {
			return nil, nil
		})
		if err != nil {
			b.Fatal(err)
		}
		e.Release()
	}
}

// BenchmarkHybridMixedParallel measures the mixed hot path under
// concurrency: inserts, memory hits, disk hits and fetches.
func BenchmarkHybridMixedParallel(b *testing.B) {
	cache, ctx := benchHybrid(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key-" + string(rune('a'+i%26))
			switch i % 4 {
			case 0:
				cache.Insert(key, make([]byte, 128))
			case 1:
				e, err := cache.Get(ctx, key)
				if err != nil {
					b.Error(err)
					return
				}
				e.Release()
			case 2:
				e, err := cache.GetOrFetch(ctx, key, func() ([]byte, error) {
					return make([]byte, 128), nil
				})
				if err != nil {
					b.Error(err)
					return
				}
				e.Release()
			case 3:
				cache.Contains(key)
			}
			i++
		}
	})
	cache.Wait(ctx)
}
