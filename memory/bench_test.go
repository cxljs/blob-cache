package memory

import (
	"strconv"
	"testing"
)

func benchCache(b *testing.B, shards int) *Cache[int, int] {
	b.Helper()
	return NewBuilder[int, int](1 << 26).WithShards(shards).Build()
}

func BenchmarkCacheInsert(b *testing.B) {
	c := benchCache(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := c.Insert(i, i)
		e.Release()
	}
}

func BenchmarkCacheInsertParallel(b *testing.B) {
	c := benchCache(b, 8)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e := c.Insert(i, i)
			e.Release()
			i++
		}
	})
}

func BenchmarkCacheGetHit(b *testing.B) {
	c := benchCache(b, 8)
	for i := 0; i < 65536; i++ {
		c.Insert(i, i).Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := c.Get(i & 0xffff)
		e.Release()
	}
}

func BenchmarkCacheGetHitParallel(b *testing.B) {
	c := benchCache(b, 8)
	for i := 0; i < 65536; i++ {
		c.Insert(i, i).Release()
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e := c.Get(i & 0xffff)
			e.Release()
			i++
		}
	})
}

// BenchmarkCacheGetHitNoFinalizer quantifies the cost of the entry
// finalizer safety net on the Get path.
func BenchmarkCacheGetHitNoFinalizer(b *testing.B) {
	c := NewBuilder[int, int](1<<26).WithShards(8).WithEntryFinalizers(false).Build()
	for i := 0; i < 65536; i++ {
		c.Insert(i, i).Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := c.Get(i & 0xffff)
		e.Release()
	}
}

func BenchmarkCacheGetMiss(b *testing.B) {
	c := benchCache(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := c.Get(-i - 1)
		e.Release()
	}
}

func BenchmarkCacheGetOrFetchHit(b *testing.B) {
	c := benchCache(b, 8)
	for i := 0; i < 65536; i++ {
		c.Insert(i, i).Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := c.GetOrFetch(i&0xffff, func() (int, error) {
			return -1, nil
		})
		if err != nil {
			b.Fatal(err)
		}
		e.Release()
	}
}

func BenchmarkCacheMixedParallel(b *testing.B) {
	c := benchCache(b, 8)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % 8192
			switch i % 4 {
			case 0:
				c.Insert(key, i).Release()
			case 1:
				c.Get(key).Release()
			case 2:
				if e, err := c.GetOrFetch(key, func() (int, error) { return i, nil }); err == nil {
					e.Release()
				}
			case 3:
				c.Contains(key)
			}
			i++
		}
	})
}

var sinkHash uint64

func BenchmarkHashStringKey(b *testing.B) {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	c := NewBuilder[string, int](1 << 20).WithShards(8).Build()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkHash = c.hash(keys[i&1023])
	}
}
