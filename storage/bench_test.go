package storage

import (
	"context"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

func benchStore(b *testing.B) (*Store[string, []byte], context.Context) {
	b.Helper()
	dir := b.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).
		WithCodec(BytesCodec()).
		WithBlobFileSize(64 << 20).
		WithTotalCapacity(1 << 30).
		Build()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close(context.Background()) })
	return store, context.Background()
}

// BenchmarkStoreEnqueue measures the enqueue (submit) path only; the
// background flush keeps up on its own.
func BenchmarkStoreEnqueue(b *testing.B) {
	store, ctx := benchStore(b)
	value := make([]byte, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		piece := memory.NewPiece[string, []byte]("key-"+string(rune(97+i%26))+string(rune(48+i/26%10)), value, common.Properties{}, 0, 1)
		store.Enqueue(piece, false)
	}
	store.Wait(ctx)
}

// BenchmarkStoreLoadDisk measures loads that hit the disk (after flush).
func BenchmarkStoreLoadDisk(b *testing.B) {
	store, ctx := benchStore(b)
	value := make([]byte, 128)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26%10))
		store.Enqueue(memory.NewPiece[string, []byte](keys[i], value, common.Properties{}, 0, 1), false)
	}
	store.Wait(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Load(ctx, keys[i&4095]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreLoadKeeper measures loads served by the keeper (pending,
// not yet flushed).
func BenchmarkStoreLoadKeeper(b *testing.B) {
	store, ctx := benchStore(b)
	value := make([]byte, 128)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26%10))
		store.Enqueue(memory.NewPiece[string, []byte](keys[i], value, common.Properties{}, 0, 1), false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Load(ctx, keys[i&4095]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreEnqueueLoadParallel measures the mixed hot path.
func BenchmarkStoreEnqueueLoadParallel(b *testing.B) {
	store, ctx := benchStore(b)
	value := make([]byte, 128)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key-" + string(rune('a'+i%26))
			if i%2 == 0 {
				store.Enqueue(memory.NewPiece[string, []byte](key, value, common.Properties{}, 0, 1), false)
			} else if _, err := store.Load(ctx, key); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
	store.Wait(ctx)
}
