package storage

import (
	"context"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

func TestStoreEnqueueLoadDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).
		WithCodec(BytesCodec()).
		WithBlobFileSize(64 << 10).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	// Enqueue through the store: the hash is recomputed stably inside.
	piece := memory.NewPiece[string, []byte]("k1", []byte("v1"), common.Properties{}, 0xdeadbeef, 1)
	store.Enqueue(piece, false)
	store.Wait(ctx)

	if !store.MayContains("k1") {
		t.Fatal("store should contain k1")
	}
	load, err := store.Load(ctx, "k1")
	if err != nil || load.Kind != LoadEntry || string(load.Value) != "v1" {
		t.Fatalf("load k1 = %+v, %v", load, err)
	}

	store.Delete("k1")
	store.Wait(ctx)
	load, err = store.Load(ctx, "k1")
	if err != nil || !load.IsMiss() {
		t.Fatalf("load k1 after delete = %+v, %v", load, err)
	}

	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	piece := memory.NewPiece[string, []byte]("k", []byte("persistent"), common.Properties{}, 42, 1)
	store.Enqueue(piece, true)
	store.Wait(ctx)
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}

	store2, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close(ctx)
	load, err := store2.Load(ctx, "k")
	if err != nil || string(load.Value) != "persistent" {
		t.Fatalf("load k after reopen = %+v, %v", load, err)
	}
}

func TestStoreDestroy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	piece := memory.NewPiece[string, []byte]("k", []byte("v"), common.Properties{}, 1, 1)
	store.Enqueue(piece, true)
	store.Wait(ctx)
	if err := store.Destroy(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestStoreLoadBeforeFlush verifies that entries enqueued but not yet
// flushed are served from the keeper, mirroring foyer's `Store::load`.
func TestStoreLoadBeforeFlush(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	piece := memory.NewPiece[string, []byte]("pending", []byte("not-yet-flushed"), common.Properties{}, 0, 1)
	store.Enqueue(piece, false)

	// No Wait here: the entry must be served from the keeper.
	load, err := store.Load(ctx, "pending")
	if err != nil || load.Kind != LoadEntry || string(load.Value) != "not-yet-flushed" {
		t.Fatalf("load pending before flush = %+v, %v", load, err)
	}

	// Same key with different case is a different key: keeper must not
	// serve it (keeper lookup is by key, not just hash).
	load, err = store.Load(ctx, "Pending")
	if err != nil || !load.IsMiss() {
		t.Fatalf("load wrong key should miss = %+v, %v", load, err)
	}
}

// TestStoreEnqueueThenDeleteSameBatch verifies that an entry that is
// enqueued and deleted before any flush does not resurrect through its own
// write: deletes are applied after the writes of the same batch.
func TestStoreEnqueueThenDeleteSameBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	piece := memory.NewPiece[string, []byte]("k", []byte("v"), common.Properties{}, 0, 1)
	store.Enqueue(piece, false)
	store.Delete("k")
	store.Wait(ctx)

	load, err := store.Load(ctx, "k")
	if err != nil || !load.IsMiss() {
		t.Fatalf("load k after enqueue+delete = %+v, %v", load, err)
	}
	// NOTE: the physical write of the entry may already have happened, so
	// like foyer (without a tombstone log) the entry can reappear after a
	// restart; only the in-memory index is guaranteed to be clean.
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
