package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

// failingDevice wraps a device and fails the first N writes after the
// switch is armed.
type failingDevice struct {
	Device
	arm      atomic.Bool
	failNext atomic.Int32
}

func (d *failingDevice) Write(id BlobID, buf []byte, offset int64) error {
	if d.arm.Load() && d.failNext.Add(-1) >= 0 {
		return common.NewError(common.ErrorKindIo, "injected write failure")
	}
	return d.Device.Write(id, buf, offset)
}

// TestStoreDeleteThenInsertSameBatch verifies the operation order is kept
// inside one flusher batch: delete-then-insert must keep the inserted
// entry (regression: the deferred-delete fix used to remove the freshly
// written entry).
func TestStoreDeleteThenInsertSameBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	v1 := memory.NewPiece[string, []byte]("k", []byte("v1"), common.Properties{}, 0, 1)
	store.Enqueue(v1, false)
	store.Delete("k")
	v2 := memory.NewPiece[string, []byte]("k", []byte("v2"), common.Properties{}, 0, 1)
	store.Enqueue(v2, false)
	store.Wait(ctx)

	load, err := store.Load(ctx, "k")
	if err != nil || load.Kind != LoadEntry || string(load.Value) != "v2" {
		t.Fatalf("load k after [insert, delete, insert] = %+v, %v (want v2)", load, err)
	}
}

// TestStoreWriteFailureDropsKeeper verifies that a device write failure
// drops the keeper registrations of the aborted batch, so the entries are
// not served from the keeper forever.
func TestStoreWriteFailureDropsKeeper(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	fd := &failingDevice{Device: dev}
	engine, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device: fd,
		Codec:  BytesCodec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore[string, []byte](StoreConfig[string, []byte]{
		Engine: engine,
		Codec:  BytesCodec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	enqueue := func(key, value string) {
		piece := memory.NewPiece[string, []byte](key, []byte(value), common.Properties{}, 0, 1)
		store.Enqueue(piece, true)
	}
	enqueue("a", "aa") // written
	store.Wait(ctx)

	// Arm failures and enqueue three more: the first write fails and the
	// whole batch is aborted.
	fd.arm.Store(true)
	fd.failNext.Store(2)
	enqueue("b", "bb")
	enqueue("c", "cc")
	enqueue("d", "dd")
	store.Wait(ctx)

	for _, key := range []string{"b", "c", "d"} {
		if store.MayContains(key) {
			t.Fatalf("key %q still may-contains after the write failure (keeper leak)", key)
		}
		if load, err := store.Load(ctx, key); err != nil || !load.IsMiss() {
			t.Fatalf("load %q after failure = %+v, %v (want miss)", key, load, err)
		}
	}
	// The entry written before the failure is untouched.
	if load, err := store.Load(ctx, "a"); err != nil || string(load.Value) != "aa" {
		t.Fatalf("load a = %+v, %v", load, err)
	}
}

// TestMayContainsBeforeFlush verifies MayContains agrees with Load for
// entries that are enqueued but not yet flushed.
func TestMayContainsBeforeFlush(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewStoreBuilder[string, []byte](dir).WithCodec(BytesCodec()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	piece := memory.NewPiece[string, []byte]("pending", []byte("v"), common.Properties{}, 0, 1)
	store.Enqueue(piece, false)
	if !store.MayContains("pending") {
		t.Fatal("MayContains should cover keeper-held entries")
	}
}

// TestReclaimTerminatesWithSelfishPicker verifies that a custom picker
// always returning the current blob does not spin the flusher forever.
func TestReclaimTerminatesWithSelfishPicker(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device:        dev,
		Codec:         BytesCodec(),
		TotalCapacity: 512, // maxBlobs = 1: every rotation triggers reclaim
		Picker:        selfishPicker{},
	})
	if err != nil {
		t.Fatal(err)
	}
	keeper := NewKeeper[string, []byte](1)
	codec := BytesCodec()

	for i := 0; i < 50; i++ {
		kb, _ := codec.EncodeKey("k")
		vb, _ := codec.EncodeValue([]byte("payload-payload-payload"))
		hash := common.DefaultHashBuilder.Hash64(kb)
		piece := memory.NewPiece[string, []byte]("k", vb, common.Properties{}, hash, 1)
		ref := keeper.Insert(piece)
		engine.Enqueue(ref, entryHeaderSize+len(kb)+len(vb))
	}
	engine.Wait(ctx) // must terminate despite the selfish picker
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type selfishPicker struct{}

func (selfishPicker) Pick(blobs map[BlobID]*BlobInfo, current BlobID) (BlobID, bool) {
	return current, true
}
