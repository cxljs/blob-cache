package storage

import (
	"context"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

// TestVacuumStaleDuplicate is a white-box regression test: a victim blob
// holding two records with the same hash (older first) must end up
// pointing at the NEWER copy after vacuum, never the stale one.
func TestVacuumStaleDuplicate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng, err := NewBlobEngine(BlobEngineConfig[string, string]{
		Device: mustDevice(t, dir, 4096),
		Codec:  GobCodec[string, string](),
	})
	if err != nil {
		t.Fatal(err)
	}
	e := eng.(*blobEngine[string, string])
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// The flusher is stopped; the internals can be driven directly.

	codec := e.cfg.Codec
	kb, _ := codec.EncodeKey("k")
	vOld, _ := codec.EncodeValue("OLD")
	vNew, _ := codec.EncodeValue("NEW")

	// Two records, same hash, older first (submission order).
	if err := e.writeRecord(nil, 42, kb, vOld, common.Properties{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writeRecord(nil, 42, kb, vNew, common.Properties{}); err != nil {
		t.Fatal(err)
	}
	if a, _ := e.ix.get(42); a.Offset == 8 {
		t.Fatalf("setup: indexer should point at the newer copy, got %+v", a)
	}

	// Rotate to a fresh blob with plenty of space, then vacuum blob 0.
	if err := e.openBlob(1); err != nil {
		t.Fatal(err)
	}
	if err := e.vacuum(0); err != nil {
		t.Fatal(err)
	}

	a, ok := e.ix.get(42)
	if !ok {
		t.Fatal("live entry lost by vacuum (unexpected eviction)")
	}
	buf := make([]byte, a.Len)
	if err := e.dev.Read(a.Blob, buf, a.Offset); err != nil {
		t.Fatal(err)
	}
	h, ok := parseEntryHeader(buf)
	if !ok {
		t.Fatal("parse failed")
	}
	_, value, ok := entryPayload(buf[entryHeaderSize:], h)
	if !ok {
		t.Fatal("payload failed")
	}
	v, err := codec.DecodeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	if v != "NEW" {
		t.Fatalf("BUG: vacuum resurrected stale copy: got %q want NEW (addr=%+v)", v, a)
	}
}

func mustDevice(t *testing.T, dir string, blobSize int64) Device {
	t.Helper()
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: blobSize})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

// TestInsertBatchDuplicateOrderIsStable ensures the shard grouping sort
// never reorders same-hash entries: the last address must win.
func TestInsertBatchDuplicateOrderIsStable(t *testing.T) {
	ix := newIndexer(8)
	batch := make([]hashedAddress, 0, 64)
	for i := 0; i < 64; i++ {
		batch = append(batch, hashedAddress{
			hash:    42, // same hash, many duplicates
			address: EntryAddress{Blob: BlobID(i), Offset: int64(i), Len: 1},
		})
	}
	_ = ix.insertBatch(batch)
	a, ok := ix.get(42)
	if !ok || a.Blob != 63 {
		t.Fatalf("last duplicate must win: got %+v", a)
	}
}

// TestPropsBitsRoundtrip verifies the properties byte encoding roundtrip.
func TestPropsBitsRoundtrip(t *testing.T) {
	p := common.Properties{Hint: common.HintLow, Location: common.LocationOnDisk, Age: common.AgeOld}
	if got := decodeProps(encodeProps(p)); got != p {
		t.Fatalf("props roundtrip: got %+v want %+v", got, p)
	}
}

// TestVacuumCurrentBlobGuard ensures vacuum refuses to vacuum the blob
// being written.
func TestVacuumCurrentBlobGuard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng, err := NewBlobEngine(BlobEngineConfig[string, string]{
		Device: mustDevice(t, dir, 4096),
		Codec:  GobCodec[string, string](),
	})
	if err != nil {
		t.Fatal(err)
	}
	e := eng.(*blobEngine[string, string])
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	codec := e.cfg.Codec
	kb, _ := codec.EncodeKey("k")
	vb, _ := codec.EncodeValue("v")
	if err := e.writeRecord(nil, 7, kb, vb, common.Properties{}); err != nil {
		t.Fatal(err)
	}
	// Vacuuming the current blob must be a no-op (not delete the file).
	if err := e.vacuum(e.current); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.blobs[e.current]; !ok {
		t.Fatal("current blob must survive a vacuum attempt on itself")
	}
	if _, ok := e.ix.get(7); !ok {
		t.Fatal("entry in the current blob must survive")
	}
}

// TestVacuumDropsDeletedEntries is a regression test: entries deleted from
// the indexer must NOT be resurrected when their blob is vacuumed (the
// vacuum used to treat indexer-missing entries as live orphans).
func TestVacuumDropsDeletedEntries(t *testing.T) {
	ctx := context.Background()
	eng, err := NewBlobEngine(BlobEngineConfig[string, string]{
		Device: mustDevice(t, t.TempDir(), 4096),
		Codec:  GobCodec[string, string](),
	})
	if err != nil {
		t.Fatal(err)
	}
	e := eng.(*blobEngine[string, string])
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	codec := e.cfg.Codec
	kb, _ := codec.EncodeKey("k")
	v1, _ := codec.EncodeValue("v1")
	if err := e.writeRecord(nil, 42, kb, v1, common.Properties{}); err != nil {
		t.Fatal(err)
	}

	// Simulate a delete: the indexer entry is removed while the on-disk
	// copy stays behind.
	if _, ok := e.ix.remove(42); !ok {
		t.Fatal("setup: expected the entry to be indexed")
	}

	// Rotate to a fresh blob so the victim is not the current one, then
	// vacuum: the deleted entry's copy must be dropped, not resurrected.
	if err := e.openBlob(1); err != nil {
		t.Fatal(err)
	}
	if err := e.vacuum(0); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.ix.get(42); ok {
		t.Fatal("deleted entry was resurrected by the vacuum")
	}
}

// TestEngineDestroyWithCancelledContext ensures Destroy joins the flusher
// even when the context is already cancelled, and leaves a working engine.
func TestEngineDestroyWithCancelledContext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng, dev := mustEngineWithKeeper(t, dir, 64<<10, 8*64<<10)
	codec := BytesCodec()

	enqueue2(eng, codec, "a", "aa")
	eng.Wait(ctx)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := eng.Destroy(cancelled); err != nil {
		t.Fatal(err)
	}
	dev.Close()

	// The engine must still be usable after the destroy: blob 0 was
	// recreated empty and accepts new writes.
	eng2, dev2 := mustEngineWithKeeper(t, dir, 64<<10, 8*64<<10)
	defer dev2.Close()
	enqueue2(eng2, codec, "b", "bb")
	eng2.Wait(ctx)
	if load, err := eng2.Load(ctx, hashOf(t, codec, "b")); err != nil || string(load.Value) != "bb" {
		t.Fatalf("load b after destroyed-restart = %+v, %v", load, err)
	}
}

func enqueue2(e Engine[string, []byte], codec *Codec[string, []byte], key, value string) {
	kb, _ := codec.EncodeKey(key)
	vb, _ := codec.EncodeValue([]byte(value))
	hash := common.DefaultHashBuilder.Hash64(kb)
	piece := memory.NewPiece[string, []byte](key, []byte(value), common.Properties{}, hash, 1)
	keeper := NewKeeper[string, []byte](1)
	ref := keeper.Insert(piece)
	e.Enqueue(ref, entryHeaderSize+len(kb)+len(vb))
}

func mustEngineWithKeeper(t *testing.T, dir string, blobSize, total int64) (Engine[string, []byte], Device) {
	t.Helper()
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: blobSize})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	eng, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device:        dev,
		Codec:         BytesCodec(),
		TotalCapacity: total,
	})
	if err != nil {
		t.Fatal(err)
	}
	return eng, dev
}
