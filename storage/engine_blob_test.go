package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

func newTestEngine(t *testing.T, dir string, blobSize int64, totalCapacity int64) (Engine[string, []byte], Device) {
	t.Helper()
	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: blobSize})
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device:        dev,
		Codec:         BytesCodec(),
		TotalCapacity: totalCapacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e, dev
}

func enqueuePiece(t *testing.T, e Engine[string, []byte], codec *Codec[string, []byte], key string, value []byte, age common.Age) {
	t.Helper()
	kb, err := codec.EncodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	hash := common.DefaultHashBuilder.Hash64(kb)
	p := memory.NewPiece[string, []byte](key, value, common.Properties{}.WithAge(age), hash, 1)
	e.Enqueue(p, entryHeaderSize+len(kb)+len(value))
}

func hashOf(t *testing.T, codec *Codec[string, []byte], key string) uint64 {
	t.Helper()
	kb, err := codec.EncodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return common.DefaultHashBuilder.Hash64(kb)
}

func TestBlobEngineBasic(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache")
	e, dev := newTestEngine(t, dir, 64<<10, 8*64<<10)
	codec := BytesCodec()

	enqueuePiece(t, e, codec, "k1", []byte("value-1"), common.AgeFresh)
	enqueuePiece(t, e, codec, "k2", []byte("value-2"), common.AgeYoung)
	e.Wait(ctx)

	if !e.MayContains(hashOf(t, codec, "k1")) {
		t.Fatal("engine should contain k1 after flush")
	}
	load, err := e.Load(ctx, hashOf(t, codec, "k1"))
	if err != nil {
		t.Fatal(err)
	}
	if load.Kind != LoadEntry || load.Key != "k1" || string(load.Value) != "value-1" || load.Age != common.AgeFresh {
		t.Fatalf("load k1 = %+v", load)
	}
	load, err = e.Load(ctx, hashOf(t, codec, "k2"))
	if err != nil || load.Age != common.AgeYoung {
		t.Fatalf("load k2 = %+v, %v", load, err)
	}

	// Overwrite k1 with a new value.
	enqueuePiece(t, e, codec, "k1", []byte("value-1-new"), common.AgeFresh)
	e.Wait(ctx)
	load, err = e.Load(ctx, hashOf(t, codec, "k1"))
	if err != nil || string(load.Value) != "value-1-new" {
		t.Fatalf("load k1 after overwrite = %+v, %v", load, err)
	}

	// Delete k2.
	e.Delete(hashOf(t, codec, "k2"))
	e.Wait(ctx)
	load, err = e.Load(ctx, hashOf(t, codec, "k2"))
	if err != nil || !load.IsMiss() {
		t.Fatalf("load k2 after delete = %+v, %v", load, err)
	}

	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	dev.Close()

	// Recovery: reopen and load everything again.
	e2, dev2 := newTestEngine(t, dir, 64<<10, 8*64<<10)
	defer dev2.Close()
	load, err = e2.Load(ctx, hashOf(t, codec, "k1"))
	if err != nil || string(load.Value) != "value-1-new" {
		t.Fatalf("load k1 after recovery = %+v, %v", load, err)
	}
	load, err = e2.Load(ctx, hashOf(t, codec, "k2"))
	// NOTE: without a tombstone log, an entry deleted right before a
	// restart reappears after recovery; the deletion only affects the
	// in-memory index. This mirrors foyer without its optional tombstone
	// log.
	if err != nil || load.Kind != LoadEntry || string(load.Value) != "value-2" {
		t.Fatalf("load k2 after recovery = %+v, %v", load, err)
	}
}

func TestBlobEngineReclaim(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache")
	// 4 blobs of 1 KiB: 4 KiB total capacity.
	e, dev := newTestEngine(t, dir, 1024, 4*1024)
	defer dev.Close()
	codec := BytesCodec()

	for i := 0; i < 300; i++ {
		key := "key-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		value := make([]byte, 40)
		for j := range value {
			value[j] = byte(i)
		}
		enqueuePiece(t, e, codec, key, value, common.AgeFresh)
	}
	e.Wait(ctx)

	ids, err := dev.ListBlobIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) > 4 {
		t.Fatalf("blob count = %d, want <= 4", len(ids))
	}

	// Recently written keys must still be loadable (they live in the
	// current blob or were reinserted by the vacuum).
	for i := 290; i < 300; i++ {
		key := "key-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		load, err := e.Load(ctx, hashOf(t, codec, key))
		if err != nil {
			t.Fatal(err)
		}
		if load.Kind != LoadEntry {
			t.Fatalf("key %q missing after reclaim", key)
		}
		if len(load.Value) != 40 {
			t.Fatalf("key %q value length = %d", key, len(load.Value))
		}
	}
}

func TestBlobEngineCorruptTail(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache")
	e, dev := newTestEngine(t, dir, 64<<10, 8*64<<10)
	codec := BytesCodec()

	enqueuePiece(t, e, codec, "a", []byte("aa"), common.AgeFresh)
	enqueuePiece(t, e, codec, "b", []byte("bb"), common.AgeFresh)
	e.Wait(ctx)
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	dev.Close()

	// Corrupt the tail of blob 0: append garbage bytes.
	f, err := os.OpenFile(filepath.Join(dir, BlobFileName(0)), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("garbage garbage")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Wary recovery keeps the valid entries.
	e2, dev2 := newTestEngine(t, dir, 64<<10, 8*64<<10)
	defer dev2.Close()
	load, err := e2.Load(ctx, hashOf(t, codec, "a"))
	if err != nil || string(load.Value) != "aa" {
		t.Fatalf("load a after corrupt tail = %+v, %v", load, err)
	}
	load, err = e2.Load(ctx, hashOf(t, codec, "b"))
	if err != nil || string(load.Value) != "bb" {
		t.Fatalf("load b after corrupt tail = %+v, %v", load, err)
	}
}

func TestBlobEngineCorruptStrict(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	e, dev := newTestEngine(t, dir, 64<<10, 8*64<<10)
	codec := BytesCodec()

	enqueuePiece(t, e, codec, "a", []byte("aa"), common.AgeFresh)
	e.Wait(context.Background())
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	dev.Close()

	// Corrupt the middle of blob 0: flip bytes inside the first record.
	f, err := os.OpenFile(filepath.Join(dir, BlobFileName(0)), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite the entry magic at offset 8.
	if _, err := f.WriteAt([]byte{0, 0, 0, 0}, 8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	bdev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer bdev.Close()
	if _, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device:      bdev,
		Codec:       BytesCodec(),
		RecoverMode: RecoverStrict,
	}); err == nil {
		t.Fatal("expected strict recovery to fail on corrupted blob")
	}
}

func TestBlobEngineSizeFilter(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache")
	e, dev := newTestEngine(t, dir, 4096, 8*4096)
	defer dev.Close()
	codec := BytesCodec()

	// An entry larger than the blob capacity is rejected by the built-in
	// size filter.
	big := make([]byte, 8192)
	enqueuePiece(t, e, codec, "big", big, common.AgeFresh)
	e.Wait(ctx)
	if load, err := e.Load(ctx, hashOf(t, codec, "big")); err != nil || !load.IsMiss() {
		t.Fatalf("oversized entry should be rejected: %+v, %v", load, err)
	}

	// The estimated size filter path is exercised through Filter.
	if res := e.Filter(0, 8192); res.IsAdmitted() {
		t.Fatal("oversized entry should not be admitted")
	}
	if res := e.Filter(0, 16); !res.IsAdmitted() {
		t.Fatal("small entry should be admitted")
	}
}

func TestFifoAndInvalidRatioPickers(t *testing.T) {
	blobs := map[BlobID]*BlobInfo{
		3: {Written: 100, Invalid: 90},
		1: {Written: 100, Invalid: 10},
		2: {Written: 100, Invalid: 50},
		4: {Written: 100, Invalid: 95},
	}
	if victim, ok := (FifoPicker{}).Pick(blobs, 2); !ok || victim != 1 {
		t.Fatalf("fifo picker victim = %d, want 1", victim)
	}
	if victim, ok := (InvalidRatioPicker{Threshold: 0.8}).Pick(blobs, 3); !ok || victim != 4 {
		t.Fatalf("invalid ratio picker victim = %d, want 4", victim)
	}
	if _, ok := (InvalidRatioPicker{Threshold: 0.99}).Pick(blobs, 3); ok {
		t.Fatal("no blob exceeds the 0.99 threshold")
	}
}
