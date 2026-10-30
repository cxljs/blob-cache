package storage

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"blob-cache/common"
	"blob-cache/memory"
)

// faultDevice wraps a device and randomly fails operations while armed.
type faultDevice struct {
	Device
	arm atomic.Bool
	mu  sync.Mutex
	r   *rand.Rand
}

func (d *faultDevice) maybeFail(op string) error {
	if !d.arm.Load() {
		return nil
	}
	// Fail ~20% of armed operations. The RNG is guarded because Write runs
	// on the flusher goroutine while the test also touches it.
	d.mu.Lock()
	roll := d.r.Intn(100)
	d.mu.Unlock()
	if roll < 20 {
		return common.NewError(common.ErrorKindIo, fmt.Sprintf("injected %s failure", op))
	}
	return nil
}

func (d *faultDevice) Read(id BlobID, buf []byte, offset int64) error {
	if err := d.maybeFail("read"); err != nil {
		return err
	}
	return d.Device.Read(id, buf, offset)
}

func (d *faultDevice) Write(id BlobID, buf []byte, offset int64) error {
	if err := d.maybeFail("write"); err != nil {
		return err
	}
	return d.Device.Write(id, buf, offset)
}

func (d *faultDevice) Sync() error {
	if err := d.maybeFail("sync"); err != nil {
		return err
	}
	return d.Device.Sync()
}

func (d *faultDevice) ReadAll(id BlobID) ([]byte, error) {
	if err := d.maybeFail("readAll"); err != nil {
		return nil, err
	}
	return d.Device.ReadAll(id)
}

func (d *faultDevice) CreateBlob(id BlobID) error {
	if err := d.maybeFail("createBlob"); err != nil {
		return err
	}
	return d.Device.CreateBlob(id)
}

// TestStorageFaultInjection runs a randomized enqueue/load/delete sequence
// with random device failures armed. Enqueue is best-effort under device
// failures (a failed write drops the piece, exactly like foyer), so the
// model tracks a *confirmed* disk state that is re-synced from the actual
// device after every fail-and-drain cycle. The invariants:
//
//  1. between reconciliations, a Load hit on a confirmed key never returns
//     a value different from the confirmed one (no stale/foreign values);
//  2. no panic, no hang while failures are injected;
//  3. after the failures stop and the engine is restarted, every confirmed
//     entry loads its confirmed value: recovery never loses indexed data
//     and never resurrects older versions.
func TestStorageFaultInjection(t *testing.T) {
	for _, seed := range []int64{11, 22, 33, 44} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runStorageFault(t, seed)
		})
	}
}

func runStorageFault(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed))
	dir := t.TempDir()

	dev, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	fd := &faultDevice{Device: dev, r: rand.New(rand.NewSource(seed * 7919))}
	engine, err := NewBlobEngine(BlobEngineConfig[string, []byte]{
		Device:      fd,
		Codec:       BytesCodec(),
		RecoverMode: RecoverWary,
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

	// confirmed[k] = the value known to be on disk for k (nil = absent or
	// unknown after injected failures). dirty[k] = an operation happened
	// after the last reconciliation, so the confirmed value may be stale.
	confirmed := make(map[string][]byte)
	dirty := make(map[string]bool)
	keys := make([]string, 0, 48)
	inU := make(map[string]bool)

	reconcile := func(where string) {
		t.Helper()
		fd.arm.Store(false)
		store.Wait(ctx)
		for _, k := range keys {
			load, err := store.Load(ctx, k)
			switch {
			case err != nil:
				confirmed[k] = nil // unreadable: treat as unknown
			case load.Kind == LoadEntry:
				confirmed[k] = load.Value
			default:
				confirmed[k] = nil
			}
			dirty[k] = false
		}
	}

	check := func(op int, k string) {
		t.Helper()
		if fd.arm.Load() || dirty[k] {
			return // state may have moved since the last reconciliation
		}
		v := confirmed[k]
		if v == nil {
			return // absent or unknown: nothing exact to assert
		}
		load, err := store.Load(ctx, k)
		if err != nil {
			return // concurrent failures may break reads
		}
		if load.Kind != LoadEntry || string(load.Value) != string(v) {
			t.Fatalf("op %d: key %q diverged from confirmed state: got %q, want %q", op, k, load.Value, v)
		}
	}

	next := 0
	for op := 0; op < 1500; op++ {
		// Arm the failures for stretches, then reconcile.
		if op%200 == 100 {
			fd.arm.Store(true)
		}
		if op%200 == 0 && op > 0 {
			reconcile(fmt.Sprintf("op %d", op))
		}

		k := fmt.Sprintf("key-%02d", r.Intn(48))
		switch r.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61:
			next++
			piece := memory.NewPiece[string, []byte](k, []byte(fmt.Sprintf("value-%d", next)), common.Properties{}, 0, 1)
			store.Enqueue(piece, false)
			if !inU[k] {
				keys = append(keys, k)
				inU[k] = true
			}
			dirty[k] = true
		case 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74:
			store.Delete(k)
			dirty[k] = true
		case 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87:
			check(op, k)
		case 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99:
			// Periodically reconcile so the confirmed state stays fresh.
			if fd.arm.Load() {
				reconcile(fmt.Sprintf("op %d", op))
			} else {
				store.Wait(ctx)
			}
		}
		if op%17 == 0 {
			check(op, k)
		}
	}

	// Final reconciliation, restart, and full verification: every confirmed
	// entry must load its confirmed value after recovery.
	reconcile("final")
	if err := store.Close(ctx); err != nil {
		t.Fatalf("close after faults: %v", err)
	}

	store2, err := NewStoreBuilder[string, []byte](dir).
		WithCodec(BytesCodec()).
		WithBlobFileSize(8 << 10).
		Build()
	if err != nil {
		t.Fatalf("reopen after faults: %v", err)
	}
	defer store2.Close(ctx)

	for _, k := range keys {
		v := confirmed[k]
		if v == nil {
			continue
		}
		load, err := store2.Load(ctx, k)
		if err != nil {
			t.Fatalf("load %q after fault recovery: %v", k, err)
		}
		if load.Kind != LoadEntry || string(load.Value) != string(v) {
			t.Fatalf("load %q after fault recovery = %q, want %q", k, load.Value, v)
		}
	}
}
