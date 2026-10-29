package storage

import (
	"path/filepath"
	"testing"
)

func TestFsDeviceBasic(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	d, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if d.BlobSize() != 4096 {
		t.Fatalf("blob size = %d, want 4096", d.BlobSize())
	}

	if err := d.CreateBlob(0); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBlob(1); err != nil {
		t.Fatal(err)
	}

	ids, err := d.ListBlobIDs()
	if err != nil || len(ids) != 2 {
		t.Fatalf("list blobs = %v, %v, want 2 blobs", ids, err)
	}

	// Write then read back.
	buf := []byte("hello blob world")
	if err := d.Write(0, buf, 8); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(buf))
	if err := d.Read(0, got, 8); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(buf) {
		t.Fatalf("read = %q, want %q", got, buf)
	}

	// Out-of-range writes are rejected.
	if err := d.Write(0, make([]byte, 16), 4090); err == nil {
		t.Fatal("expected out-of-range write to fail")
	}
	if err := d.Read(0, make([]byte, 16), 4090); err == nil {
		t.Fatal("expected out-of-range read to fail")
	}

	// Statistics.
	rb, rio, wb, wio := d.Stats().Snapshot()
	if wb != int64(len(buf)) || wio != 1 || rb != int64(len(buf)) || rio != 1 {
		t.Fatalf("stats = %d/%d/%d/%d", rb, rio, wb, wio)
	}

	// Delete a blob.
	if err := d.DeleteBlob(1); err != nil {
		t.Fatal(err)
	}
	ids, _ = d.ListBlobIDs()
	if len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("list blobs after delete = %v", ids)
	}

	// Re-open the device: files are still there.
	d.Close()
	d2, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	ids, _ = d2.ListBlobIDs()
	if len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("list blobs after reopen = %v", ids)
	}
	got2 := make([]byte, len(buf))
	if err := d2.Read(0, got2, 8); err != nil {
		t.Fatal(err)
	}
	if string(got2) != string(buf) {
		t.Fatalf("read after reopen = %q", got2)
	}
}

func TestFsDeviceDestroy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	d, err := OpenFsDevice(FsDeviceOptions{Dir: dir, BlobFileSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateBlob(0); err != nil {
		t.Fatal(err)
	}
	if err := d.Destroy(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ListBlobIDs(); err == nil {
		t.Fatal("expected list after destroy to fail (dir removed)")
	}
}
