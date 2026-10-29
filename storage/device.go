// Package storage provides the disk (blob) cache layer of blob-cache, the
// counterpart of foyer-storage. Entries are batched and written into
// append-only blob files, indexed in memory by their hash, and recovered by
// scanning the blob files on startup.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"blob-cache/common"
)

// BlobID identifies a blob file within the device.
type BlobID uint64

// BlobFileName formats the file name of a blob.
func BlobFileName(id BlobID) string { return fmt.Sprintf("blob_%016d", uint64(id)) }

// Statistics records the I/O statistics of the storage layer, mirroring
// foyer's `Statistics` with simplified counters.
type Statistics struct {
	ReadBytes  atomic.Int64
	ReadIOs    atomic.Int64
	WriteBytes atomic.Int64
	WriteIOs   atomic.Int64
}

// Snapshot returns the current counter values.
func (s *Statistics) Snapshot() (readBytes, readIOs, writeBytes, writeIOs int64) {
	return s.ReadBytes.Load(), s.ReadIOs.Load(), s.WriteBytes.Load(), s.WriteIOs.Load()
}

// Device is the blob storage device abstraction: a collection of
// fixed-size-capacity, append-mostly blob files with random read access.
// It is the counterpart of foyer's `Device` trait.
type Device interface {
	// Read reads exactly len(buf) bytes from the blob id at offset.
	Read(id BlobID, buf []byte, offset int64) error
	// Write writes buf to the blob id at offset, extending the blob as
	// needed. Writing past the blob capacity returns an error.
	Write(id BlobID, buf []byte, offset int64) error
	// BlobSize returns the capacity of one blob file.
	BlobSize() int64
	// CreateBlob creates (or truncates) the blob file with the given id.
	CreateBlob(id BlobID) error
	// DeleteBlob removes the blob file with the given id.
	DeleteBlob(id BlobID) error
	// ListBlobIDs returns the ids of all existing blob files.
	ListBlobIDs() ([]BlobID, error)
	// Sync flushes all written data to stable storage.
	Sync() error
	// Close releases device resources.
	Close() error
	// Destroy removes all files of the device (and its directory).
	Destroy() error
	// Stats returns the statistics of the device.
	Stats() *Statistics
}

// FsDeviceOptions configures a filesystem-backed device.
type FsDeviceOptions struct {
	// Dir is the directory holding the blob files.
	Dir string
	// BlobFileSize is the capacity of each blob file. Defaults to 64 MiB.
	BlobFileSize int64
}

func (o *FsDeviceOptions) blobFileSize() int64 {
	if o.BlobFileSize <= 0 {
		return 64 << 20
	}
	return o.BlobFileSize
}

// fsDevice is a filesystem-backed Device: each blob is one file named
// `blob_%016d`. It is the counterpart of foyer's `FileDevice` (with
// blob-file granularity instead of a single preallocated file).
type fsDevice struct {
	opts FsDeviceOptions
	blob int64
	stats *Statistics

	mu    sync.Mutex
	files map[BlobID]*os.File
}

// OpenFsDevice opens (creating the directory if needed) a filesystem
// device in dir.
func OpenFsDevice(opts FsDeviceOptions) (Device, error) {
	if opts.Dir == "" {
		return nil, common.NewError(common.ErrorKindConfig, "open device failed").
			WithContext("reason", "device dir must not be empty")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, common.NewError(common.ErrorKindIo, "create device dir failed").WithSource(err).
			WithContext("dir", opts.Dir)
	}
	return &fsDevice{
		opts:  opts,
		blob:  opts.blobFileSize(),
		stats: &Statistics{},
		files: make(map[BlobID]*os.File),
	}, nil
}

func (d *fsDevice) path(id BlobID) string {
	return filepath.Join(d.opts.Dir, BlobFileName(id))
}

func (d *fsDevice) file(id BlobID) (*os.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if f, ok := d.files[id]; ok {
		return f, nil
	}
	f, err := os.OpenFile(d.path(id), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	d.files[id] = f
	return f, nil
}

func (d *fsDevice) Read(id BlobID, buf []byte, offset int64) error {
	if offset < 0 || offset+int64(len(buf)) > d.blob {
		return common.NewError(common.ErrorKindOutOfRange, "read out of blob range").
			WithContext("blob", uint64(id)).
			WithContext("offset", offset).
			WithContext("len", len(buf))
	}
	f, err := d.file(id)
	if err != nil {
		return common.NewError(common.ErrorKindIo, "open blob for read failed").WithSource(err).
			WithContext("blob", uint64(id))
	}
	n, err := f.ReadAt(buf, offset)
	d.stats.ReadIOs.Add(1)
	d.stats.ReadBytes.Add(int64(n))
	if err != nil {
		return common.NewError(common.ErrorKindIo, "read blob failed").WithSource(err).
			WithContext("blob", uint64(id)).
			WithContext("offset", offset)
	}
	return nil
}

func (d *fsDevice) Write(id BlobID, buf []byte, offset int64) error {
	if offset < 0 || offset+int64(len(buf)) > d.blob {
		return common.NewError(common.ErrorKindNoSpace, "write out of blob range").
			WithContext("blob", uint64(id)).
			WithContext("offset", offset).
			WithContext("len", len(buf)).
			WithContext("blob_size", d.blob)
	}
	f, err := d.file(id)
	if err != nil {
		return common.NewError(common.ErrorKindIo, "open blob for write failed").WithSource(err).
			WithContext("blob", uint64(id))
	}
	n, err := f.WriteAt(buf, offset)
	d.stats.WriteIOs.Add(1)
	d.stats.WriteBytes.Add(int64(n))
	if err != nil {
		return common.NewError(common.ErrorKindIo, "write blob failed").WithSource(err).
			WithContext("blob", uint64(id)).
			WithContext("offset", offset)
	}
	return nil
}

func (d *fsDevice) BlobSize() int64 { return d.blob }

func (d *fsDevice) CreateBlob(id BlobID) error {
	f, err := os.OpenFile(d.path(id), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return common.NewError(common.ErrorKindIo, "create blob failed").WithSource(err).
			WithContext("blob", uint64(id))
	}
	d.mu.Lock()
	if old, ok := d.files[id]; ok {
		_ = old.Close()
	}
	d.files[id] = f
	d.mu.Unlock()
	return nil
}

func (d *fsDevice) DeleteBlob(id BlobID) error {
	d.mu.Lock()
	if f, ok := d.files[id]; ok {
		_ = f.Close()
		delete(d.files, id)
	}
	d.mu.Unlock()
	if err := os.Remove(d.path(id)); err != nil && !os.IsNotExist(err) {
		return common.NewError(common.ErrorKindIo, "delete blob failed").WithSource(err).
			WithContext("blob", uint64(id))
	}
	return nil
}

func (d *fsDevice) ListBlobIDs() ([]BlobID, error) {
	entries, err := os.ReadDir(d.opts.Dir)
	if err != nil {
		return nil, common.NewError(common.ErrorKindIo, "list blobs failed").WithSource(err).
			WithContext("dir", d.opts.Dir)
	}
	var ids []BlobID
	for _, e := range entries {
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "blob_%016d", &id); err == nil {
			ids = append(ids, BlobID(id))
		}
	}
	return ids, nil
}

func (d *fsDevice) Sync() error {
	d.mu.Lock()
	files := make([]*os.File, 0, len(d.files))
	for _, f := range d.files {
		files = append(files, f)
	}
	d.mu.Unlock()
	for _, f := range files {
		if err := f.Sync(); err != nil {
			return common.NewError(common.ErrorKindIo, "sync blob failed").WithSource(err)
		}
	}
	return nil
}

func (d *fsDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, f := range d.files {
		_ = f.Close()
		delete(d.files, id)
	}
	return nil
}

func (d *fsDevice) Destroy() error {
	if err := d.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(d.opts.Dir); err != nil {
		return common.NewError(common.ErrorKindIo, "destroy device failed").WithSource(err).
			WithContext("dir", d.opts.Dir)
	}
	return nil
}

func (d *fsDevice) Stats() *Statistics { return d.stats }
