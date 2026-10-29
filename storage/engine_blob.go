package storage

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"

	"blob-cache/common"
)

// BlobPicker picks the next blob to reclaim, mirroring foyer's
// `EvictionPicker` for the blob granularity.
type BlobPicker interface {
	// Pick returns the id of the blob to reclaim. `current` is the blob
	// currently being written and must not be picked. Returns ok=false
	// when no victim is found.
	Pick(blobs map[BlobID]*BlobInfo, current BlobID) (victim BlobID, ok bool)
}

// BlobInfo is the runtime accounting of one blob file.
type BlobInfo struct {
	// Written is the number of bytes written to the blob (including the
	// blob header and invalid regions).
	Written int64
	// Invalid is the number of written bytes that no longer belong to any
	// live entry.
	Invalid int64
}

// FifoPicker picks the blob with the smallest id (the oldest one).
type FifoPicker struct{}

// Pick implements BlobPicker.
func (FifoPicker) Pick(blobs map[BlobID]*BlobInfo, current BlobID) (BlobID, bool) {
	victim, ok := BlobID(0), false
	for id := range blobs {
		if id == current {
			continue
		}
		if !ok || id < victim {
			victim, ok = id, true
		}
	}
	return victim, ok
}

// InvalidRatioPicker picks the blob whose invalid ratio exceeds the
// threshold the most.
type InvalidRatioPicker struct {
	// Threshold is the minimum invalid ratio for a blob to be reclaimable.
	Threshold float64
}

// Pick implements BlobPicker.
func (p InvalidRatioPicker) Pick(blobs map[BlobID]*BlobInfo, current BlobID) (BlobID, bool) {
	var victim BlobID
	best := p.Threshold
	found := false
	for id, info := range blobs {
		if id == current || info.Written <= 0 {
			continue
		}
		ratio := float64(info.Invalid) / float64(info.Written)
		if ratio >= best {
			best = ratio
			victim, found = id, true
		}
	}
	return victim, found
}

// BlobEngineConfig configures the blob disk cache engine.
type BlobEngineConfig[K comparable, V any] struct {
	// Device holds the blob files. Required.
	Device Device
	// Codec serializes keys and values. Required.
	Codec *Codec[K, V]
	// TotalCapacity is the total device capacity in bytes. The engine
	// keeps at most TotalCapacity/BlobFileSize blob files by reclaiming.
	// Zero defaults to 8 blob files.
	TotalCapacity int64
	// Filter is an extra admission filter applied after the built-in size
	// limit check.
	Filter StorageFilter
	// Picker picks reclaim victims. Defaults to FifoPicker.
	Picker BlobPicker
	// RecoverMode controls recovery behavior on corrupted blobs.
	// Defaults to RecoverWary.
	RecoverMode RecoverMode
	// IndexerShards is the shard count of the in-memory indexer.
	IndexerShards int
	// Metrics receives engine-level counters. Optional.
	Metrics *common.Metrics
}

type subKind int

const (
	subInsert subKind = iota
	subDelete
	subWait
)

type submission[K comparable, V any] struct {
	kind subKind
	// insert fields: the piece reference and its pre-encoded payload.
	ref    *PieceRef[K, V]
	hash   uint64
	key    []byte
	value  []byte
	size   int
	waitCh chan struct{}
}

// rawRecord is an entry record ready to be written.
type rawRecord[K comparable, V any] struct {
	hash  uint64
	props common.Properties
	key   []byte
	value []byte
	ref   *PieceRef[K, V] // nil for reclaimed survivors
}

// blobEngine is the blob-based disk cache engine. Entries are batched and
// appended into blob files; the in-memory indexer maps hashes to their
// addresses; over-capacity blobs are reclaimed by copying their surviving
// entries into fresh writes (vacuum).
type blobEngine[K comparable, V any] struct {
	cfg      BlobEngineConfig[K, V]
	dev      Device
	ix       *indexer
	maxBlobs int
	picker   BlobPicker
	metrics  *common.Metrics

	mu   sync.Mutex
	cond *sync.Cond
	subs []submission[K, V]
	// spare recycles the backing array of the previous submission batch so
	// that enqueue does not re-grow the queue after every handoff.
	spare    []submission[K, V]
	closed   bool
	closeErr error
	done     chan struct{}

	// scratch is a flusher-goroutine-local serialization buffer, reused
	// across records to avoid one allocation per entry.
	scratch []byte
	// recordBuf/deletesBuf/waitsBuf/batchBuf/refsBuf are flusher-goroutine
	//-local scratch slices reused across batches.
	recordBuf  []rawRecord[K, V]
	deletesBuf []uint64
	waitsBuf   []chan struct{}
	batchBuf   []hashedAddress
	refsBuf    []*PieceRef[K, V]

	// Flusher-owned state. Guarded by mu while the flusher is not running
	// (i.e. during recovery and close).
	current BlobID
	offset  int64
	blobs   map[BlobID]*BlobInfo
}

// NewBlobEngine builds and opens a blob engine, recovering existing data
// from the device.
func NewBlobEngine[K comparable, V any](cfg BlobEngineConfig[K, V]) (Engine[K, V], error) {
	if cfg.Device == nil {
		return nil, common.NewError(common.ErrorKindConfig, "build blob engine failed").
			WithContext("reason", "device must not be nil")
	}
	if cfg.Codec == nil {
		return nil, common.NewError(common.ErrorKindConfig, "build blob engine failed").
			WithContext("reason", "codec must not be nil")
	}
	blobSize := cfg.Device.BlobSize()
	if blobSize < int64(blobHeaderSize)+int64(entryHeaderSize) {
		return nil, common.NewError(common.ErrorKindConfig, "build blob engine failed").
			WithContext("reason", "blob size too small")
	}
	maxBlobs := cfg.TotalCapacity / blobSize
	if maxBlobs <= 0 {
		maxBlobs = 8
	}
	recoverMode := cfg.RecoverMode
	if recoverMode != RecoverStrict && recoverMode != RecoverWary && recoverMode != RecoverQuiet {
		recoverMode = RecoverWary
	}
	picker := cfg.Picker
	if picker == nil {
		picker = FifoPicker{}
	}

	e := &blobEngine[K, V]{
		cfg:       cfg,
		dev:       cfg.Device,
		ix:        newIndexer(cfg.IndexerShards),
		maxBlobs:  int(maxBlobs),
		picker:    picker,
		metrics:   cfg.Metrics,
		done:      make(chan struct{}),
		blobs:     make(map[BlobID]*BlobInfo),
		spare:     make([]submission[K, V], 0, 64),
		recordBuf: make([]rawRecord[K, V], 0, 64),
		batchBuf:  make([]hashedAddress, 0, 64),
		refsBuf:   make([]*PieceRef[K, V], 0, 64),
	}
	e.cond = sync.NewCond(&e.mu)

	if err := e.recover(recoverMode); err != nil {
		return nil, err
	}
	go e.run()
	return e, nil
}

// recover rebuilds the indexer and blob accounting from the device.
func (e *blobEngine[K, V]) recover(mode RecoverMode) error {
	ids, err := e.dev.ListBlobIDs()
	if err != nil {
		return err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		data, err := e.dev.ReadAll(id)
		if err != nil {
			if mode == RecoverStrict {
				return common.NewError(common.ErrorKindRecover, "read blob failed").WithSource(err).
					WithContext("blob", uint64(id))
			}
			continue
		}
		if !checkBlobHeader(data) {
			// A blob without a valid header is treated as empty; it gets
			// reclaimed (and its file deleted) on the next vacuum.
			if mode == RecoverStrict {
				return common.NewError(common.ErrorKindRecover, "blob header mismatch").
					WithContext("blob", uint64(id))
			}
			e.blobs[id] = &BlobInfo{}
			continue
		}

		info := &BlobInfo{Written: int64(blobHeaderSize)}
		e.blobs[id] = info
		var batch []hashedAddress
		off := int64(blobHeaderSize)
		for off < int64(len(data)) {
			h, ok := parseEntryHeader(data[off:])
			if !ok {
				break
			}
			total := int64(entryLen(h.keyLen, h.valueLen))
			if off+total > int64(len(data)) {
				break
			}
			key, value, ok := entryPayload(data[off+entryHeaderSize:], h)
			if !ok || entryCRC(key, value) != h.crc {
				break
			}
			batch = append(batch, hashedAddress{
				hash: h.hash,
				address: EntryAddress{
					Blob:   id,
					Offset: off,
					Len:    int32(total),
				},
			})
			off += total
		}

		overwritten := e.ix.insertBatch(batch)
		for _, old := range overwritten {
			bi, ok := e.blobs[old.Blob]
			if !ok {
				bi = &BlobInfo{}
				e.blobs[old.Blob] = bi
			}
			bi.Invalid += int64(old.Len)
		}
		info.Written = off

		if off < int64(len(data)) && mode == RecoverStrict {
			return common.NewError(common.ErrorKindRecover, "corrupted entry in blob").
				WithContext("blob", uint64(id)).
				WithContext("offset", off)
		}
		// Wary and Quiet modes keep the valid prefix of a corrupted blob.
	}

	// Continue writing into the valid blob with the largest id; blobs
	// without a valid header cannot be appended to (their content layout
	// is unknown), so they are left for the reclaimer instead.
	for id, info := range e.blobs {
		if info.Written >= int64(blobHeaderSize) && id > e.current {
			e.current = id
		}
	}
	if e.current == 0 && (len(e.blobs) == 0 || e.blobs[e.current] == nil || e.blobs[e.current].Written < int64(blobHeaderSize)) {
		if len(e.blobs) > 0 {
			// All existing blobs are unusable; start afresh after them.
			var maxID BlobID
			for id := range e.blobs {
				if id > maxID {
					maxID = id
				}
			}
			return e.openBlob(maxID + 1)
		}
		return e.openBlob(0)
	}
	e.offset = e.blobs[e.current].Written
	if e.offset >= e.dev.BlobSize() {
		// The latest blob is full; the flusher rotates before the first
		// write.
		e.offset = e.dev.BlobSize()
	}
	return nil
}

// openBlob creates the blob file with the given id and writes its header.
// Only called from the flusher goroutine (or before it starts).
func (e *blobEngine[K, V]) openBlob(id BlobID) error {
	if err := e.dev.CreateBlob(id); err != nil {
		return err
	}
	header := appendBlobHeader(nil)
	if err := e.dev.Write(id, header, 0); err != nil {
		return err
	}
	e.current = id
	e.offset = int64(blobHeaderSize)
	e.blobs[id] = &BlobInfo{Written: e.offset}
	return nil
}

// run is the flusher main loop.
func (e *blobEngine[K, V]) run() {
	defer close(e.done)
	for {
		e.mu.Lock()
		for len(e.subs) == 0 && !e.closed {
			e.cond.Wait()
		}
		batch := e.subs
		e.subs = e.spare[:0]
		e.spare = nil
		closed := e.closed
		e.mu.Unlock()

		if err := e.handle(batch); err != nil {
			e.mu.Lock()
			e.closeErr = err
			e.mu.Unlock()
		}

		if closed {
			if err := e.dev.Sync(); err != nil {
				e.mu.Lock()
				e.closeErr = err
				e.mu.Unlock()
			}
			return
		}
		// Recycle the processed batch's backing array for a future batch.
		e.mu.Lock()
		e.spare = batch[:0]
		e.mu.Unlock()
	}
}

// handle processes one batch of submissions: inserts are encoded once and
// written, deletes are applied after the writes (so that an entry that is
// enqueued and deleted within the same batch is not resurrected by its own
// write), and waiters are unblocked last.
func (e *blobEngine[K, V]) handle(batch []submission[K, V]) error {
	records := e.recordBuf[:0]
	deletes := e.deletesBuf[:0]
	waitChs := e.waitsBuf[:0]
	for _, s := range batch {
		switch s.kind {
		case subInsert:
			if len(s.key)+len(s.value) > maxEntrySize(e.dev.BlobSize())-entryHeaderSize {
				e.failEnqueue()
				s.ref.Remove()
				continue
			}
			records = append(records, rawRecord[K, V]{
				hash:  s.hash,
				props: s.ref.Piece.Properties(),
				key:   s.key,
				value: s.value,
				ref:   s.ref,
			})
		case subDelete:
			deletes = append(deletes, s.hash)
		case subWait:
			waitChs = append(waitChs, s.waitCh)
		}
	}
	e.recordBuf = records
	e.deletesBuf = deletes
	e.waitsBuf = waitChs

	err := e.writeRecords(records)
	for _, hash := range deletes {
		if old, ok := e.ix.remove(hash); ok {
			if bi, ok := e.blobs[old.Blob]; ok {
				bi.Invalid += int64(old.Len)
			}
		}
	}
	// Unblock waiters only after the batch has been processed.
	for _, ch := range waitChs {
		close(ch)
	}
	return err
}

// writeRecords appends records into the current blob, rotating and
// reclaiming as needed.
func (e *blobEngine[K, V]) writeRecords(records []rawRecord[K, V]) error {
	batch := e.batchBuf[:0]
	writtenRefs := e.refsBuf[:0]
	defer func() {
		e.batchBuf = batch
		e.refsBuf = writtenRefs
	}()

	for _, r := range records {
		if err := e.ensureSpace(int64(entryLen(len(r.key), len(r.value)))); err != nil {
			// Drop the keeper registrations of what was already written;
			// their bytes stay on the device unindexed and are treated as
			// invalid by the next recovery/vacuum.
			for _, ref := range writtenRefs {
				ref.Remove()
			}
			return err
		}
		e.scratch = appendEntry(e.scratch[:0], r.hash, r.key, r.value, r.props)
		if err := e.dev.Write(e.current, e.scratch, e.offset); err != nil {
			for _, ref := range writtenRefs {
				ref.Remove()
			}
			return err
		}
		batch = append(batch, hashedAddress{hash: r.hash, address: EntryAddress{
			Blob:   e.current,
			Offset: e.offset,
			Len:    int32(len(e.scratch)),
		}})
		e.offset += int64(len(e.scratch))
		e.blobs[e.current].Written = e.offset
		if r.ref != nil {
			writtenRefs = append(writtenRefs, r.ref)
		}
	}

	for _, old := range e.ix.insertBatch(batch) {
		if bi, ok := e.blobs[old.Blob]; ok {
			bi.Invalid += int64(old.Len)
		}
	}
	for _, ref := range writtenRefs {
		ref.Remove()
	}

	// Reclaim over-capacity blobs after a rotation.
	if len(e.blobs) > e.maxBlobs {
		if err := e.reclaim(); err != nil {
			return err
		}
	}
	return nil
}

// ensureSpace rotates to a fresh blob if the current one cannot fit the
// incoming record.
func (e *blobEngine[K, V]) ensureSpace(need int64) error {
	if e.offset+need <= e.dev.BlobSize() {
		return nil
	}
	if err := e.dev.Sync(); err != nil {
		return err
	}
	return e.openBlob(e.current + 1)
}

// reclaim vacuums blobs until the blob count fits the capacity.
func (e *blobEngine[K, V]) reclaim() error {
	for len(e.blobs) > e.maxBlobs {
		victim, ok := e.picker.Pick(e.blobs, e.current)
		if !ok {
			return nil
		}
		if err := e.vacuum(victim); err != nil {
			return err
		}
	}
	return nil
}

// vacuum copies the surviving entries of the victim blob into the current
// blob, then removes the victim entirely.
//
// Survivors are reinserted only while they fit into the current blob
// without rotating: vacuum must free one blob per call, otherwise reclaim
// could never make progress. Entries that no longer fit are evicted (this
// is the disk cache eviction), as are survivors that were superseded by a
// newer write elsewhere.
func (e *blobEngine[K, V]) vacuum(victim BlobID) error {
	if victim == e.current {
		// The victim must never be the blob being written: reinserting
		// survivors into the victim itself would dangle the indexer.
		return nil
	}
	info, ok := e.blobs[victim]
	if !ok {
		return nil
	}

	blobSize := e.dev.BlobSize()
	var batch []hashedAddress
	reinserted := 0

	if info.Written > int64(blobHeaderSize) {
		data, err := e.dev.ReadAll(victim)
		if err != nil {
			return err
		}
		limit := info.Written
		if int64(len(data)) < limit {
			limit = int64(len(data))
		}
		off := int64(blobHeaderSize)
		for off < limit {
			h, ok := parseEntryHeader(data[off:])
			if !ok {
				break
			}
			total := int64(entryLen(h.keyLen, h.valueLen))
			if off+total > limit {
				break
			}
			key, value, ok := entryPayload(data[off+entryHeaderSize:], h)
			if !ok || entryCRC(key, value) != h.crc {
				break
			}
			off += total

			// Skip entries superseded by a newer write outside the victim.
			if addr, ok := e.ix.get(h.hash); ok && addr.Blob != victim {
				continue
			}
			// Stop reinserting when the current blob cannot fit the entry
			// without rotating; the remaining survivors are evicted.
			if e.offset+total > blobSize {
				break
			}
			e.scratch = appendEntry(e.scratch[:0], h.hash, key, value, h.props)
			if err := e.dev.Write(e.current, e.scratch, e.offset); err != nil {
				return err
			}
			batch = append(batch, hashedAddress{hash: h.hash, address: EntryAddress{
				Blob:   e.current,
				Offset: e.offset,
				Len:    int32(len(e.scratch)),
			}})
			e.offset += int64(len(e.scratch))
			e.blobs[e.current].Written = e.offset
			reinserted++
		}
	}

	// Drop all victim addresses (this evicts the survivors that were not
	// reinserted), then register the new addresses of the reinserted ones.
	e.ix.removeBlob(victim)
	for _, old := range e.ix.insertBatch(batch) {
		if bi, ok := e.blobs[old.Blob]; ok {
			bi.Invalid += int64(old.Len)
		}
	}

	if e.metrics != nil {
		e.metrics.StorageReinsert.Add(int64(reinserted))
	}
	delete(e.blobs, victim)
	return e.dev.DeleteBlob(victim)
}

func (e *blobEngine[K, V]) failEnqueue() {
	if e.metrics != nil {
		e.metrics.StorageEnqueueFail.Inc()
	}
}

// Enqueue implements Engine. The payload is encoded here; engines used
// through the Store should prefer EnqueuePrepared to avoid encoding twice.
func (e *blobEngine[K, V]) Enqueue(piece *PieceRef[K, V], estimatedSize int) {
	key, err := e.cfg.Codec.EncodeKey(piece.Piece.Key())
	if err != nil {
		e.failEnqueue()
		piece.Remove()
		return
	}
	value, err := e.cfg.Codec.EncodeValue(piece.Piece.Value())
	if err != nil {
		e.failEnqueue()
		piece.Remove()
		return
	}
	e.EnqueuePrepared(piece, key, value, estimatedSize)
}

// EnqueuePrepared enqueues a piece with its already encoded payload. It is
// used by the Store, which encodes the payload once for hashing and size
// estimation.
func (e *blobEngine[K, V]) EnqueuePrepared(piece *PieceRef[K, V], key, value []byte, estimatedSize int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		piece.Remove()
		return
	}
	e.subs = append(e.subs, submission[K, V]{
		kind:  subInsert,
		ref:   piece,
		hash:  piece.Piece.Hash(),
		key:   key,
		value: value,
		size:  estimatedSize,
	})
	e.cond.Broadcast()
}

// Delete implements Engine.
func (e *blobEngine[K, V]) Delete(hash uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.subs = append(e.subs, submission[K, V]{kind: subDelete, hash: hash})
	e.cond.Broadcast()
}

// Load implements Engine.
func (e *blobEngine[K, V]) Load(ctx context.Context, hash uint64) (Load[K, V], error) {
	if err := ctx.Err(); err != nil {
		return Load[K, V]{}, common.NewError(common.ErrorKindTaskCancelled, "load cancelled").WithSource(err)
	}
	addr, ok := e.ix.get(hash)
	if !ok {
		return Load[K, V]{Kind: LoadMiss}, nil
	}
	buf := make([]byte, addr.Len)
	if err := e.dev.Read(addr.Blob, buf, addr.Offset); err != nil {
		if isNotExist(err) {
			// The blob was reclaimed (or destroyed) concurrently with this
			// load; treat the entry as missing instead of failing.
			return Load[K, V]{Kind: LoadMiss}, nil
		}
		return Load[K, V]{}, err
	}
	h, ok := parseEntryHeader(buf)
	if !ok {
		return Load[K, V]{}, common.NewError(common.ErrorKindMagicMismatch, "load entry magic mismatch").
			WithContext("blob", uint64(addr.Blob)).
			WithContext("offset", addr.Offset)
	}
	key, value, ok := entryPayload(buf[entryHeaderSize:], h)
	if !ok || entryCRC(key, value) != h.crc {
		return Load[K, V]{}, common.NewError(common.ErrorKindChecksumMismatch, "load entry checksum mismatch").
			WithContext("blob", uint64(addr.Blob)).
			WithContext("offset", addr.Offset)
	}
	k, err := e.cfg.Codec.DecodeKey(key)
	if err != nil {
		return Load[K, V]{}, err
	}
	v, err := e.cfg.Codec.DecodeValue(value)
	if err != nil {
		return Load[K, V]{}, err
	}
	return Load[K, V]{Kind: LoadEntry, Key: k, Value: v, Age: h.props.Age}, nil
}

// isNotExist reports whether err wraps a file-not-found error.
func isNotExist(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return os.IsNotExist(pathErr)
	}
	return false
}

// MayContains implements Engine.
func (e *blobEngine[K, V]) MayContains(hash uint64) bool {
	_, ok := e.ix.get(hash)
	return ok
}

// Filter implements Engine.
func (e *blobEngine[K, V]) Filter(hash uint64, estimatedSize int) StorageFilterResult {
	if estimatedSize > maxEntrySize(e.dev.BlobSize()) {
		return FilterRejected
	}
	if e.cfg.Filter != nil {
		return e.cfg.Filter.Filter(hash, estimatedSize)
	}
	return FilterAdmitted
}

// Wait implements Engine.
func (e *blobEngine[K, V]) Wait(ctx context.Context) {
	ch := make(chan struct{})
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.subs = append(e.subs, submission[K, V]{kind: subWait, waitCh: ch})
	e.cond.Broadcast()
	e.mu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

// Close implements Engine.
func (e *blobEngine[K, V]) Close(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		err := e.closeErr
		e.mu.Unlock()
		return err
	}
	e.closed = true
	e.cond.Broadcast()
	e.mu.Unlock()

	select {
	case <-e.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := e.dev.Sync(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closeErr
}

// Destroy implements Engine. It removes all data and resets the engine so
// that it keeps working with an empty device (used by hybrid clear).
//
// The old flusher is always joined (regardless of ctx cancellation)
// before the state reset: letting it run past the reset would leave two
// flushers mutating the same unlocked state. Destroy therefore always
// completes atomically; ctx cancellation does not abort it.
func (e *blobEngine[K, V]) Destroy(ctx context.Context) error {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		e.cond.Broadcast()
	}
	e.mu.Unlock()

	<-e.done

	if err := e.dev.Destroy(); err != nil {
		return err
	}

	e.ix.clear()
	e.mu.Lock()
	e.blobs = make(map[BlobID]*BlobInfo)
	e.current = 0
	e.offset = 0
	e.closed = false
	e.closeErr = nil
	e.done = make(chan struct{})
	err := e.openBlob(0)
	e.mu.Unlock()
	if err != nil {
		return err
	}
	go e.run()
	return nil
}

// Stats implements Engine.
func (e *blobEngine[K, V]) Stats() *Statistics { return e.dev.Stats() }
