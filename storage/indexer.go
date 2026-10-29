package storage

import (
	"slices"
	"sync"
)

// EntryAddress locates one entry record on the device, mirroring foyer's
// `EntryAddress`.
type EntryAddress struct {
	Blob   BlobID
	Offset int64
	// Len is the total record length (header + key + value).
	Len int32
}

// hashedAddress pairs a hash with its entry address.
type hashedAddress struct {
	hash    uint64
	address EntryAddress
}

// indexerShard is one shard of the sharded indexer.
type indexerShard struct {
	mu sync.RWMutex
	m  map[uint64]EntryAddress
}

// indexer maps entry hashes to their on-device addresses. It is sharded to
// reduce lock contention, mirroring foyer's sharded `Indexer`.
type indexer struct {
	shards []*indexerShard
}

func newIndexer(shards int) *indexer {
	if shards <= 0 {
		shards = 8
	}
	ix := &indexer{shards: make([]*indexerShard, shards)}
	for i := range ix.shards {
		ix.shards[i] = &indexerShard{m: make(map[uint64]EntryAddress)}
	}
	return ix
}

func (ix *indexer) shard(hash uint64) *indexerShard {
	return ix.shards[hash%uint64(len(ix.shards))]
}

// insertBatch inserts a batch of addresses, returning the overwritten
// addresses (whose entries became invalid). The batch is grouped by shard
// with a stable in-place sort so that each shard lock is held once, no
// grouping map is allocated, and the caller's ordering is preserved: for
// duplicate hashes the LAST address must win.
func (ix *indexer) insertBatch(batch []hashedAddress) []EntryAddress {
	var overwritten []EntryAddress
	if len(batch) == 0 {
		return nil
	}
	if len(ix.shards) > 1 {
		shards := uint64(len(ix.shards))
		slices.SortStableFunc(batch, func(a, b hashedAddress) int {
			return int(a.hash%shards) - int(b.hash%shards)
		})
	}
	for i := 0; i < len(batch); {
		shardIdx := int(batch[i].hash % uint64(len(ix.shards)))
		sh := ix.shards[shardIdx]
		j := i
		for j < len(batch) && int(batch[j].hash%uint64(len(ix.shards))) == shardIdx {
			j++
		}
		sh.mu.Lock()
		for _, ha := range batch[i:j] {
			if old, ok := sh.m[ha.hash]; ok {
				overwritten = append(overwritten, old)
			}
			sh.m[ha.hash] = ha.address
		}
		sh.mu.Unlock()
		i = j
	}
	return overwritten
}

// get returns the address for the hash.
func (ix *indexer) get(hash uint64) (EntryAddress, bool) {
	sh := ix.shard(hash)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	a, ok := sh.m[hash]
	return a, ok
}

// remove removes the address for the hash, returning the removed address.
func (ix *indexer) remove(hash uint64) (EntryAddress, bool) {
	sh := ix.shard(hash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	a, ok := sh.m[hash]
	if ok {
		delete(sh.m, hash)
	}
	return a, ok
}

// removeBatch removes a batch of hashes, returning the removed addresses.
func (ix *indexer) removeBatch(hashes []uint64) []EntryAddress {
	var removed []EntryAddress
	byShard := make(map[int][]uint64)
	for _, h := range hashes {
		s := int(h % uint64(len(ix.shards)))
		byShard[s] = append(byShard[s], h)
	}
	for s, items := range byShard {
		sh := ix.shards[s]
		sh.mu.Lock()
		for _, h := range items {
			if a, ok := sh.m[h]; ok {
				removed = append(removed, a)
				delete(sh.m, h)
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// removeBlob removes all addresses pointing into the given blob and
// returns their hashes. Used when a blob is reclaimed.
func (ix *indexer) removeBlob(blob BlobID) []uint64 {
	var hashes []uint64
	for _, sh := range ix.shards {
		sh.mu.Lock()
		for h, a := range sh.m {
			if a.Blob == blob {
				hashes = append(hashes, h)
				delete(sh.m, h)
			}
		}
		sh.mu.Unlock()
	}
	return hashes
}

// clear removes all addresses.
func (ix *indexer) clear() {
	for _, sh := range ix.shards {
		sh.mu.Lock()
		sh.m = make(map[uint64]EntryAddress)
		sh.mu.Unlock()
	}
}

// len returns the number of indexed entries.
func (ix *indexer) len() int {
	total := 0
	for _, sh := range ix.shards {
		sh.mu.RLock()
		total += len(sh.m)
		sh.mu.RUnlock()
	}
	return total
}
