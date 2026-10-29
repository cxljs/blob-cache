package memory

import (
	"fmt"
	"testing"

	"blob-cache/common"
)

func TestLfuWindowOverflow(t *testing.T) {
	// capacity 4: window capacity = 0 (10% of 4), protected = 3 (80% of 4).
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(LfuConfig{
			WindowCapacityRatio:    0.1,
			ProtectedCapacityRatio: 0.8,
			CmsEps:                 0.01,
			CmsConfidence:          0.9,
		})
	})

	for i := 0; i < 4; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}
	// All entries overflowed to probation; inserting one more evicts the
	// probation front (k0) whose frequency is not lower than the window
	// front.
	e := c.Insert("k4", 4)
	e.Release()
	if c.Contains("k0") {
		t.Error("expected k0 to be evicted")
	}
	if !c.Contains("k1") {
		t.Error("expected k1 to be present")
	}

	// Access k1 to promote it into the protected queue.
	c.Get("k1").Release()
	for i := 5; i < 12; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i).Release()
	}
	// The protected entry k1 must survive the promotions and evictions.
	if !c.Contains("k1") {
		t.Error("expected promoted entry k1 to survive")
	}
}

func TestLfuConfigValidation(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid config")
		}
	}()
	newLfu[string, int](4, LfuConfig{WindowCapacityRatio: 0.9, ProtectedCapacityRatio: 0.8, CmsEps: 0.01, CmsConfidence: 0.9})
}

func TestS3FifoPromotionAndGhost(t *testing.T) {
	// capacity 100: small capacity 10, ghost capacity 100.
	c := newTestCache(t, 100, func(b *Builder[string, int]) {
		b.WithEvictionConfig(S3FifoConfig{
			SmallQueueCapacityRatio:  0.1,
			GhostQueueCapacityRatio:  1.0,
			SmallToMainFreqThreshold: 1,
		})
	})

	for i := 0; i < 100; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}
	// The small queue is over its capacity (100 > 10).

	// Access k0 so it has enough frequency to be promoted to main.
	c.Get("k0").Release()

	// Inserting one more entry triggers eviction: k0 is promoted to main,
	// k1 (frequency 0) is evicted into the ghost queue.
	c.Insert("k100", 100).Release()
	if !c.Contains("k0") {
		t.Error("expected k0 to be promoted to the main queue")
	}
	if c.Contains("k1") {
		t.Error("expected k1 to be evicted")
	}
	if !c.Contains("k99") || !c.Contains("k100") {
		t.Error("expected k99 and k100 to be present")
	}

	// k1 is now in the ghost queue: re-inserting it goes straight into the
	// main queue and it survives the next eviction from the small queue.
	c.Insert("k1", 1).Release()
	c.Get("k2").Release()
	c.Insert("k101", 101).Release()
	if !c.Contains("k1") {
		t.Error("expected re-inserted k1 to stay in the main queue")
	}
}

func TestS3FifoConfigValidation(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid config")
		}
	}()
	newS3fifo[string, int](4, S3FifoConfig{SmallQueueCapacityRatio: 0})
}

func TestSieveSecondChance(t *testing.T) {
	c := newTestCache(t, 4, func(b *Builder[string, int]) {
		b.WithEvictionConfig(SieveConfig{})
	})

	for i := 0; i < 4; i++ {
		e := c.Insert(fmt.Sprintf("k%d", i), i)
		e.Release()
	}

	// Mark k1 as visited (a second chance).
	c.Get("k1").Release()

	// Insert k4: the hand starts at the front (k0, unvisited) -> k0 evicted.
	c.Insert("k4", 4).Release()
	if c.Contains("k0") {
		t.Error("expected k0 to be evicted")
	}
	if !c.Contains("k1") {
		t.Error("expected visited k1 to survive")
	}

	// Insert k5: the hand is at k1, which is visited -> second chance
	// cleared, k2 evicted.
	c.Insert("k5", 5).Release()
	if c.Contains("k2") {
		t.Error("expected k2 to be evicted")
	}
	if !c.Contains("k1") {
		t.Error("expected k1 to survive its second chance")
	}

	// Insert k6: after evicting k2 the hand sits at k3 (unvisited), so k3
	// is evicted while k1 keeps its remaining chance.
	c.Insert("k6", 6).Release()
	if c.Contains("k3") {
		t.Error("expected k3 to be evicted")
	}
	if !c.Contains("k1") {
		t.Error("expected k1 to still be present")
	}

	// With interleaved inserts the hand keeps chasing the tail and the
	// queue front (k1) is only re-examined after a wrap, so it survives.
	for i := 7; i < 12; i++ {
		c.Insert(fmt.Sprintf("k%d", i), i).Release()
		if !c.Contains("k1") {
			t.Fatal("expected k1 to survive while the hand chases the tail")
		}
	}
}

func TestSieveContainerBasic(t *testing.T) {
	// Container-level test mirroring foyer's `test_sieve_basic`.
	s := newSieve[uint64, uint64](100, SieveConfig{})
	recs := make([]*record[uint64, uint64], 7)
	for i := range recs {
		recs[i] = newRecord(uint64(i), uint64(i), common.Properties{}, uint64(i), 1)
	}
	dump := func() []*record[uint64, uint64] {
		var out []*record[uint64, uint64]
		for e := s.queue.Front(); e != nil; e = e.Next() {
			out = append(out, e.Value.(*record[uint64, uint64]))
		}
		return out
	}
	assertDump := func(want ...int) {
		t.Helper()
		got := dump()
		if len(got) != len(want) {
			t.Fatalf("queue = %v, want %v", got, want)
		}
		for i, w := range want {
			if got[i] != recs[w] {
				t.Fatalf("queue[%d] = %v, want %v", i, got[i].key, w)
			}
		}
	}

	s.push(recs[0])
	s.push(recs[1])
	s.push(recs[2])
	s.push(recs[3])
	assertDump(0, 1, 2, 3)

	s.acquire(recs[1])
	s.acquire(recs[3])

	// 0 is the oldest and unvisited, so it is evicted first.
	if got := s.pop(); got != recs[0] {
		t.Fatalf("first victim = %v, want 0", got.key)
	}
	// 1 is visited, so the scan moves on and evicts 2.
	if got := s.pop(); got != recs[2] {
		t.Fatalf("second victim = %v, want 2", got.key)
	}
	assertDump(1, 3)

	// The hand points at 3 (visited): it gets a second chance, the scan
	// wraps to the front and evicts 1.
	if got := s.pop(); got != recs[1] {
		t.Fatalf("third victim = %v, want 1", got.key)
	}
	assertDump(3)

	s.remove(recs[3])
	assertDump()

	s.push(recs[4])
	s.push(recs[5])
	s.push(recs[6])
	s.clear()
	assertDump()
}

func TestCmsBasics(t *testing.T) {
	cms := newCountMinSketch(suggestNumHashes(0.9), suggestNumBuckets(0.001))
	if cms.numHashes != 3 {
		t.Fatalf("numHashes = %d, want 3", cms.numHashes)
	}
	if cms.numBuckets != 2000 {
		t.Fatalf("numBuckets = %d, want 2000", cms.numBuckets)
	}
	for i := 0; i < 100; i++ {
		cms.update(42)
	}
	if got := cms.estimate(42); got < 99 {
		t.Fatalf("estimate(42) = %d, want ~100", got)
	}
	if got := cms.estimate(43); got != 0 {
		t.Fatalf("estimate(43) = %d, want 0", got)
	}
	cms.halve()
	if got := cms.estimate(42); got != 50 {
		t.Fatalf("estimate after halve = %d, want 50", got)
	}
}
