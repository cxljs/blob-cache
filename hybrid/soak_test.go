package hybrid

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"blob-cache/common"
	"blob-cache/storage"
)

// validValue reports whether v is one of the pooled values.
func validValue(v []byte, pool [][]byte) bool {
	for _, p := range pool {
		if bytes.Equal(v, p) {
			return true
		}
	}
	return false
}

func randomLocation(r *rand.Rand) common.Properties {
	switch r.Intn(3) {
	case 0:
		return common.Properties{}.WithLocation(common.LocationInMem)
	case 1:
		return common.Properties{}.WithLocation(common.LocationOnDisk)
	default:
		return common.Properties{}
	}
}

// TestHybridConcurrencySoak hammers a hybrid cache from many goroutines
// with a randomized mix of Insert / Get / GetOrFetch / Remove / Contains /
// Wait, exercising the memory cache, the disk layer, the pipe pump and the
// in-flight dedup concurrently. The invariant: a Get/GetOrFetch hit always
// returns one of the pooled values for the run (never a foreign value,
// never a panic), and the cache stays usable through the whole run.
func TestHybridConcurrencySoak(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	c, err := NewBuilder[string, []byte](4096).WithStorage(dir, func(sb *storage.StoreBuilder[string, []byte]) {
		sb.WithCodec(storage.BytesCodec()).WithBlobFileSize(16 << 10).WithTotalCapacity(8 << 20)
	}).Build()
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const opsPerWorker = 1500

	values := make([][]byte, 8)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("value-%d-pad-", i))
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)*1000 + 7))
			for op := 0; op < opsPerWorker; op++ {
				key := fmt.Sprintf("key-%d", r.Intn(64))
				switch r.Intn(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
					e := c.Insert(key, values[r.Intn(len(values))])
					e.Release()
				case 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39:
					e, err := c.Get(ctx, key)
					if err != nil {
						t.Errorf("get: %v", err)
						return
					}
					if !e.IsNil() {
						if !validValue(e.Value(), values) {
							t.Errorf("get returned a foreign value: %q", e.Value())
							return
						}
						e.Release()
					}
				case 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59:
					e, err := c.GetOrFetch(ctx, key, func() ([]byte, error) {
						return values[r.Intn(len(values))], nil
					})
					if err != nil {
						t.Errorf("getOrFetch: %v", err)
						return
					}
					if !e.IsNil() {
						if !validValue(e.Value(), values) {
							t.Errorf("getOrFetch returned a foreign value: %q", e.Value())
							return
						}
						e.Release()
					}
				case 60, 61, 62, 63, 64, 65, 66, 67, 68, 69:
					e := c.InsertWithProperties(key, values[r.Intn(len(values))], randomLocation(r))
					e.Release()
				case 70, 71, 72, 73, 74:
					c.Remove(key)
				case 75, 76, 77, 78, 79:
					_ = c.Contains(key)
				case 80, 81, 82, 83, 84:
					e, err := c.GetOrFetchWithProperties(ctx, key, func() ([]byte, error) {
						return values[r.Intn(len(values))], nil
					}, randomLocation(r))
					if err != nil {
						t.Errorf("getOrFetchWithProperties: %v", err)
						return
					}
					if !e.IsNil() {
						if !validValue(e.Value(), values) {
							t.Errorf("getOrFetchWithProperties returned a foreign value: %q", e.Value())
							return
						}
						e.Release()
					}
				default:
					c.Wait(ctx)
					_ = c.Contains(key)
				}
			}
		}(w)
	}

	// A background writer keeps churn going on separate keys.
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			e := c.Insert(fmt.Sprintf("churn-%d", i%32), []byte(fmt.Sprintf("churn-%d", i)))
			e.Release()
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	// Stop the churn writer BEFORE waiting: it only exits when stop is
	// closed, so waiting first would deadlock.
	close(stop)
	wg.Wait()
	c.Wait(ctx)
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}
