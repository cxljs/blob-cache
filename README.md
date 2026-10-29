# blob-cache

`blob-cache` is a hybrid cache library written in Go. It is a Go rewrite of
[foyer](https://github.com/foyer-rs/foyer), a Rust hybrid cache (in-memory +
disk) library.

## Features

- In-memory cache with pluggable eviction algorithms:
  - LRU (with high-priority pool and pin support)
  - FIFO
  - LFU (W-TinyLFU style, with count-min sketch frequency estimation)
  - S3-FIFO
  - Sieve
- Generic API based on Go type parameters for both keys and values.
- Reference-counted cache entries, entry weight (via custom weighter), and
  entry-level hints.
- In-flight `GetOrFetch` support with per-key fetch deduplication
  (singleflight).
- Disk (blob) cache engine:
  - Entries are batched and written into append-only blob files.
  - In-memory indexer maps hash -> (blob id, offset, length).
  - Recovery by scanning blob files with per-entry checksum validation.
  - Space reclaim (vacuum) with pluggable blob pickers (FIFO, invalid-ratio).
- Hybrid cache that combines the in-memory cache and the disk cache with a
  single API, including fetch-on-miss with deduplication.

## Status

This is a work-in-progress Go rewrite of foyer. The API mirrors foyer's
concepts (`Cache`, `Store`, `HybridCache`, eviction configs, storage
filters...) while following Go idioms.

## Usage

See `examples/` for runnable examples.

```go
hybridCache, err := hybrid.NewBuilder[string, []byte]().
    WithMemoryOptions(memory.WithCapacity(64 * 1024 * 1024)).
    WithStorageOptions(storage.WithDir("./cache-data")).
    Build()
```

## License

Apache-2.0, the same as foyer.
