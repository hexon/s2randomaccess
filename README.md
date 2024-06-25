# Random Access over an S2 compressed stream

S2 is an extension of [Snappy](https://github.com/google/snappy). This package builds on top of th exellent [github.com/klauspost/compress/s2](https://pkg.go.dev/github.com/klauspost/compress/s2).

This package is similar to [s2.ReadSeeker](https://pkg.go.dev/github.com/klauspost/compress/s2#ReadSeeker), but optimized for concurrent requests and caching decompression of blocks.

This package requires the entire stream as a `[]byte`, which should not be modified during the lifetime of the `*Seeker`.

```go
var compressedStream []byte

func baseLibrary() {
	seeker, err := s2.NewReader(bytes.NewReader(compressedStream)).ReadSeeker(true, nil)
	_, err := seeker.ReadAt(buf, uncompressedOffset)
}

func withS2RandomAccess() {
	seeker := s2randomaccess.Open(compressedStream)
	buf, deref, err := seeker.Get(uncompressedOffset, uncompressedLength)
	use(buf)
	deref()
}
```

## Allocators

The default allocator simply uses `make([]byte, n)` and never reuses memory. `WithAllocator(&SyncPoolAllocator{})` can be used to reuse buffers using `sync.Pool`s. `WithAllocator(s2ramalloc.Allocator{})` can be used to use malloc(3) and free(3), but requires CGO.

When using the default allocator, the returned deref functions can be ignored. With other allocators they must be called exactly once after being done with the returned slice.

Returned slices might point into memory allocated by the chosen Allocator, or straight into the input data if it was in an uncompressed data block in the stream.

## Caching

This library caches the last 100 decoded blocks globally. The number of globally cached blocks can be changed with `SetGlobalLRUSize(1000)` at any time.
