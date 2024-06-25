package s2randomaccess

import (
	"math/bits"
	"sync"
)

type Allocator interface {
	Alloc(n int) []byte
	Free([]byte)
}

type defaultAllocator struct{}

func (defaultAllocator) Alloc(n int) []byte {
	return make([]byte, n)
}

func (defaultAllocator) Free(b []byte) {
}

func WithAllocator(a Allocator) Option {
	return func(s *Seeker) error {
		s.allocator = a
		return nil
	}
}

const (
	SyncPoolAllocatorMinSize       = (1 << (SyncPoolAllocatorSkipBuckets - 1)) + 1
	SyncPoolAllocatorSkipBuckets   = 6
	SyncPoolAllocatorLargestBucket = 33
)

type SyncPoolAllocator struct {
	Pools [SyncPoolAllocatorLargestBucket - SyncPoolAllocatorSkipBuckets]sync.Pool
}

func (a *SyncPoolAllocator) Alloc(n int) []byte {
	if n < SyncPoolAllocatorMinSize {
		// Too small for the overhead of using a pool.
		return make([]byte, n)
	}
	class := bucketForSize(n)
	if class >= SyncPoolAllocatorLargestBucket {
		// Too big for the predeclared classes.
		return make([]byte, n)
	}
	if ret := a.Pools[class-SyncPoolAllocatorSkipBuckets].Get(); ret != nil {
		return ret.([]byte)[:n]
	}
	return make([]byte, n, 1<<class)
}

func (a *SyncPoolAllocator) Free(b []byte) {
	if cap(b) < SyncPoolAllocatorMinSize {
		return
	}
	class := bucketForSize(cap(b))
	if class >= SyncPoolAllocatorLargestBucket {
		// Too big for the predeclared classes.
		return
	}
	a.Pools[class-SyncPoolAllocatorSkipBuckets].Put(b)
}

func bucketForSize(n int) int {
	return 64 - bits.LeadingZeros64(uint64(n)-1)
}
