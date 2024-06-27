package s2randomaccess

import (
	"math"
	"sync"
)

// Allocator defines the methods a custom allocator needs to have.
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

// WithAllocator can be passed to New() to use an alternative allocator.
func WithAllocator(a Allocator) Option {
	return func(s *Seeker) error {
		s.allocator = a
		return nil
	}
}

const (
	SyncPoolAllocatorSkipBuckets   = 6
	SyncPoolAllocatorLargestBucket = 33
)

// SyncPoolAllocator is an Allocator that uses one `sync.Pool` for each n**2 for n in (SyncPoolAllocatorSkipBuckets, SyncPoolAllocatorLargestBucket].
type SyncPoolAllocator struct {
	Pools [SyncPoolAllocatorLargestBucket - SyncPoolAllocatorSkipBuckets]sync.Pool
}

func (a *SyncPoolAllocator) Alloc(n int) []byte {
	class := int(math.Ceil(math.Log2(float64(n))))
	if class < SyncPoolAllocatorSkipBuckets || class >= SyncPoolAllocatorLargestBucket {
		// Too small/big for the predeclared classes.
		return make([]byte, n)
	}
	for {
		ret := a.Pools[class-SyncPoolAllocatorSkipBuckets].Get()
		if ret == nil {
			return make([]byte, n, 1<<class)
		}
		b := ret.([]byte)
		if n <= cap(b) {
			return b[:n]
		}
		// Someone put a too-small buffer into this bucket. Drop it.
	}
}

func (a *SyncPoolAllocator) Free(b []byte) {
	class := int(math.Floor(math.Log2(float64(cap(b)))))
	if class < SyncPoolAllocatorSkipBuckets || class >= SyncPoolAllocatorLargestBucket {
		// Too small/big for the predeclared classes.
		return
	}
	a.Pools[class-SyncPoolAllocatorSkipBuckets].Put(b)
}
