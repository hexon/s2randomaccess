// Package s2ramalloc provides a `s2randomacess.Allocator` that uses calloc(3).
// It requires cgo.
package s2ramalloc

// #include <malloc.h>
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Allocator is an allocator that uses calloc(3) and free(3).
type Allocator struct{}

func (Allocator) Alloc(n int) []byte {
	p := C.calloc(C.ulong(n), 1)
	if p == nil {
		panic(fmt.Errorf("out of memory: calloc(%d, 1) returned nil", n))
	}
	out := &reflect.SliceHeader{
		Data: uintptr(p),
		Len:  n,
		Cap:  n,
	}
	return *(*[]byte)(unsafe.Pointer(out))
}

func (Allocator) Free(b []byte) {
	C.free(unsafe.Pointer(&b[0]))
}
