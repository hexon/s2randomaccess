package s2randomaccess

import (
	"math/rand"
	"testing"
)

func TestSyncPoolAllocator(t *testing.T) {
	var a SyncPoolAllocator
	for i := 0; 1000000 > i; i++ {
		n := rand.Intn(1 << 20)
		b := a.Alloc(n)
		a.Free(b)
		if i%100 == 99 {
			for n := range a.Pools {
				for a.Pools[n].Get() != nil {
				}
			}
			a.Free(make([]byte, rand.Intn(1<<20)))
		}
	}
}
