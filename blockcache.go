package s2randomaccess

import (
	"github.com/Jille/easymutex"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/s2"
)

// globalLRU contains *decompressedBlocks that contain decompressed data, but are not actively in use.
var globalLRU, _ = lru.NewWithEvict[lruKey, *decompressedBlock](100, onEvicted)

type lruKey struct {
	inner       *inner
	blockOffset int64
}

type decompressedBlock struct {
	lruKey
	decompressed []byte
	refcount     int // guarded by Seeker.mtx
}

// deref is called when a user of this library is done with the slice they got from Get().
func (d *decompressedBlock) deref() {
	d.inner.mtx.Lock()
	d.refcount--
	if d.refcount == 0 {
		delete(d.inner.active, d.blockOffset)
		if d.inner.dying {
			d.free()
		} else {
			found, _ := globalLRU.ContainsOrAdd(d.lruKey, d)
			if found {
				// This block was already in the cache. Likely this block was requested twice at the same time. (We're not holding any lock during decompression, so this can happen.)
				d.free()
			}
		}
	}
	d.inner.mtx.Unlock()
}

// onEvicted is called when a decompressedBlock is thrown out of the LRU cache.
func onEvicted(k lruKey, d *decompressedBlock) {
	if d.inner.mtx.TryLock() {
		onEvicted_locked(d)
	} else {
		// Someone has the lock, possibly this eviction was caused by an Add() holding this lock. Clean up asynchronously.
		go func() {
			d.inner.mtx.Lock()
			onEvicted_locked(d)
		}()
	}
}

func onEvicted_locked(d *decompressedBlock) {
	if d.refcount == 0 {
		d.free()
	}
	d.inner.mtx.Unlock()
}

func (d *decompressedBlock) free() {
	d.refcount = -666
	d.inner.allocator.Free(d.decompressed)
	d.decompressed = nil
}

// getDecompressedBlock finds the S2 block
func (s *Seeker) getDecompressedBlock(offset int64, compressedLength, uncompressedLength int) ([]byte, func(), error) {
	em := easymutex.LockMutex(&s.mtx)
	defer em.Unlock()
	if db, ok := s.active[offset]; ok {
		db.refcount++
		return db.decompressed, db.deref, nil
	}
	k := lruKey{s.inner, offset}
	if db, ok := globalLRU.Get(k); ok && db.refcount >= 0 {
		db.refcount++
		return db.decompressed, db.deref, nil
	}
	em.Unlock()
	buf := s.allocator.Alloc(uncompressedLength)
	decoded, err := s2.Decode(buf, s.data[offset:][:compressedLength])
	if err != nil {
		return nil, nil, err
	}
	em.Lock()
	db := &decompressedBlock{k, decoded, 1}
	s.active[offset] = db
	return decoded, db.deref, err
}

// SetGlobalLRUSize configures the number of blocks that can be in the global compressed blocks LRU at any time.
func SetGlobalLRUSize(n int) {
	globalLRU.Resize(n)
}

// PurgeGlobalCache purges the global LRU cache that holds decompressed blocks.
// It is safe to call this function at any time.
func PurgeGlobalCache() {
	globalLRU.Purge()
}

func (i *inner) removeFromGlobalCache() {
	i.mtx.Lock()
	i.dying = true
	i.mtx.Unlock()
	for _, d := range globalLRU.Values() {
		if d.inner == i {
			// Remove triggers onEvicted.
			globalLRU.Remove(d.lruKey)
		}
	}
}
