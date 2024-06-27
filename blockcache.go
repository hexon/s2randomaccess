package s2randomaccess

import (
	"github.com/Jille/easymutex"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/s2"
)

// globalLRU contains *decompressedBlocks that contain decompressed data, but are not actively in use.
var globalLRU, _ = lru.NewWithEvict[lruKey, *decompressedBlock](100, onEvicted)

type lruKey struct {
	seeker      *Seeker
	blockOffset int64
}

type decompressedBlock struct {
	lruKey
	decompressed []byte
	refcount     int // guarded by Seeker.mtx
}

// deref is called when a user of this library is done with the slice they got from Get().
func (d *decompressedBlock) deref() {
	d.seeker.mtx.Lock()
	d.refcount--
	if d.refcount == 0 {
		delete(d.seeker.active, d.blockOffset)
		defer globalLRU.Add(d.lruKey, d)
	}
	d.seeker.mtx.Unlock()
}

// onEvicted is called when a decompressedBlock is thrown out of the LRU cache.
func onEvicted(k lruKey, d *decompressedBlock) {
	d.seeker.mtx.Lock()
	if d.refcount == 0 {
		d.refcount = -666
		d.seeker.allocator.Free(d.decompressed)
		d.decompressed = nil
	}
	d.seeker.mtx.Unlock()
}

// getDecompressedBlock finds the S2 block
func (s *Seeker) getDecompressedBlock(offset, length int64) ([]byte, func(), error) {
	em := easymutex.LockMutex(&s.mtx)
	defer em.Unlock()
	if db, ok := s.active[offset]; ok {
		db.refcount++
		return db.decompressed, db.deref, nil
	}
	k := lruKey{s, offset}
	if db, ok := globalLRU.Get(k); ok && db.refcount >= 0 {
		db.refcount++
		return db.decompressed, db.deref, nil
	}
	em.Unlock()
	buf := s.allocator.Alloc(int(length))
	decoded, err := s2.Decode(buf, s.data[offset:][:length])
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
