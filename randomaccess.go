package s2randomaccess

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

type Seeker struct {
	data []byte
	idx  s2.Index

	mtx    sync.Mutex
	active map[int64]*decompressedBlock

	allocator       Allocator
	indexGiven      bool
	allowBuildIndex bool
}

type Option func(*Seeker) error

func New(data []byte, options ...Option) (*Seeker, error) {
	ret := Seeker{
		data:   data,
		active: map[int64]*decompressedBlock{},
	}
	for _, o := range options {
		if err := o(&ret); err != nil {
			return nil, err
		}
	}
	if err := ret.loadIndex(); err != nil {
		if err != s2.ErrUnsupported {
			return nil, err
		}
	}
	return &ret, nil
}

// WithIndex passes the index rather than having it loaded from the stream.
func WithIndex(idx s2.Index) Option {
	return func(s *Seeker) error {
		s.idx = idx
		s.indexGiven = true
		return nil
	}
}

// WithAllowBuildIndex fall back to indexing the stream ourselves if it isn't present in the stream.
func WithAllowBuildIndex() Option {
	return func(s *Seeker) error {
		s.allowBuildIndex = true
		return nil
	}
}

func (s *Seeker) loadIndex() error {
	if s.indexGiven {
		// Given through WithIndex.
		return nil
	}
	if err := s.idx.LoadStream(bytes.NewReader(s.data)); err != nil {
		if err != s2.ErrUnsupported {
			return err
		}
	}
	if !s.allowBuildIndex {
		return errors.New("s2randomaccess: didn't find index in data and WithAllowBuildIndex() is not enabled")
	}
	idx, err := s2.IndexStream(bytes.NewReader(s.data))
	if err != nil {
		return err
	}
	if _, err := s.idx.Load(idx); err != nil {
		return err
	}
	return nil
}

const (
	headerSize                = 4
	checksumSize              = 4
	chunkTypeCompressedData   = 0x00
	chunkTypeUncompressedData = 0x01
)

func (s *Seeker) Get(offset, length int64) ([]byte, func(), error) {
	comprOff, uncomprOff, err := s.idx.Find(offset)
	if err != nil {
		return nil, nil, err
	}
	skipUncompr := offset - uncomprOff
	var partial []byte
	partialDeref := noop
	for comprOff < int64(len(s.data)) {
		chunkHeader := s.data[comprOff:][:headerSize]
		chunkType := chunkHeader[0]
		chunkLen := int(chunkHeader[1]) | int(chunkHeader[2])<<8 | int(chunkHeader[3])<<16
		chunk := s.data[comprOff+headerSize:][:chunkLen]

		var plain []byte
		var plainDeref func()

		switch chunkType {
		case chunkTypeCompressedData:
			dLen, err := s2.DecodedLen(chunk[checksumSize:])
			if err != nil {
				partialDeref()
				return nil, nil, err
			}
			if skipUncompr >= int64(dLen) {
				skipUncompr -= int64(dLen)
				break
			}
			block, deref, err := s.getDecompressedBlock(comprOff+headerSize+checksumSize, len(chunk)-checksumSize, dLen)
			if err != nil {
				partialDeref()
				return nil, nil, err
			}
			plain = block[skipUncompr:]
			plainDeref = deref
		case chunkTypeUncompressedData:
			dLen := len(chunk) - checksumSize
			if skipUncompr >= int64(dLen) {
				skipUncompr -= int64(dLen)
				break
			}
			plain = chunk[checksumSize+skipUncompr:]
			plainDeref = noop
		default:
			if chunkType <= 0x7f {
				// Unknown reserved unskippable chunk
				partialDeref()
				return nil, nil, s2.ErrUnsupported
			}
		}
		if plain != nil {
			if partial == nil {
				if length <= int64(len(plain)) {
					return plain[:length], plainDeref, nil
				}
				partial = s.allocator.Alloc(int(length))[:0]
				partialDeref = func() {
					s.allocator.Free(partial)
				}
			}
			partial = append(partial, plain...)
			plainDeref()
			if len(partial) == int(length) {
				return partial, partialDeref, nil
			}
			skipUncompr = 0
		}
		comprOff += headerSize + int64(chunkLen)
	}
	partialDeref()
	return nil, nil, io.ErrUnexpectedEOF
}

func noop() {
}
