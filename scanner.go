package chunkio

import (
	"errors"
	"fmt"
	"io"
)

type Chunk struct {
	Start int64
	End   int64
	Raw   []byte
}

type ChunkSplit func(i int, buf []byte) bool

var SplitLines ChunkSplit = func(i int, buf []byte) bool {
	return buf[i] == '\n'
}

var ErrChunkTooBig error = errors.New("chunk size is bigger than buffer size")

type Scanner struct {
	FD        io.ReadSeeker
	Split     ChunkSplit
	Buf       []byte
	iBufRead  int
	iBufWrite int
	err       error
	eof       error
	lastChunk Chunk
}

func (s *Scanner) Scan() bool {
	if s.err != nil && s.iBufRead == s.iBufWrite {
		return false
	}

	if s.eof == nil {
		n, err := s.FD.Read(s.Buf[s.iBufWrite:])
		if err != nil {
			if err == io.EOF {
				s.eof = err
			} else {
				s.err = err
			}
		}
		s.iBufWrite += n
	}

	for i := s.iBufRead; i < s.iBufWrite; i++ {
		if s.Split(i, s.Buf) {
			s.lastChunk = Chunk{
				Start: int64(s.iBufRead),
				End:   int64(i) + 1,
				Raw:   s.Buf[s.iBufRead : i+1],
			}
			s.iBufRead = i + 1
			return true
		}
	}

	if s.iBufRead == 0 && s.iBufWrite == len(s.Buf) {
		// the whole buffer has no chunk
		s.err = fmt.Errorf("%w: %d", ErrChunkTooBig, len(s.Buf))
		return false
	}

	if s.iBufRead == s.iBufWrite {
		// buffered data exhausted
		if s.eof != nil {
			s.err = s.eof
			return false
		} else {
			s.iBufRead = 0
			s.iBufWrite = 0
		}
	} else {
		n := copy(s.Buf[0:], s.Buf[s.iBufRead:])
		s.iBufRead = 0
		s.iBufWrite = n
	}

	return s.Scan()
}

func (s *Scanner) ResetEOF() bool {
	return false
}

func (s Scanner) Chunk() Chunk {
	return s.lastChunk
}

func (s Scanner) Err() error {
	return s.err
}