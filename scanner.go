package chunkio

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type Chunk struct {
	Start int64
	End   int64
	Raw   []byte
}

type ChunkSplit func(data []byte, atEOF bool) (advance int, token []byte, err error)

var ErrChunkTooBig error = errors.New("chunk size is bigger than buffer size")

type Scanner struct {
	FD        *os.File
	Split     ChunkSplit
	Buf       []byte
	totalRead int
	iBufRead  int
	iBufWrite int
	err       error
	eof       error
	lastChunk Chunk
}

func (s *Scanner) Scan() bool {
	if s.err != nil {
		return false
	}

	if s.eof == nil && s.iBufWrite < len(s.Buf) {
		n, err := s.FD.Read(s.Buf[s.iBufWrite:])
		if err != nil {
			if err == io.EOF {
				s.eof = err
			} else {
				s.err = err
			}
		}
		s.iBufWrite += n
		if s.iBufWrite == len(s.Buf) {
			// buffer full, check EOF
			// this is necessary because if the buffer is just enough to fit the last chunk,
			// s.FD.Read(s.Buf[s.iBufWrite:]) will never return io.EOF
			// a zero read always gets (0, nil)
			cur, err := s.FD.Seek(0, io.SeekCurrent)
			if err != nil {
				s.err = err
				return false
			}

			fi, err := s.FD.Stat()
			if err != nil {
				s.err = err
				return false
			}

			if cur == fi.Size() {
				s.eof = io.EOF
			}
		}
	}

	var adv int
	var chunk []byte
	var err error

	for i := s.iBufRead; i < s.iBufWrite; i += adv {
		adv, chunk, err = s.Split(s.Buf[i:s.iBufWrite], s.eof != nil)
		if err != nil {
			s.err = err
		}

		if adv == 0 {
			if s.eof != nil {
				return false
			}

			break
		}

		if chunk != nil {
			s.lastChunk = Chunk{
				Start: int64(s.totalRead),
				End:   int64(s.totalRead + adv),
				Raw:   chunk,
			}
			s.totalRead += adv
			s.iBufRead += adv
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
	} else if s.iBufRead > 0 {
		n := copy(s.Buf[0:], s.Buf[s.iBufRead:s.iBufWrite])
		s.iBufRead = 0
		s.iBufWrite = n
	}

	return s.Scan()
}

func (s *Scanner) ResetEOF() bool {
	if s.eof == io.EOF {
		s.eof = nil
		s.err = nil
		s.FD.Seek(0, io.SeekCurrent)
		return true
	}

	return false
}

func (s Scanner) Chunk() Chunk {
	return s.lastChunk
}

func (s Scanner) Err() error {
	return s.err
}
