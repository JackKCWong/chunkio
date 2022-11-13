package chunkio

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestScan(t *testing.T) {
	Convey("it can split by newline", t, func() {
		fd, err := ioutil.TempFile(os.TempDir(), "scanner_test")
		So(err, ShouldBeNil)
		defer fd.Close()

		fd.WriteString("hello\nworld\n")
		fd.Sync()

		assertScannerWithBufSizeCanDo := func(n int) {
			rfd, err := os.Open(fd.Name())
			So(err, ShouldBeNil)

			s := Scanner{
				FD:    rfd,
				Buf:   make([]byte, n),
				Split: bufio.ScanLines,
			}

			hasNext := s.Scan()
			So(hasNext, ShouldBeTrue)
			So(s.Err(), ShouldBeNil)

			chunk := s.Chunk()
			So(chunk, ShouldResemble, Chunk{
				Start: 0,
				End:   6,
				Raw:   []byte("hello"),
			})

			hasNext = s.Scan()
			So(hasNext, ShouldBeTrue)
			So(s.Err(), ShouldBeNil)

			chunk = s.Chunk()
			So(chunk, ShouldResemble, Chunk{
				Start: 6,
				End:   12,
				Raw:   []byte("world"),
			})

			hasNext = s.Scan()
			So(hasNext, ShouldBeFalse)
			So(s.Err(), ShouldEqual, io.EOF)
		}

		Convey("when the buffer is smaller than the file", func() {
			assertScannerWithBufSizeCanDo(10)
		})
		Convey("when the buffer is bigger than the file", func() {
			assertScannerWithBufSizeCanDo(1024)
		})
		Convey("when the buffer is same size as the file", func() {
			assertScannerWithBufSizeCanDo(12)
		})
		Convey("when the buffer is same size as chunks", func() {
			assertScannerWithBufSizeCanDo(6)
		})
		Convey("when the buffer is smaller than a chunk", func() {
			rfd, err := os.Open(fd.Name())
			So(err, ShouldBeNil)

			s := Scanner{
				FD:    rfd,
				Buf:   make([]byte, 5),
				Split: bufio.ScanLines,
			}

			hasNext := s.Scan()
			So(hasNext, ShouldBeFalse)
			So(errors.Is(s.Err(), ErrChunkTooBig), ShouldBeTrue)
		})
	})
}

func TestResetEOF(t *testing.T) {
	Convey("it can resume from EOF", t, func() {
		fd, err := os.CreateTemp(os.TempDir(), "TestResetEOF")
		So(err, ShouldBeNil)

		fd.Write([]byte("hello\n"))
		fd.Sync()

		rfd, err := os.Open(fd.Name())
		So(err, ShouldBeNil)

		s := Scanner{
			FD:    rfd,
			Split: bufio.ScanLines,
			Buf:   make([]byte, 1024),
		}

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk := s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 0,
			End:   6,
			Raw:   []byte("hello"),
		})
		So(s.Scan(), ShouldBeFalse)
		So(s.Err(), ShouldEqual, io.EOF)

		fd.Write([]byte("world\n"))
		fd.Sync()

		So(s.ResetEOF(), ShouldBeTrue)
		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk = s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 6,
			End:   12,
			Raw:   []byte("world"),
		})
	})
}

func TestCustomSplitFunc(t *testing.T) {
	Convey("it can use custom split function", t, func() {
		fd, err := os.CreateTemp(os.TempDir(), "TestResetEOF")
		So(err, ShouldBeNil)

		fd.Write([]byte("newline: hi\nnewline: hello\nworld\n"))
		fd.Sync()

		rfd, err := os.Open(fd.Name())
		So(err, ShouldBeNil)

		tokStart := []byte("newline: ")
		tokEnd := []byte("\nnewline: ")
		s := Scanner{
			FD: rfd,
			Split: func(data []byte, atEOF bool) (int, []byte, error) {
				if len(data) < len(tokStart) {
					return 0, nil, nil
				}

				if !bytes.HasPrefix(data, tokStart) {
					return 0, nil, errors.New("invalid token start")
				}

				if i := bytes.Index(data, tokEnd); i > 0 {
					return i + 1, data[:i], nil
				}

				if atEOF {
					if data[len(data)-1] == '\n' {
						return len(data), data[:len(data)-1], nil
					} else {
						return 0, nil, errors.New("chunk too big")
					}
				}

				return 0, nil, nil
			},
			Buf: make([]byte, 21), // when the buffer size is just enough to fit the last chunk, it gets tricky
		}

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk := s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 0,
			End:   12,
			Raw:   []byte("newline: hi"),
		})

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk = s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 12,
			End:   33,
			Raw:   []byte("newline: hello\nworld"),
		})
	})
}
