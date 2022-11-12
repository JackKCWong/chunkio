package chunkio

import (
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
				Split: SplitLines,
			}

			hasNext := s.Scan()
			So(hasNext, ShouldBeTrue)
			So(s.Err(), ShouldBeNil)

			chunk := s.Chunk()
			So(chunk, ShouldResemble, Chunk{
				Start: 0,
				End:   6,
				Raw:   []byte("hello\n"),
			})

			hasNext = s.Scan()
			So(hasNext, ShouldBeTrue)
			So(s.Err(), ShouldBeNil)

			chunk = s.Chunk()
			So(chunk, ShouldResemble, Chunk{
				Start: 6,
				End:   12,
				Raw:   []byte("world\n"),
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
		Convey("when the buffer is smaller than a chunk", func() {
			rfd, err := os.Open(fd.Name())
			So(err, ShouldBeNil)

			s := Scanner{
				FD:    rfd,
				Buf:   make([]byte, 5),
				Split: SplitLines,
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
			Split: SplitLines,
			Buf:   make([]byte, 1024),
		}

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk := s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 0,
			End:   6,
			Raw:   []byte("hello\n"),
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
			Raw:   []byte("world\n"),
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

		s := Scanner{
			FD: rfd,
			Split: func(buf []byte, atEOF bool) bool {
				if buf[0] != '\n' {
					return false
				}

				if len(buf) == 1 && atEOF {
					return true
				}

				if 9 > len(buf) {
					return false
				}

				sol := buf[:9]
				return bytes.Equal(sol, []byte("\nnewline:"))
			},
			Buf: make([]byte, 22),
		}

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk := s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 0,
			End:   12,
			Raw:   []byte("newline: hi\n"),
		})

		So(s.Scan(), ShouldBeTrue)
		So(s.Err(), ShouldBeNil)
		chunk = s.Chunk()
		So(chunk, ShouldResemble, Chunk{
			Start: 12,
			End:   33,
			Raw:   []byte("newline: hello\nworld\n"),
		})
	})
}
