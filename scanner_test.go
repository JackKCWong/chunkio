package chunkio

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHappyFlow(t *testing.T) {
	Convey("it can split by newline", t, func() {
		fd, err := ioutil.TempFile(os.TempDir(), "scanner_test")
		So(err, ShouldBeNil)
		defer fd.Close()

		fd.WriteString("hello\nworld\n")
		fd.Sync()

		testScannerWithBufSize := func(n int) {
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
			testScannerWithBufSize(10)
		})
		Convey("when the buffer is bigger than the file", func() {
			testScannerWithBufSize(1024)
		})
		Convey("when the buffer is same size as the file", func() {
			testScannerWithBufSize(12)
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
