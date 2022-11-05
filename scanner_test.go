package chunkio

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSplit(t *testing.T) {
	Convey("It can split by newline", t, func() {
		fd, err := ioutil.TempFile(os.TempDir(), "scanner_test")
		So(err, ShouldBeNil)
		defer fd.Close()

		rfd, err := os.Open(fd.Name())
		So(err, ShouldBeNil)

		s := Scanner{
			FD:    rfd,
			Buf:   make([]byte, 1024),
			Split: SplitLines,
		}

		fd.WriteString("hello\nworld\n")
		fd.Sync()

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
	})
}
