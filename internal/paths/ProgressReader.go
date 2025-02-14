package paths

import (
	"io"
)

type ProgressFn func(size int)

type ProgressReader struct {
	reader io.Reader
	fn     ProgressFn
}

func NewProgressReader(reader io.Reader, fn ProgressFn) *ProgressReader {
	return &ProgressReader{
		reader: reader,
		fn:     fn,
	}
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	pr.fn(n)
	return n, err
}
