package filecache

import (
	"io"
	"os"
)

type WritableFile interface {
	io.WriteCloser
	GetFile() *os.File
	Cancel()
}

type writableFile struct {
	f          *os.File
	afterClose func(good bool)
}

func (w *writableFile) GetFile() *os.File {
	return w.f
}

func (w *writableFile) Write(p []byte) (n int, err error) {
	return w.f.Write(p)
}

func (w *writableFile) Close() error {
	if err := w.f.Close(); err != nil {
		os.Remove(w.f.Name())
		return err
	}
	w.afterClose(true)
	return nil
}

func (w *writableFile) Cancel() {
	w.f.Close()
	w.afterClose(false)
}
