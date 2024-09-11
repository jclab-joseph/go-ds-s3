package filecache

import "os"

type noopFileCache struct {
}

func NewNoop() FileCache {
	return &noopFileCache{}
}

func (n *noopFileCache) Start() error {
	return nil
}

func (n *noopFileCache) Stop() {
}

func (n *noopFileCache) IsActive() bool {
	return true
}

func (n *noopFileCache) Open(name string) (*os.File, error) {
	f, err := os.Open(name)
	if err != nil && os.IsNotExist(err) {
		return nil, ItemNotInCache
	}
	return f, err
}

func (n *noopFileCache) Create(name string) (WritableFile, error) {
	return &noopWritableFile{}, nil
}

func (n *noopFileCache) Remove(name string) {}

type noopWritableFile struct{}

func (w *noopWritableFile) GetFile() *os.File {
	return nil
}

func (w *noopWritableFile) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (w *noopWritableFile) Close() error {
	return nil
}

func (w *noopWritableFile) Cancel() {}
