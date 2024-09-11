package filecache

import (
	"os"
	"sync"
	"time"
)

type cacheItem struct {
	clock      Clock
	name       string
	lock       sync.Mutex
	path       string
	size       int64
	lastaccess time.Time
	modified   time.Time
}

func (item *cacheItem) Name() string {
	return item.name
}

func (item *cacheItem) RemoveFile() error {
	return os.Remove(item.path)
}

func (item *cacheItem) Access() string {
	item.lock.Lock()
	defer item.lock.Unlock()
	item.lastaccess = item.clock.Now()
	return item.path
}

func (item *cacheItem) Size() int64 {
	return item.size
}

func (item *cacheItem) Duration() time.Duration {
	item.lock.Lock()
	defer item.lock.Unlock()
	return item.clock.Now().Sub(item.lastaccess)
}

func DefaultCacheFile(clock Clock, name string, path string) (itm CacheItem, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return
	} else if fi.Mode().IsDir() {
		return nil, ItemIsDirectory
	}

	itm = &cacheItem{
		path:       path,
		name:       name,
		clock:      clock,
		size:       fi.Size(),
		modified:   fi.ModTime(),
		lastaccess: clock.Now(),
	}
	return
}

func DefaultCacheComparer(a CacheItem, b CacheItem) int {
	if a.Duration() > b.Duration() {
		return -1
	} else if a.Duration() < b.Duration() {
		return 1
	}
	return 0
}
