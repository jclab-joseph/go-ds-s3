package filecache

import (
	"errors"
	golog "github.com/ipfs/go-log/v2"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// File size constants for use with FileCache.MaxSize.
// For example, cache.MaxSize = 64 * Megabyte
const (
	Kilobyte = 1024
	Megabyte = 1024 * 1024
	Gigabyte = 1024 * 1024 * 1024
)

var (
	DefaultMaxSize  int64         = 16 * Megabyte
	DefaultMaxItems int           = 4096
	DefaultEvery    time.Duration = time.Minute // 1 minute
)

var (
	ItemIsDirectory = errors.New("can't cache a Directory")
	ItemNotInCache  = errors.New("item not in cache")
	ItemTooLarge    = errors.New("item too large for cache")
)

// CacheItem Immutable Cache Item
type CacheItem interface {
	Name() string
	RemoveFile() error
	Access() string
	Size() int64
	Duration() time.Duration
}

type FileCache interface {
	Start() error
	Stop()
	IsActive() bool
	Open(name string) (*os.File, error)
	Create(name string) (WritableFile, error)
	Remove(name string)
}

type CheckExpired func(cache FileCache, item CacheItem) bool
type CacheComparer func(a CacheItem, b CacheItem) int

type FileCacheImpl struct {
	clock Clock

	mutex     sync.Mutex
	Directory string
	capacity  int64

	items    map[string]CacheItem
	shutdown chan interface{}
	wait     sync.WaitGroup
	MaxItems int   // Maximum number of files to cache
	MaxSize  int64 // Maximum file size to store

	CacheFile func(name string, path string) (CacheItem, error)

	// CheckExpired if return true, delete item
	CheckExpired CheckExpired

	// CacheComparer oldest is first
	CacheComparer func(a CacheItem, b CacheItem) int

	GCPeriod time.Duration // Run an expiration check GCPeriod seconds
}

var logging = golog.Logger("filecache")

// NewDefaultCache returns a new FileCache with sane defaults.
func NewDefaultCache(directory string, checkExpire CheckExpired, cacheComparer CacheComparer) *FileCacheImpl {
	return &FileCacheImpl{
		clock: &realClock{},
		items: nil,

		Directory:     directory,
		MaxItems:      DefaultMaxItems,
		MaxSize:       DefaultMaxSize,
		CheckExpired:  checkExpire,
		CacheComparer: cacheComparer,
		GCPeriod:      DefaultEvery,
		CacheFile: func(name string, path string) (itm CacheItem, err error) {
			return DefaultCacheFile(&realClock{}, name, path)
		},
	}
}

// Start activates the file cache; it will start up the background
// automatic cache expiration goroutines and initialise the internal
// data structures.
func (cache *FileCacheImpl) Start() error {
	if cache.CacheComparer == nil {
		cache.CacheComparer = DefaultCacheComparer
	}
	if cache.CacheFile == nil {
		cache.CacheFile = func(name string, path string) (itm CacheItem, err error) {
			return DefaultCacheFile(&realClock{}, name, path)
		}
	}

	if cache.shutdown != nil {
		close(cache.shutdown)
	}

	cache.items = make(map[string]CacheItem)
	cache.shutdown = make(chan interface{}, 1)
	os.MkdirAll(cache.Directory, 0700)
	cache.capacity = 0
	go cache.loadExisting()
	go cache.gcWorker()
	return nil
}

func (cache *FileCacheImpl) Open(name string) (*os.File, error) {
	item := cache.GetCache(name)
	if item != nil {
		return os.Open(item.Access())
	}
	return nil, ItemNotInCache
}

func (cache *FileCacheImpl) Create(name string) (WritableFile, error) {
	path := filepath.Join(cache.Directory, name)
	dirPath := filepath.Dir(path)
	os.MkdirAll(dirPath, 0700)

	f, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}
	return &writableFile{
		f: f,
		afterClose: func(good bool) {
			if !good {
				os.Remove(f.Name())
				return
			}
			cache.Remove(name)

			err2 := os.Rename(f.Name(), path)
			if err2 == nil {
				err2 = cache.addItemWithLock(name)
			}
			if err2 != nil {
				os.Remove(f.Name())
			}
		},
	}, nil
}

func (cache *FileCacheImpl) Remove(name string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	old := cache.swapItem(name, nil)
	if old != nil {
		old.RemoveFile()
	}
}

func (cache *FileCacheImpl) ExpireOldest(purgeCount int, purgeSize int64) bool {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if cache.isCacheNull() {
		return false
	}
	return cache.expireOldest(purgeCount, purgeSize)
}

// IsActive returns true if the cache has been started, and false otherwise.
func (cache *FileCacheImpl) IsActive() bool {
	return !cache.isCacheNull()
}

// Count returns the number of entries in the cache.
func (cache *FileCacheImpl) Count() int {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return len(cache.items)
}

// Capacity returns the disk capacity in the cache.
func (cache *FileCacheImpl) Capacity() int64 {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cache.capacity
}

// GetCache returns true if the item is in the cache.
func (cache *FileCacheImpl) GetCache(name string) CacheItem {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cache.getItem(name)
}

// Stop turns off the file cache.
// This closes the concurrent caching mechanism, destroys the cache, and
// the background scanner that it should stop.
// If there are any items or cache operations ongoing while Stop() is called,
// it is undefined how they will behave.
func (cache *FileCacheImpl) Stop() {
	if cache.shutdown != nil {
		close(cache.shutdown)
		<-time.After(1 * time.Microsecond) // give goroutines time to shutdown
	}

	if cache.items != nil {
		cache.mutex.Lock()
		cache.items = nil
		cache.mutex.Unlock()
	}

	cache.wait.Wait()
}

func (cache *FileCacheImpl) loadExisting() {
	logging.Infof("[go-ds-s3] load existing start")

	bulkOp := func(items []CacheItem) {
		cache.mutex.Lock()
		defer cache.mutex.Unlock()

		for _, item := range items {
			cache.capacity += item.Size()
			cache.items[item.Name()] = item
		}
	}

	bufferedItems := make([]CacheItem, 0, 100)
	filepath.WalkDir(cache.Directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}

		name, err := filepath.Rel(cache.Directory, path)
		if err != nil {
			return nil
		}

		item, err := cache.CacheFile(name, path)
		if err != nil {
			logging.Infof("[go-ds-s3] cache file failed: %+v", err)
			return nil
		}
		bufferedItems = append(bufferedItems, item)
		if len(bufferedItems) >= 100 {
			bulkOp(bufferedItems)
			bufferedItems = make([]CacheItem, 0, 100)
		}

		return nil
	})
	if len(bufferedItems) > 0 {
		bulkOp(bufferedItems)
	}

	logging.Infof("[go-ds-s3] load existing done")
}

func (cache *FileCacheImpl) isCacheNull() bool {
	return cache.items == nil
}

func (cache *FileCacheImpl) swapItem(name string, item CacheItem) CacheItem {
	if cache.items == nil {
		return nil
	}
	oldItem, oldHas := cache.items[name]
	if oldHas {
		cache.capacity -= oldItem.Size()
	}
	if item == nil {
		if oldHas {
			delete(cache.items, name)
			oldItem.RemoveFile()
		}
	} else {
		cache.items[name] = item
		cache.capacity += item.Size()
	}
	return oldItem
}

func (cache *FileCacheImpl) getItem(name string) CacheItem {
	if cache.isCacheNull() {
		return nil
	}
	item, ok := cache.items[name]
	if ok {
		return item
	}
	return nil
}

// addItem is an internal function for adding an item to the cache.
func (cache *FileCacheImpl) addItemWithLock(name string) (err error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cache.addItem(name)
}

// addItem is an internal function for adding an item to the cache.
func (cache *FileCacheImpl) addItem(name string) (err error) {
	path := filepath.Join(cache.Directory, name)

	if cache.isCacheNull() {
		return
	}

	item := cache.getItem(name)
	if item != nil && !cache.checkExpired(item) {
		return nil
	} else if item != nil {
		cache.swapItem(name, nil)
	}

	item, err = cache.CacheFile(name, path)
	if item == nil || err != nil {
		return err
	}

	var purgeCount int
	var purgeSize int64

	if len(cache.items) >= cache.MaxItems {
		purgeCount = len(cache.items) - cache.MaxItems + 1
	}

	if item.Size() > cache.MaxSize {
		return ItemTooLarge
	}

	remainingCapacity := cache.MaxSize - cache.capacity
	if remainingCapacity < item.Size() {
		purgeSize = cache.MaxSize - item.Size()
	}

	if (purgeCount > 0 || purgeSize > 0) && !cache.expireOldest(purgeCount, purgeSize) {
		return ItemTooLarge
	}

	cache.swapItem(name, item)
	return nil
}

func (cache *FileCacheImpl) deleteItem(name string) {
	cache.swapItem(name, nil)
}

// expireOldest is used to expire the oldest item in the cache.
// The force argument is used to indicate it should remove at least one
// entry; for example, if a large number of files are cached at once, none
// may appear older than another.
func (cache *FileCacheImpl) expireOldest(purgeCount int, purgeSize int64) bool {
	var sortedItems []CacheItem
	for _, item := range cache.items {
		sortedItems = append(sortedItems, item)
	}
	slices.SortFunc(sortedItems, cache.CacheComparer)

	if purgeCount < 0 {
		purgeCount = 0
	}
	if purgeSize < 0 {
		purgeSize = 0
	}

	pos := 0

	for ; pos < purgeCount && pos < len(sortedItems); pos++ {
		cache.deleteItem(sortedItems[pos].Name())
		pos++
	}

	for pos < len(sortedItems) {
		remainingCapacity := cache.MaxSize - cache.capacity
		if remainingCapacity >= purgeSize {
			break
		}
		cache.deleteItem(sortedItems[pos].Name())
		pos++
	}

	deletedCount := pos

	for pos < len(sortedItems) {
		if cache.checkExpired(sortedItems[pos]) {
			cache.deleteItem(sortedItems[pos].Name())
			deletedCount++
		}
		pos++
	}

	return deletedCount > 0
}

// gcWorker is a background goroutine responsible for cleaning the cache.
// It runs periodically, every cache.GCPeriod seconds. If cache.GCPeriod is set
// to 0, it will not run.
func (cache *FileCacheImpl) gcWorker() {
	if cache.GCPeriod <= 0 {
		return
	}

	cache.wait.Add(1)
	defer cache.wait.Done()

	for {
		select {
		case _ = <-cache.shutdown:
			return
		case <-cache.clock.After(cache.GCPeriod):
			if cache.isCacheNull() {
				return
			}
			cache.ExpireOldest(len(cache.items)-cache.MaxItems, cache.capacity-cache.MaxSize)
		}
	}
}

// checkExpired returns true if an item is expired.
func (cache *FileCacheImpl) checkExpired(item CacheItem) bool {
	if cache.CheckExpired != nil && cache.CheckExpired(cache, item) {
		return true
	}
	return false
}
