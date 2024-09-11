package filecache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func getTimeExpiredCacheItem() *cacheItem {
	TwoHours, err := time.ParseDuration("-2h")
	if err != nil {
		panic(err.Error())
	}
	item := &cacheItem{
		name:       "expired",
		clock:      &clock,
		lastaccess: clock.Now().Add(TwoHours),
	}
	return item
}

func writeToCache(t *testing.T, cache FileCache, content string) string {
	h := sha256.New()
	h.Write([]byte(content))
	digest := h.Sum(nil)
	name := hex.EncodeToString(digest)

	f, err := cache.Create(name)
	if err != nil {
		fmt.Println("failed")
		fmt.Println("[!] couldn't create temporary file: ", err.Error())
		t.Fail()
		return ""
	}
	_, err = f.Write([]byte(content))
	if err != nil {
		f.Close()
		fmt.Println("failed")
		fmt.Println("[!] couldn't create temporary file: ", err.Error())
		t.Fail()
		return ""
	}
	err = f.Close()
	if err != nil {
		fmt.Println("failed")
		fmt.Println("[!] couldn't create temporary file: ", err.Error())
		t.Fail()
		return ""
	}

	return name
}

var clock fakeClock

func newTestCache(t *testing.T, checkExpire CheckExpired) *FileCacheImpl {
	tmpDir, err := os.MkdirTemp("", "tmp-*")
	if err != nil {
		t.Error(err)
		return nil
	}
	log.Println("tmpDir : ", tmpDir)
	cache := &FileCacheImpl{
		clock:     &clock,
		items:     nil,
		Directory: tmpDir,
		MaxItems:  DefaultMaxItems,
		MaxSize:   DefaultMaxSize,
		CacheFile: func(name string, path string) (CacheItem, error) {
			return DefaultCacheFile(&clock, name, path)
		},
		CheckExpired: checkExpire,
		GCPeriod:     DefaultEvery,
	}
	t.Cleanup(func() {
		cache.Stop()
		os.RemoveAll(tmpDir)
	})
	return cache
}

func TestCacheStartStop(t *testing.T) {
	fmt.Printf("[+] testing cache start up and shutdown: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Minute*5))
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}
	clock.Sleep(1 * time.Second)

	fmt.Println("ok")
}

func TestTimeExpiration(t *testing.T) {
	fmt.Printf("[+] ensure item expires after ExpireItem: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Minute*5))
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}
	itm := getTimeExpiredCacheItem()
	if !cache.checkExpired(itm) {
		fmt.Println("failed")
		fmt.Println("[!] item should have expired!")
		t.Fail()
	} else {
		fmt.Println("ok")
	}
}

func TestCache(t *testing.T) {
	fmt.Printf("[+] testing asynchronous file caching: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Minute*5))
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}
	name := writeToCache(t, cache, "lorem ipsum akldfjsdlf")
	if name == "" {
		t.FailNow()
	}

	var (
		delay int
		ok    bool
		step  = 10
		stop  = 500
		dur   = time.Duration(step) * time.Microsecond
	)

	for ok = cache.GetCache(name) != nil; !ok; ok = cache.GetCache(name) != nil {
		clock.Sleep(dur)
		delay += step
		if delay >= stop {
			break
		}
	}

	if !ok {
		fmt.Println("failed")
		fmt.Printf("\t[*] cache check stopped after %dµs\n", delay)
		t.Fail()
	} else {
		fmt.Println("ok")
		fmt.Printf("\t[*] item cached in %dµs\n", delay)
	}
	os.Remove(name)
}

func TestExpireAll(t *testing.T) {
	fmt.Printf("[+] testing background expiration: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Second*2))
	cache.GCPeriod = time.Second
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}

	name := writeToCache(t, cache, "this is a first file and some stuff should go here")
	if name == "" {
		t.Fail()
		return
	}
	clock.Sleep(500 * time.Millisecond)

	name2 := writeToCache(t, cache, "this is the second file")
	if name2 == "" {
		t.Fail()
		return
	}
	clock.Sleep(500 * time.Millisecond)

	clock.Sleep(2000 * time.Millisecond)
	cacheSize := cache.Count()
	if cacheSize > 0 {
		fmt.Println("failed")
		fmt.Printf("[!] %d items still in cache", cacheSize)
		t.Fail()
	}

	if !t.Failed() {
		fmt.Println("ok")
	}
}

func TestExpireOldestOverItem(t *testing.T) {
	fmt.Printf("[+] validating item limit on cache: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Minute*5))
	cache.MaxItems = 5
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}

	names := make([]string, 0)
	for i := 0; i < 1000; i++ {
		name := writeToCache(t, cache, fmt.Sprintf("file number %d\n", i))
		if t.Failed() {
			break
		}
		names = append(names, name)
	}

	if !t.Failed() && cache.Count() > cache.MaxItems {
		fmt.Println("failed")
		fmt.Printf("[!] %d items in cache (limit should be %d)", cache.Count(), cache.MaxItems)
		t.Fail()
	}

	realCount := 0
	filepath.WalkDir(cache.Directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		realCount++
		return nil
	})
	if !t.Failed() && realCount > cache.MaxItems {
		fmt.Println("failed")
		fmt.Printf("[!] %d items in filesystem (limit should be %d)", realCount, cache.MaxItems)
		t.Fail()
	}

	if !t.Failed() {
		fmt.Println("ok")
	}
}

func TestExpireOldestOverSize(t *testing.T) {
	fmt.Printf("[+] validating size limit on cache: ")
	cache := newTestCache(t, DefaultCheckExpireLRU(time.Minute*5))
	cache.MaxItems = 1000
	cache.MaxSize = 4 * Kilobyte
	if err := cache.Start(); err != nil {
		fmt.Println("failed")
		fmt.Println("[!] cache failed to start: ", err.Error())
	}

	names := make([]string, 0)
	for i := 0; i < 10; i++ {
		content := fmt.Sprintf("file number %d\n", i)
		remaining := 1024 - len(content)
		content += strings.Repeat("a", remaining)

		name := writeToCache(t, cache, content)
		if t.Failed() {
			break
		}
		names = append(names, name)
	}

	if !t.Failed() && cache.Count() > 4 {
		fmt.Println("failed")
		fmt.Printf("[!] %d items in cache (limit should be %d)", cache.Count(), cache.MaxItems)
		t.Fail()
	}

	realCount := 0
	filepath.WalkDir(cache.Directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		realCount++
		return nil
	})
	if !t.Failed() && realCount > 4 {
		fmt.Println("failed")
		fmt.Printf("[!] %d items in filesystem (limit should be %d)", realCount, cache.MaxItems)
		t.Fail()
	}

	if !t.Failed() && cache.Capacity() > cache.MaxSize {
		fmt.Println("failed")
		fmt.Printf("[!] %d bytes in filesystem (limit should be %d)", cache.Capacity(), cache.MaxSize)
		t.Fail()
	}

	if !t.Failed() {
		fmt.Println("ok")
	}
}
