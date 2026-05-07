package filecache

import (
	"time"
)

func DefaultCheckExpireLRU(expireTime time.Duration) CheckExpired {
	return func(cache FileCache, item CacheItem) bool {
		dur := item.Duration()
		return dur >= expireTime
	}
}
