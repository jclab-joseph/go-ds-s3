package filecache

import (
	"sync"
	"time"
)

type fakeClock struct {
	mutex sync.RWMutex
	delay time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mutex.RLock()
	delay := c.delay
	c.mutex.RUnlock()
	return time.Now().Add(delay)
}

func (c *fakeClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		st := c.Now()
		for {
			if c.Since(st) >= d {
				break
			}
			time.Sleep(time.Millisecond)
		}
		ch <- c.Now()
	}()
	return ch
}

func (c *fakeClock) Sleep(t time.Duration) {
	minimalDelay := time.Millisecond * 100
	if t < minimalDelay {
		minimalDelay = t
	}
	c.mutex.Lock()
	c.delay += t - minimalDelay
	c.mutex.Unlock()
	time.Sleep(minimalDelay) // allow to execute other go-routine
}
