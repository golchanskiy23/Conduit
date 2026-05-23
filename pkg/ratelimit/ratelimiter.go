package ratelimit

import (
	"sync"
	"time"
)

type SlidingWindow struct {
	mu        sync.Mutex
	limit     int
	window    time.Duration
	prevCount int
	currCount int
	currStart time.Time
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:     limit,
		window:    window,
		currStart: time.Now(),
	}
}

func (sw *SlidingWindow) Allow() bool {
	return sw.AllowAt(time.Now())
}

func (sw *SlidingWindow) AllowAt(now time.Time) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.advanceWindow(now)

	elapsed := now.Sub(sw.currStart)
	weight := 1.0 - float64(elapsed)/float64(sw.window)
	approx := int(float64(sw.prevCount)*weight) + sw.currCount

	if approx >= sw.limit {
		return false
	}

	sw.currCount++
	return true
}

func (sw *SlidingWindow) advanceWindow(now time.Time) {
	elapsed := now.Sub(sw.currStart)

	switch {
	case elapsed >= 2*sw.window:
		sw.prevCount = 0
		sw.currCount = 0
		sw.currStart = now

	case elapsed >= sw.window:
		sw.prevCount = sw.currCount
		sw.currCount = 0
		sw.currStart = sw.currStart.Add(sw.window)
	}
}