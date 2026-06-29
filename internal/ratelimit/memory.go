package ratelimit

import (
	"context"
	"net/url"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type MemoryRateLimiter struct {
	limiters sync.Map
}

func NewMemoryRateLimiter() *MemoryRateLimiter {
	return &MemoryRateLimiter{}
}

func (m *MemoryRateLimiter) Wait(ctx context.Context, domain string) error {
	u, err := url.Parse(domain)
	dom := "global"
	if err == nil && u.Host != "" {
		dom = u.Host
	} else if domain != "" {
		dom = domain
	}

	var limiter *rate.Limiter
	if l, ok := m.limiters.Load(dom); ok {
		limiter = l.(*rate.Limiter)
	} else {
		// Default: 3 requests per second, burst of 5
		newLimiter := rate.NewLimiter(rate.Every(333*time.Millisecond), 5)
		actual, _ := m.limiters.LoadOrStore(dom, newLimiter)
		limiter = actual.(*rate.Limiter)
	}

	return limiter.Wait(ctx)
}
