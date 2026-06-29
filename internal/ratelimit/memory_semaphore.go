package ratelimit

import (
	"context"
	"sync"
)

type MemorySemaphore struct {
	locks sync.Map
	max   int
}

func NewMemorySemaphore(maxConcurrency int) *MemorySemaphore {
	return &MemorySemaphore{
		max: maxConcurrency,
	}
}

func (m *MemorySemaphore) Acquire(ctx context.Context, domain string) (func(), error) {
	if domain == "" {
		domain = "global"
	}

	actual, _ := m.locks.LoadOrStore(domain, make(chan struct{}, m.max))
	sem := actual.(chan struct{})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case sem <- struct{}{}:
		return func() {
			<-sem
		}, nil
	}
}
