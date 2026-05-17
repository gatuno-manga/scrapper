package scraper

import (
	"context"
	"testing"
	"time"
)

func TestBrowserPool_Concurrency(t *testing.T) {
	pool, err := NewBrowserPool("", 2) 
	if err != nil {
		t.Skipf("Skipping BrowserPool test: could not init pool: %v", err)
		return
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Acquire first
	ctx1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire 1: %v", err)
	}

	// 2. Acquire second
	ctx2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire 2: %v", err)
	}

	// 3. Third acquire should timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer timeoutCancel()
	_, err = pool.Acquire(timeoutCtx)
	if err == nil {
		t.Error("Expected timeout for third acquire, but got context")
	}

	// 4. Release one and try again
	pool.Release(ctx1)
	ctx3, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire after release: %v", err)
	}
	if ctx3 == nil {
		t.Error("Got nil context after release")
	}

	pool.Release(ctx2)
	pool.Release(ctx3)
}
