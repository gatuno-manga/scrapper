package scraper

import (
	"context"
	"testing"
	"time"

	"github.com/mxschmitt/playwright-go"
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

// TestBrowserPool_NewContextWithOpts_ReleasesSlot guards against BUG-01: a
// browser-pool slot acquired through NewContextWithOpts (the path taken for
// requests with a custom proxy/User-Agent) must be returned by Release, the
// same as slots acquired through Acquire. Before the fix, ScrapeChapter's
// cleanup closure called bCtx.Close() directly for these contexts instead of
// going through Release, so the semaphore slot was never drained and the
// pool exhausted permanently after poolSize such requests.
func TestBrowserPool_NewContextWithOpts_ReleasesSlot(t *testing.T) {
	const poolSize = 2
	pool, err := NewBrowserPool("", poolSize)
	if err != nil {
		t.Skipf("Skipping BrowserPool test: could not init pool: %v", err)
		return
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	customOpts := playwright.BrowserNewContextOptions{
		UserAgent: playwright.String("test-agent"),
	}

	acquired := make([]playwright.BrowserContext, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		bCtx, err := pool.NewContextWithOpts(ctx, customOpts)
		if err != nil {
			t.Fatalf("NewContextWithOpts %d failed: %v", i, err)
		}
		acquired = append(acquired, bCtx)
	}

	// Pool is now at capacity; a further call must not block indefinitely.
	fullCtx, fullCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer fullCancel()
	if _, err := pool.NewContextWithOpts(fullCtx, customOpts); err == nil {
		t.Fatal("expected error when pool is at capacity")
	}

	// Release every slot the way ScrapeChapter's fixed cleanup closure does
	// (unconditionally through pool.Release, regardless of custom context).
	for _, bCtx := range acquired {
		pool.Release(bCtx)
	}

	// If Release drained the semaphore correctly, Acquire must succeed
	// immediately instead of blocking until context cancellation.
	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer acquireCancel()
	bCtx, err := pool.Acquire(acquireCtx)
	if err != nil {
		t.Fatalf("Acquire after releasing custom contexts should not block/fail: %v", err)
	}
	pool.Release(bCtx)
}
