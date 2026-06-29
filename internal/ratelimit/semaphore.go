package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Semaphore interface {
	Acquire(ctx context.Context, domain string) (func(), error)
}

type RedisSemaphore struct {
	rdb            *redis.Client
	maxConcurrency int
	timeout        time.Duration
}

func NewRedisSemaphore(rdb *redis.Client, maxConcurrency int) *RedisSemaphore {
	return &RedisSemaphore{
		rdb:            rdb,
		maxConcurrency: maxConcurrency,
		timeout:        10 * time.Minute, // Maximum time a job can hold the lock globally
	}
}

const acquireScript = `
local key = KEYS[1]
local max = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
local uuid = ARGV[4]

-- Remove expired sessions (cleanup dead pods)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Check current active browsers for this domain
local count = redis.call('ZCARD', key)
if count < max then
	redis.call('ZADD', key, now + timeout, uuid)
	redis.call('EXPIRE', key, timeout + 5)
	return 1
else
	return 0
end
`

const releaseScript = `
local key = KEYS[1]
local uuid = ARGV[1]
redis.call('ZREM', key, uuid)
return 1
`

func (s *RedisSemaphore) Acquire(ctx context.Context, domain string) (func(), error) {
	key := fmt.Sprintf("semaphore:browser:%s", domain)
	id := uuid.New().String()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		now := time.Now().Unix()
		timeoutSecs := int64(s.timeout.Seconds())

		res, err := s.rdb.Eval(ctx, acquireScript, []string{key}, s.maxConcurrency, now, timeoutSecs, id).Result()
		if err != nil {
			return nil, fmt.Errorf("redis eval error (semaphore): %w", err)
		}

		if allowed, ok := res.(int64); ok && allowed == 1 {
			// Acquired successfully
			return func() {
				// Use Background context to guarantee release even if parent context timed out
				s.rdb.Eval(context.Background(), releaseScript, []string{key}, id)
			}, nil
		}

		// Not acquired, wait a bit and retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
			// retry
		}
	}
}
