package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimiter interface {
	Wait(ctx context.Context, domain string) error
}

type RedisRateLimiter struct {
	rdb *redis.Client
}

func NewRedisRateLimiter(rdb *redis.Client) *RedisRateLimiter {
	return &RedisRateLimiter{rdb: rdb}
}

// tokenBucketScript uses Redis TIME to avoid clock drift between clients
const tokenBucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local redis_time = redis.call('TIME')
local now = tonumber(redis_time[1]) + (tonumber(redis_time[2]) / 1000000)

local last_tokens = tonumber(redis.call("hget", key, "tokens"))
if last_tokens == nil then
  last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("hget", key, "timestamp"))
if last_refreshed == nil then
  last_refreshed = now
end

local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
  new_tokens = filled_tokens - requested
end

redis.call("hset", key, "tokens", new_tokens)
redis.call("hset", key, "timestamp", now)
redis.call("expire", key, math.ceil(capacity/rate)*2 + 5)

if allowed then
	return 1
else
	return 0
end
`

func (l *RedisRateLimiter) Wait(ctx context.Context, domain string) error {
	rate := 3.0
	capacity := 5.0

	key := fmt.Sprintf("rate_limit:domain:%s", domain)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		
		res, err := l.rdb.Eval(ctx, tokenBucketScript, []string{key}, rate, capacity, 1).Result()
		if err != nil {
			return fmt.Errorf("redis eval error: %w", err)
		}

		if allowed, ok := res.(int64); ok && allowed == 1 {
			return nil
		}
		
		// Espera e tenta novamente se não houver tokens (backoff curto)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
			// retry loop
		}
	}
}
