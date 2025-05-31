package dbTypes

import (
	"context"
	"sync"
	"time"
)

var defaultShardedCacheOptions *ShardedCacheOptions
var initOnceShardedCacheOptions sync.Once

type ShardedCacheOptions struct {
	Ctx          context.Context
	Ttl          time.Duration
	TtlDecrement time.Duration
}

func NewShardedCacheOptions(ctx context.Context, ttl time.Duration, ttlDecrement time.Duration) *ShardedCacheOptions {
	return &ShardedCacheOptions{
		Ctx:          ctx,
		Ttl:          ttl,
		TtlDecrement: ttlDecrement,
	}
}

func GetLongDefaultShardedCacheOptions() *ShardedCacheOptions {
	initOnceShardedCacheOptions.Do(func() {
		if defaultShardedCacheOptions == nil {
			defaultShardedCacheOptions = NewShardedCacheOptions(context.Background(), 1000000*time.Hour, 10000*time.Hour)
		}
	})

	return defaultShardedCacheOptions
}
