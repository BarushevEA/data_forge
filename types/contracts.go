package types

import "github.com/BarushevEA/in_memory_cache/types"

type ITable[T any] interface {
	Get(key string) (T, bool)
	GetTop() (map[string]T, error)

	Range(callback func(key string, value T) bool) error
	RangeCacheMetrics(callback func(metric types.Metric[T]) bool) error

	Set(key string, value T) error

	Delete(key string) error

	Destroy() error
	Clear() error
	Len() int
}
