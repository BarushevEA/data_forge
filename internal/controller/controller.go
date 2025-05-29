package controller

import (
	"context"
	"github.com/BarushevEA/data_forge/types"
	lib "github.com/BarushevEA/in_memory_cache/types"

	"time"
)

type TableController[T any] struct {
	TableName    string
	TableType    T
	Context      context.Context
	TTL          time.Duration
	TTLDecrement time.Duration
	cache        *lib.ICacheInMemory[T]
	//db           *sql.DB
}

func NewTableController[T any](option types.TableOption[T]) (types.ITable[T], error) {
	return &TableController[T]{
		TableName:    option.TableName,
		TableType:    option.TableType,
		Context:      option.Context,
		TTL:          option.TTL,
		TTLDecrement: option.TTLDecrement,
	}, nil
}

func (table *TableController[T]) Get(key string) (T, bool) {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) GetTop() (map[string]T, error) {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) BatchGet(keys []string) ([]lib.BatchNode[T], error) {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Range(callback func(key string, value T) bool) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) RangeCacheMetrics(callback func(metric lib.Metric[T]) bool) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Set(key string, value T) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) BatchSet(items map[string]T) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) SetTopLimit(limit uint) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) SetTopAccessThreshold(threshold uint) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) SetTopWriteThreshold(threshold uint) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Delete(key string) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) BatchDelete(keys []string) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Destroy() error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Clear() error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Len() int {
	//TODO implement me
	panic("implement me")
}
