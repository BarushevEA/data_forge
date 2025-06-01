package controller

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
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
	cache        lib.ICacheInMemory[T]
	db           dbTypes.ITableDB

	destroyCallback func(tableName string)
}

func NewTableController[T any](option types.TableOption[T], db dbTypes.ITableDB) (types.ITable[T], error) {
	controller := &TableController[T]{
		db:           db,
		TableName:    option.TableName,
		TableType:    option.TableType,
		Context:      option.Context,
		TTL:          option.TTL,
		TTLDecrement: option.TTLDecrement,
	}

	err := db.RegisterTable(option.TableName, controller)
	if err != nil {
		return nil, err
	}
	return controller, nil
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
	defer func() {
		if table.destroyCallback != nil {
			table.destroyCallback(table.TableName)
		}
	}()
	return nil
}

func (table *TableController[T]) SetDestroyCallback(callback func(tableName string)) {
	table.destroyCallback = callback
}

func (table *TableController[T]) Clear() error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Len() int {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Serialize(key string) ([]byte, error) {
	return nil, nil
}
