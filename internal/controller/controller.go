package controller

import (
	"bytes"
	"context"
	"fmt"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/BarushevEA/data_forge/types"
	"github.com/BarushevEA/in_memory_cache/pkg"
	lib "github.com/BarushevEA/in_memory_cache/types"
	"github.com/vmihailenco/msgpack/v5"
	"sync"

	"time"
)

type TableController[T any] struct {
	TableName    string
	TableType    T
	Context      context.Context
	TTL          time.Duration
	TTLDecrement time.Duration
	cache        lib.ICacheInMemory[T]
	lostKeys     lib.ICacheInMemory[struct{}]
	db           dbTypes.ITableDB

	mutex sync.Mutex
}

func NewTableController[T any](option types.TableOption[T], db dbTypes.ITableDB) (types.ITable[T], error) {
	controller := &TableController[T]{
		db:           db,
		TableName:    option.TableName,
		TableType:    option.TableType,
		Context:      option.Context,
		TTL:          option.TTL,
		TTLDecrement: option.TTLDecrement,
		cache:        pkg.NewShardedCache[T](option.Context, option.TTL, option.TTLDecrement),
		lostKeys:     pkg.NewShardedCache[struct{}](option.Context, option.TTL, option.TTLDecrement),
	}

	err := db.CreateTable(option.Context, option.TableName)
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func (table *TableController[T]) Get(key string) (T, bool) {
	data, exists := table.cache.Get(key)
	if exists {
		return data, exists
	}

	_, exists = table.lostKeys.Get(key)
	if exists {
		return *new(T), false
	}
	err := table.lostKeys.Set(key, struct{}{})

	table.mutex.Lock()
	defer table.mutex.Unlock()

	dbData, exists, err := table.db.Get(table.Context, table.TableName, key)
	if err != nil {
		return *new(T), false
	}

	if !exists {
		return *new(T), false
	}

	data, err = table.Deserialize(dbData)
	if err != nil {
		return *new(T), false
	}

	err = table.cache.Set(key, data)
	if err != nil {
		return *new(T), false
	}

	table.lostKeys.Delete(key)

	return data, true
}

func (table *TableController[T]) Set(key string, value T) error {
	err := table.cache.Set(key, value)
	if err != nil {
		return err
	}

	//err = table.writePool.Set(key, value)
	//if err != nil {
	//	return err
	//}

	return table.db.Set(table.Context, table.TableName, key, nil)
}

func (table *TableController[T]) Delete(key string) error {
	err := table.db.Delete(table.Context, table.TableName, key)
	if err != nil {
		return err
	}

	table.cache.Delete(key)
	table.lostKeys.Delete(key)
	//table.writePool.Delete(key)
	return nil
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

func (table *TableController[T]) BatchDelete(keys []string) error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Destroy() error {
	return nil
}

func (table *TableController[T]) Clear() error {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) Len() int {
	return table.cache.Len()
}

func (table *TableController[T]) Serialize(key string) ([]byte, error) {
	table.mutex.Lock()
	table.mutex.Unlock()

	data, exists := table.cache.Get(key)
	if !exists {
		return nil, fmt.Errorf("key %s not found", key)
	}

	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).Encode(data)
	if err != nil {
		return nil, err
	}

	serialized := buf.Bytes()

	return serialized, nil
}

func (table *TableController[T]) Deserialize(serialized []byte) (T, error) {
	var decoded T

	err := msgpack.NewDecoder(bytes.NewReader(serialized)).Decode(&decoded)
	if err != nil {
		return *new(T), err
	}

	return decoded, nil
}
