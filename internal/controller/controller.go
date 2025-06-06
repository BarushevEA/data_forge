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

	table.mutex.Lock()
	defer table.mutex.Unlock()

	_, exists = table.lostKeys.Get(key)
	if exists {
		return *new(T), false
	}

	err := table.lostKeys.Set(key, struct{}{})
	if err != nil {
		return *new(T), false
	}

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

	serialized, err := table.Serialize(key)
	if err != nil {
		return err
	}

	return table.db.Set(table.Context, table.TableName, key, serialized)
}

func (table *TableController[T]) Delete(key string) error {
	err := table.db.Delete(table.Context, table.TableName, key)
	if err != nil {
		return err
	}

	table.cache.Delete(key)
	table.lostKeys.Delete(key)
	return nil
}

func (table *TableController[T]) GetTop() (map[string]T, error) {
	//TODO implement me
	panic("implement me")
}

func (table *TableController[T]) BatchGet(keys []string) ([]lib.BatchNode[T], error) {
	nodes := make([]lib.BatchNode[T], len(keys))
	keysForDb := make([]string, 0)

	for i, key := range keys {
		data, exists := table.cache.Get(key)
		nodes[i] = lib.BatchNode[T]{
			Key:    key,
			Value:  data,
			Exists: exists,
		}

		if !exists {
			_, exists = table.lostKeys.Get(key)
			if !exists {
				keysForDb = append(keysForDb, key)
			}
		}
	}

	if len(keysForDb) == 0 {
		return nodes, nil
	}

	table.mutex.Lock()
	defer table.mutex.Unlock()

	nodesFromDB, err := table.db.BatchGet(table.Context, table.TableName, keysForDb)
	if err != nil {
		return nil, err
	}

	for index, _ := range nodes {
		dbValue, existsInDB := nodesFromDB[nodes[index].Key]
		dataExists := false
		if !existsInDB {
			err := table.lostKeys.Set(nodes[index].Key, struct{}{})
			if err != nil {
				return nil, err
			}
			continue
		}

		if dbValue != nil && len(dbValue) != 0 {
			dataExists = true
		}

		data, err := table.Deserialize(dbValue)
		if err != nil {
			return nil, err
		}
		nodes[index].Value = data
		nodes[index].Exists = dataExists

		if dataExists {
			err = table.cache.Set(nodes[index].Key, data)
			if err != nil {
				return nil, err
			}
			table.lostKeys.Delete(nodes[index].Key)
		} else {
			err := table.lostKeys.Set(nodes[index].Key, struct{}{})
			if err != nil {
				return nil, err
			}
		}
	}

	return nodes, nil
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
	for key, value := range items {
		err := table.Set(key, value)
		if err != nil {
			return err
		}
	}
	return nil
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
	for _, key := range keys {
		err := table.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (table *TableController[T]) Destroy() error {
	err := table.db.DropTable(table.Context, table.TableName)
	if err != nil {
		return err
	}
	return table.Clear()
}

func (table *TableController[T]) Clear() error {
	table.cache.Clear()
	table.lostKeys.Clear()
	return table.db.Close()
}

func (table *TableController[T]) Len() int {
	return table.cache.Len()
}

func (table *TableController[T]) Serialize(key string) ([]byte, error) {
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
