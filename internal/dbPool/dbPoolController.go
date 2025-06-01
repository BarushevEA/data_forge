package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/BarushevEA/in_memory_cache/pkg"
	"github.com/BarushevEA/in_memory_cache/types"
	"time"
)

type PoolController struct {
	db dbTypes.ITableDB

	// Interval between writing accumulated data from pool to database
	writePoolInterval time.Duration

	// Maximum size of the writing pool
	maxPoolSize int

	writePool  types.ICacheInMemory[[]string]
	deletePool types.ICacheInMemory[[]string]
	tables     types.ICacheInMemory[dbTypes.ITableRegister]
}

func NewPoolController(db dbTypes.ITableDB, writePoolInterval time.Duration, maxPoolSize int) dbTypes.ITableDB {
	stmtsOptions := dbTypes.GetLongDefaultShardedCacheOptions()

	return &PoolController{
		db:                db,
		writePoolInterval: writePoolInterval,
		maxPoolSize:       maxPoolSize,

		writePool: pkg.NewShardedCache[[]string](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		deletePool: pkg.NewShardedCache[[]string](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		tables: pkg.NewShardedCache[dbTypes.ITableRegister](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
	}
}

func (controller *PoolController) RegisterTable(tableName string, table dbTypes.ITableRegister) error {
	return controller.tables.Set(tableName, table)
}

func (controller *PoolController) CreateTable(ctx context.Context, name string) error {
	return controller.db.CreateTable(ctx, name)
}

func (controller *PoolController) DropTable(ctx context.Context, name string) error {
	controller.writePool.Delete(name)
	controller.deletePool.Delete(name)
	controller.tables.Delete(name)
	return controller.db.DropTable(ctx, name)
}

func (controller *PoolController) Set(ctx context.Context, tableName, key string, value []byte) error {
	poolKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		poolKeys = make([]string, 0, controller.maxPoolSize)
	}

	poolKeys = append(poolKeys, key)
	err := controller.writePool.Set(tableName, poolKeys)
	if err != nil {
		return err
	}

	if len(poolKeys) >= controller.maxPoolSize {
		return controller.writePoolFlush(tableName)
	}

	return nil
}

func (controller *PoolController) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	return controller.db.Get(ctx, tableName, key)
}

func (controller *PoolController) Delete(ctx context.Context, tableName, key string) error {
	poolKeys, ok := controller.deletePool.Get(tableName)
	if !ok {
		poolKeys = make([]string, 0, controller.maxPoolSize)
	}

	poolKeys = append(poolKeys, key)
	err := controller.deletePool.Set(tableName, poolKeys)
	if err != nil {
		return err
	}

	if len(poolKeys) >= controller.maxPoolSize {
		return controller.deletePoolFlush(tableName)
	}

	return nil
}

func (controller *PoolController) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	return controller.db.BatchSet(ctx, tableName, items)
}

func (controller *PoolController) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	return controller.db.BatchGet(ctx, tableName, keys)
}

func (controller *PoolController) Close() error {
	controller.writePool.Clear()
	controller.deletePool.Clear()
	controller.tables.Clear()
	return controller.db.Close()
}

func (controller *PoolController) writePoolFlush(tableName string) error {
	writeKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		return nil
	}

	table, ok := controller.tables.Get(tableName)
	if !ok {
		return nil
	}

	batch := make(map[string][]byte)
	for _, key := range writeKeys {
		value, err := table.Serialize(key)
		if err != nil {
			return err
		}
		batch[key] = value
	}

	err := controller.db.BatchSet(context.Background(), tableName, batch)
	if err != nil {
		return err
	}

	controller.writePool.Delete(tableName)
	return nil
}

func (controller *PoolController) deletePoolFlush(tableName string) error {
	deleteKeys, ok := controller.deletePool.Get(tableName)
	if !ok {
		return nil
	}

	writeKeys, ok := controller.writePool.Get(tableName)

	newWriteKeys := make([]string, 0, len(writeKeys))

	for _, deleteKey := range deleteKeys {
		for _, writeKey := range writeKeys {
			if deleteKey == writeKey {
				continue
			}
			newWriteKeys = append(newWriteKeys, writeKey)
		}

		err := controller.db.Delete(context.Background(), tableName, deleteKey)
		if err != nil {
			return err
		}
	}

	err := controller.writePool.Set(tableName, newWriteKeys)
	if err != nil {
		return err
	}

	controller.deletePool.Delete(tableName)
	return nil
}
