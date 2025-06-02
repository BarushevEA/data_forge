package dbPool

import (
	"context"
	"fmt"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/BarushevEA/in_memory_cache/pkg"
	"github.com/BarushevEA/in_memory_cache/types"
	"log"
	"sync"
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

	isDeletePoolFlushing bool
	isWritePoolFlushing  bool

	stopChan chan struct{}
	ticker   *time.Ticker

	poolMutex sync.Mutex
}

func NewPoolController(db dbTypes.ITableDB, writePoolInterval time.Duration, maxPoolSize int, isWritePoolFlushing, isDeletePoolFlushing bool) dbTypes.ITableDB {
	stmtsOptions := dbTypes.GetLongDefaultShardedCacheOptions()
	controller := &PoolController{
		db:                   db,
		writePoolInterval:    writePoolInterval,
		maxPoolSize:          maxPoolSize,
		isWritePoolFlushing:  isWritePoolFlushing,
		isDeletePoolFlushing: isDeletePoolFlushing,

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
		stopChan: make(chan struct{}),
		ticker:   time.NewTicker(writePoolInterval),
	}

	controller.Start()
	return controller
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
	if !controller.isWritePoolFlushing {
		return controller.db.Set(ctx, tableName, key, value)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

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
	if !controller.isDeletePoolFlushing {
		return controller.db.Delete(ctx, tableName, key)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	if writeKeys, ok := controller.writePool.Get(tableName); ok {
		newWriteKeys := make([]string, 0, len(writeKeys))
		for _, k := range writeKeys {
			if k != key {
				newWriteKeys = append(newWriteKeys, k)
			}
		}

		if len(newWriteKeys) > 0 {
			if err := controller.writePool.Set(tableName, newWriteKeys); err != nil {
				return fmt.Errorf("failed to update write pool: %v", err)
			}
		} else {
			controller.writePool.Delete(tableName)
		}
	}

	deleteKeys, ok := controller.deletePool.Get(tableName)
	if !ok {
		deleteKeys = make([]string, 0, 1)
	}
	deleteKeys = append(deleteKeys, key)

	if err := controller.deletePool.Set(tableName, deleteKeys); err != nil {
		return fmt.Errorf("failed to update delete pool: %v", err)
	}

	if len(deleteKeys) >= controller.maxPoolSize {
		if err := controller.deleteKeys(tableName, deleteKeys); err != nil {
			return fmt.Errorf("failed to flush delete pool: %v", err)
		}
		controller.deletePool.Delete(tableName)
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
	select {
	case <-controller.stopChan:
	default:
		close(controller.stopChan)
		controller.deletePoolFlushAll()
		controller.writePoolFlushAll()

		controller.writePool.Clear()
		controller.deletePool.Clear()
		controller.tables.Clear()
		if err := controller.db.Close(); err != nil {
			log.Printf("db.Close error: %v", err)
			return err
		}
	}

	return nil
}

func (controller *PoolController) writePoolFlush(tableName string) error {
	writeKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		return nil
	}

	err := controller.writeKeys(tableName, writeKeys)
	if err != nil {
		return err
	}
	return nil
}

func (controller *PoolController) writeKeys(tableName string, writeKeys []string) error {
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

	err := controller.deleteKeys(tableName, deleteKeys)
	if err != nil {
		return err
	}
	return nil
}

func (controller *PoolController) deleteKeys(tableName string, deleteKeys []string) error {
	deleteSet := make(map[string]struct{}, len(deleteKeys))
	for _, key := range deleteKeys {
		deleteSet[key] = struct{}{}
	}

	if writeKeys, ok := controller.writePool.Get(tableName); ok {
		newWriteKeys := make([]string, 0, len(writeKeys))
		for _, key := range writeKeys {
			if _, exists := deleteSet[key]; !exists {
				newWriteKeys = append(newWriteKeys, key)
			}
		}

		if len(newWriteKeys) > 0 {
			if err := controller.writePool.Set(tableName, newWriteKeys); err != nil {
				return err
			}
		} else {
			controller.writePool.Delete(tableName)
		}
	}

	for _, key := range deleteKeys {
		if err := controller.db.Delete(context.Background(), tableName, key); err != nil {
			return err
		}
	}

	controller.deletePool.Delete(tableName)
	return nil
}

func (controller *PoolController) Start() {
	go func() {
		for {
			select {
			case <-controller.ticker.C:
				controller.deletePoolFlushAll()
				controller.writePoolFlushAll()
			case <-controller.stopChan:
				return
			}
		}
	}()
}

func (controller *PoolController) deletePoolFlushAll() {
	err := controller.deletePool.Range(func(tableName string, keys []string) bool {
		err := controller.deleteKeys(tableName, keys)
		if err != nil {
			log.Printf("error flushing delete pool for table %s: %v", tableName, err)
		}

		return true
	})
	if err != nil {
		log.Printf("error ranging delete pool: %v", err)
	}
}

func (controller *PoolController) writePoolFlushAll() {
	err := controller.writePool.Range(func(tableName string, keys []string) bool {
		err := controller.writeKeys(tableName, keys)
		if err != nil {
			log.Printf("error flushing write pool for table %s: %v", tableName, err)
		}

		return true
	})
	if err != nil {
		log.Printf("error ranging write pool: %v", err)
	}
}
