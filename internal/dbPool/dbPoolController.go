package dbPool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/BarushevEA/in_memory_cache/pkg"
	"github.com/BarushevEA/in_memory_cache/types"
)

// PoolController manages a buffered interface to a database with writing and delete pools.
type PoolController struct {
	db dbTypes.ITableDB // Underlying database implementation.

	// writePoolInterval is the interval between flushing accumulated data from pools to the database.
	writePoolInterval time.Duration

	// maxPoolSize is the maximum number of keys in a pool before triggering a flush.
	maxPoolSize int

	// writePool stores keys pending to be written to the database.
	writePool types.ICacheInMemory[[]string]

	// deletePool stores keys pending to be deleted from the database.
	deletePool types.ICacheInMemory[[]string]

	// tables stores registered table serializers.
	tables types.ICacheInMemory[dbTypes.ITableRegister]

	// isDeletePoolFlushing enables or disables the delete pool; if false, deletes bypass the pool.
	isDeletePoolFlushing bool

	// isWritePoolFlushing enables or disables the writing pool; if false, writes bypass the pool.
	isWritePoolFlushing bool

	// stopChan signals the background flush goroutine to stop.
	stopChan chan struct{}

	// ticker triggers periodic pool flushes.
	ticker *time.Ticker

	// poolMutex synchronizes access to pools.
	poolMutex sync.Mutex
}

// NewPoolController creates a new PoolController with the specified database and pool settings.
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

	// Start the background flush goroutine.
	controller.Start()
	return controller
}

// RegisterTable associates a table name with its serializer.
func (controller *PoolController) RegisterTable(tableName string, table dbTypes.ITableRegister) error {
	return controller.tables.Set(tableName, table)
}

// CreateTable creates a new table in the underlying database.
func (controller *PoolController) CreateTable(ctx context.Context, name string) error {
	return controller.db.CreateTable(ctx, name)
}

// DropTable removes a table from the database and clears associated pools.
func (controller *PoolController) DropTable(ctx context.Context, name string) error {
	// Clear pools and table registry for the table.
	controller.writePool.Delete(name)
	controller.deletePool.Delete(name)
	controller.tables.Delete(name)
	return controller.db.DropTable(ctx, name)
}

// Set adds a key-value pair to the writing pool or directly to the database if pooling is disabled.
func (controller *PoolController) Set(ctx context.Context, tableName, key string, value []byte) error {
	if !controller.isWritePoolFlushing {
		// Bypass the pool and write directly to the database.
		return controller.db.Set(ctx, tableName, key, value)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	// Get or initialize write pool keys for the table.
	poolKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		poolKeys = make([]string, 0, controller.maxPoolSize)
	}

	// Add a key to the pool.
	poolKeys = append(poolKeys, key)
	err := controller.writePool.Set(tableName, poolKeys)
	if err != nil {
		return err
	}

	// Flush pool if max size is reached.
	if len(poolKeys) >= controller.maxPoolSize {
		return controller.writePoolFlush(tableName)
	}

	return nil
}

// Get retrieves a value from the database by table name and key.
func (controller *PoolController) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	if controller.isDeletePoolFlushing {
		if keys, exists := controller.deletePool.Get(tableName); exists {
			for _, deletedKey := range keys {
				if deletedKey == key {
					return nil, false, nil
				}
			}
		}
	}

	return controller.db.Get(ctx, tableName, key)
}

// Delete adds a key to the delete pool or deletes it directly from the database if pooling is disabled.
func (controller *PoolController) Delete(ctx context.Context, tableName, key string) error {
	if !controller.isDeletePoolFlushing {
		// Bypass pool and delete directly from the database.
		return controller.db.Delete(ctx, tableName, key)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	// Remove key from writing pool if present.
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

	// Add key to delete pool.
	deleteKeys, ok := controller.deletePool.Get(tableName)
	if !ok {
		deleteKeys = make([]string, 0, 1)
	}
	deleteKeys = append(deleteKeys, key)

	if err := controller.deletePool.Set(tableName, deleteKeys); err != nil {
		return fmt.Errorf("failed to update delete pool: %v", err)
	}

	// Flush delete pool if max size is reached.
	if len(deleteKeys) >= controller.maxPoolSize {
		if err := controller.deleteKeys(tableName, deleteKeys); err != nil {
			return fmt.Errorf("failed to flush delete pool: %v", err)
		}
		controller.deletePool.Delete(tableName)
	}

	return nil
}

// BatchSet writes multiple key-value pairs to the database.
func (controller *PoolController) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	return controller.db.BatchSet(ctx, tableName, items)
}

// BatchGet retrieves multiple values from the database by table name and keys.
func (controller *PoolController) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	return controller.db.BatchGet(ctx, tableName, keys)
}

// Close shuts down the controller, flushing pools and closing the database.
func (controller *PoolController) Close() error {
	select {
	case <-controller.stopChan:
		// Already closed, skip operations.
	default:
		// Stop the background flush goroutine.
		close(controller.stopChan)
		// Flush all pending operations.
		controller.deletePoolFlushAll()
		controller.writePoolFlushAll()
		// Clear all pools.
		controller.writePool.Clear()
		controller.deletePool.Clear()
		controller.tables.Clear()
		// Close the database connection.
		if err := controller.db.Close(); err != nil {
			log.Printf("db.Close error: %v", err)
			return err
		}
	}

	return nil
}

// writePoolFlush flushes the writing pool for a specific table to the database.
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

// writeKeys writes a batch of keys from the writing pool to the database.
func (controller *PoolController) writeKeys(tableName string, writeKeys []string) error {
	// Get table serializer.
	table, ok := controller.tables.Get(tableName)
	if !ok {
		return nil
	}

	// Serialize keys to values.
	batch := make(map[string][]byte)
	for _, key := range writeKeys {
		value, err := table.Serialize(key)
		if err != nil {
			return err
		}
		batch[key] = value
	}

	// Write a batch to a database.
	err := controller.db.BatchSet(context.Background(), tableName, batch)
	if err != nil {
		return err
	}

	// Clear the pool for the table.
	controller.writePool.Delete(tableName)
	return nil
}

// deletePoolFlush flushes the delete pool for a specific table to the database.
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

// deleteKeys removes a batch of keys from the database and updates the writing pool.
func (controller *PoolController) deleteKeys(tableName string, deleteKeys []string) error {
	// Create a set of keys to delete.
	deleteSet := make(map[string]struct{}, len(deleteKeys))
	for _, key := range deleteKeys {
		deleteSet[key] = struct{}{}
	}

	// Remove deleted keys from the writing pool.
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

	// Delete keys from a database.
	for _, key := range deleteKeys {
		if err := controller.db.Delete(context.Background(), tableName, key); err != nil {
			return err
		}
	}

	// Clear the delete pool for the table.
	controller.deletePool.Delete(tableName)
	return nil
}

// Start launches a background goroutine to periodically flush pools.
func (controller *PoolController) Start() {
	go func() {
		for {
			select {
			case <-controller.ticker.C:
				// Periodically flush all pools.
				if controller.isDeletePoolFlushing {
					controller.deletePoolFlushAll()
				}
				if controller.isWritePoolFlushing {
					controller.writePoolFlushAll()
				}
			case <-controller.stopChan:
				// Stop goroutine on close.
				return
			}
		}
	}()
}

// deletePoolFlushAll flushes all delete pools, logging any errors.
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

// writePoolFlushAll flushes all writing pools, logging any errors.
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
