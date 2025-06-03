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

// PoolController manages a buffered interface to a database with write and delete pools.
type PoolController struct {
	db dbTypes.ITableDB // Underlying database implementation.

	// writePoolInterval is the interval between flushing accumulated data from pools to the database.
	writePoolInterval time.Duration

	// maxPoolSize is the maximum number of keys in a pool before triggering a flush.
	maxPoolSize int

	// writePool stores keys pending to be written to the database.
	writePool types.ICacheInMemory[[]string]

	// writePoolBoofer stores values for keys in writePool, keyed by tableName:key.
	writePoolBoofer types.ICacheInMemory[[]byte]

	// deletePool stores keys pending to be deleted from the database.
	deletePool types.ICacheInMemory[[]string]

	// isDeletePoolFlushing enables or disables the delete pool; if false, deletes bypass the pool.
	isDeletePoolFlushing bool

	// isWritePoolFlushing enables or disables the write pool; if false, writes bypass the pool.
	isWritePoolFlushing bool

	// sigStopChan signals the background flush goroutine to stop.
	sigStopChan chan struct{}

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
		writePoolBoofer: pkg.NewShardedCache[[]byte](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		deletePool: pkg.NewShardedCache[[]string](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		sigStopChan: make(chan struct{}),
		ticker:      time.NewTicker(writePoolInterval),
	}

	// Start the background flush goroutine.
	controller.Start()
	return controller
}

// RegisterTable associates a table name with its schema.
func (controller *PoolController) RegisterTable(tableName string, table dbTypes.ITableRegister) error {
	return nil
}

// CreateTable creates a new table in the underlying database.
func (controller *PoolController) CreateTable(ctx context.Context, name string) error {
	return controller.db.CreateTable(ctx, name)
}

// DropTable removes a table from the database and clears associated pools.
func (controller *PoolController) DropTable(ctx context.Context, name string) error {
	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	// Clear pools and table registry for the table.
	if keys, ok := controller.writePool.Get(name); ok {
		for _, key := range keys {
			controller.writePoolBoofer.Delete(name + ":" + key)
		}
	}
	controller.writePool.Delete(name)
	controller.deletePool.Delete(name)
	return controller.db.DropTable(ctx, name)
}

// Set adds a key-value pair to the write pool or directly to the database if pooling is disabled.
func (controller *PoolController) Set(ctx context.Context, tableName, key string, value []byte) error {
	if !controller.isWritePoolFlushing {
		// Bypass pool and write directly to the database.
		return controller.db.Set(ctx, tableName, key, value)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	//log.Printf("Set: table=%s, key=%s", tableName, key)

	// Get or initialize write pool keys for the table.
	poolKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		poolKeys = make([]string, 0, controller.maxPoolSize)
	}

	// Add key to the pool.
	poolKeys = append(poolKeys, key)
	if err := controller.writePool.Set(tableName, poolKeys); err != nil {
		return err
	}

	// Store value in writePoolBoofer.
	if err := controller.writePoolBoofer.Set(tableName+":"+key, value); err != nil {
		return err
	}

	// Flush pool if max size is reached.
	if len(poolKeys) >= controller.maxPoolSize {
		// Unlock mutex before flush to avoid holding it during DB operations
		controller.poolMutex.Unlock()
		defer controller.poolMutex.Lock()
		return controller.writePoolFlush(tableName)
	}

	return nil
}

// Get retrieves a value, checking deletePool and writePoolBoofer first, then the database.
func (controller *PoolController) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	//log.Printf("Get: table=%s, key=%s", tableName, key)

	// Check deletePool: if key is marked for deletion, treat as not found.
	if controller.isDeletePoolFlushing {
		if keys, exists := controller.deletePool.Get(tableName); exists {
			for _, deletedKey := range keys {
				if deletedKey == key {
					return nil, false, nil
				}
			}
		}
	}

	// Check writePoolBoofer: return value if key is pending write.
	if controller.isWritePoolFlushing {
		if value, exists := controller.writePoolBoofer.Get(tableName + ":" + key); exists {
			return value, true, nil
		}
	}

	// Fall back to database.
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

	//log.Printf("Delete: table=%s, key=%s", tableName, key)

	// Remove key from write pool if present.
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

	// Remove value from writePoolBoofer.
	controller.writePoolBoofer.Delete(tableName + ":" + key)

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
		// Unlock mutex before flush to avoid holding it during DB operations
		controller.poolMutex.Unlock()
		defer controller.poolMutex.Lock()
		if err := controller.deleteKeys(tableName, deleteKeys); err != nil {
			return fmt.Errorf("failed to flush delete pool: %v", err)
		}
		controller.deletePool.Delete(tableName)
	}

	return nil
}

// BatchSet writes multiple key-value pairs to the write pool or database.
func (controller *PoolController) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	if !controller.isWritePoolFlushing {
		return controller.db.BatchSet(ctx, tableName, items)
	}

	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	//log.Printf("BatchSet: table=%s, items=%d", tableName, len(items))

	// Add each item to write pool and writePoolBoofer.
	poolKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		poolKeys = make([]string, 0, controller.maxPoolSize)
	}
	for key, value := range items {
		poolKeys = append(poolKeys, key)
		if err := controller.writePoolBoofer.Set(tableName+":"+key, value); err != nil {
			return err
		}
	}
	if err := controller.writePool.Set(tableName, poolKeys); err != nil {
		return err
	}

	// Flush pool if max size is reached.
	if len(poolKeys) >= controller.maxPoolSize {
		// Unlock mutex before flush to avoid holding it during DB operations
		controller.poolMutex.Unlock()
		defer controller.poolMutex.Lock()
		return controller.writePoolFlush(tableName)
	}

	return nil
}

// BatchGet retrieves multiple values, respecting deletePool and writePoolBoofer.
func (controller *PoolController) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	controller.poolMutex.Lock()
	defer controller.poolMutex.Unlock()

	//log.Printf("BatchGet: table=%s, keys=%d", tableName, len(keys))

	result := make(map[string][]byte)

	// Filter out deleted keys and check writePoolBoofer.
	var dbKeys []string
	if controller.isDeletePoolFlushing {
		deleteSet := make(map[string]struct{})
		if deletedKeys, exists := controller.deletePool.Get(tableName); exists {
			for _, key := range deletedKeys {
				deleteSet[key] = struct{}{}
			}
		}

		for _, key := range keys {
			if _, deleted := deleteSet[key]; deleted {
				continue
			}
			if controller.isWritePoolFlushing {
				if value, exists := controller.writePoolBoofer.Get(tableName + ":" + key); exists {
					result[key] = value
					continue
				}
			}
			dbKeys = append(dbKeys, key)
		}
	} else {
		dbKeys = keys
	}

	// Fetch remaining keys from database.
	if len(dbKeys) > 0 {
		dbResult, err := controller.db.BatchGet(ctx, tableName, dbKeys)
		if err != nil {
			return nil, err
		}
		for key, value := range dbResult {
			result[key] = value
		}
	}

	return result, nil
}

// Close shuts down the controller, flushing pools and closing the database.
func (controller *PoolController) Close() error {
	select {
	case <-controller.sigStopChan:
		// Already closed, skip operations.
	default:
		// Stop background flush goroutine.
		close(controller.sigStopChan)
		// Flush all pending operations.
		controller.deletePoolFlushAll()
		controller.writePoolFlushAll()
		// Clear all pools.
		controller.writePool.Clear()
		controller.writePoolBoofer.Clear()
		controller.deletePool.Clear()
		// Close the database connection.
		if err := controller.db.Close(); err != nil {
			log.Printf("db.Close error: %v", err)
			return err
		}
	}

	return nil
}

// writePoolFlush flushes the write pool for a specific table to the database.
func (controller *PoolController) writePoolFlush(tableName string) error {
	//log.Printf("writePoolFlush: table=%s", tableName)

	// Copy keys to avoid holding mutex during DB operations
	controller.poolMutex.Lock()
	writeKeys, ok := controller.writePool.Get(tableName)
	if !ok {
		controller.poolMutex.Unlock()
		return nil
	}
	// Copy keys to local slice
	keysCopy := make([]string, len(writeKeys))
	copy(keysCopy, writeKeys)
	controller.poolMutex.Unlock()

	err := controller.writeKeys(tableName, keysCopy)
	if err != nil {
		return err
	}
	return nil
}

// writeKeys writes a batch of keys from the write pool to the database.
func (controller *PoolController) writeKeys(tableName string, writeKeys []string) error {
	//log.Printf("writeKeys: table=%s, keys=%d", tableName, len(writeKeys))

	// Build batch from writePoolBoofer
	batch := make(map[string][]byte)
	controller.poolMutex.Lock()
	for _, key := range writeKeys {
		if value, exists := controller.writePoolBoofer.Get(tableName + ":" + key); exists {
			batch[key] = value
			controller.writePoolBoofer.Delete(tableName + ":" + key)
		}
	}
	// Clear pool for the table
	controller.writePool.Delete(tableName)
	controller.poolMutex.Unlock()

	if len(batch) == 0 {
		return nil
	}

	// Write batch to database outside mutex
	err := controller.db.BatchSet(context.Background(), tableName, batch)
	if err != nil {
		return err
	}

	return nil
}

// deletePoolFlush flushes the delete pool for a specific table to the database.
func (controller *PoolController) deletePoolFlush(tableName string) error {
	//log.Printf("deletePoolFlush: table=%s", tableName)

	// Copy keys to avoid holding mutex during DB operations
	controller.poolMutex.Lock()
	deleteKeys, ok := controller.deletePool.Get(tableName)
	if !ok {
		controller.poolMutex.Unlock()
		return nil
	}
	// Copy keys to local slice
	keysCopy := make([]string, len(deleteKeys))
	copy(keysCopy, deleteKeys)
	controller.poolMutex.Unlock()

	err := controller.deleteKeys(tableName, keysCopy)
	if err != nil {
		return err
	}
	return nil
}

// deleteKeys removes a batch of keys from the database and updates the write pool.
func (controller *PoolController) deleteKeys(tableName string, deleteKeys []string) error {
	//log.Printf("deleteKeys: table=%s, keys=%d", tableName, len(deleteKeys))

	// Create a set of keys to delete
	deleteSet := make(map[string]struct{}, len(deleteKeys))
	for _, key := range deleteKeys {
		deleteSet[key] = struct{}{}
	}

	// Update pools
	controller.poolMutex.Lock()
	// Remove deleted keys from write pool and writePoolBoofer
	if writeKeys, ok := controller.writePool.Get(tableName); ok {
		newWriteKeys := make([]string, 0, len(writeKeys))
		for _, key := range writeKeys {
			if _, exists := deleteSet[key]; !exists {
				newWriteKeys = append(newWriteKeys, key)
			} else {
				controller.writePoolBoofer.Delete(tableName + ":" + key)
			}
		}

		if len(newWriteKeys) > 0 {
			if err := controller.writePool.Set(tableName, newWriteKeys); err != nil {
				controller.poolMutex.Unlock()
				return err
			}
		} else {
			controller.writePool.Delete(tableName)
		}
	}

	// Clear delete pool for the table
	controller.deletePool.Delete(tableName)
	controller.poolMutex.Unlock()

	// Delete keys from database outside mutex
	for _, key := range deleteKeys {
		if err := controller.db.Delete(context.Background(), tableName, key); err != nil {
			return err
		}
	}

	return nil
}

// Start launches a background goroutine to periodically flush pools.
func (controller *PoolController) Start() {
	go func() {
		//log.Println("Start: background flush goroutine started")
		for {
			select {
			case <-controller.ticker.C:
				//log.Println("Ticker: periodic flush triggered")
				// Periodically flush all pools if enabled
				if controller.isDeletePoolFlushing {
					controller.deletePoolFlushAll()
				}
				if controller.isWritePoolFlushing {
					controller.writePoolFlushAll()
				}
			case <-controller.sigStopChan:
				//log.Println("Start: background flush goroutine stopped")
				return
			}
		}
	}()
}

// deletePoolFlushAll flushes all delete pools, logging any errors.
func (controller *PoolController) deletePoolFlushAll() {
	//log.Println("deletePoolFlushAll: started")

	// Collect all tables and keys to flush
	type tableKeys struct {
		tableName string
		keys      []string
	}
	var toFlush []tableKeys

	controller.poolMutex.Lock()
	err := controller.deletePool.Range(func(tableName string, keys []string) bool {
		keysCopy := make([]string, len(keys))
		copy(keysCopy, keys)
		toFlush = append(toFlush, tableKeys{tableName: tableName, keys: keysCopy})
		return true
	})
	controller.poolMutex.Unlock()

	if err != nil {
		log.Printf("error ranging delete pool: %v", err)
	}

	// Flush outside mutex
	for _, tk := range toFlush {
		if err := controller.deleteKeys(tk.tableName, tk.keys); err != nil {
			log.Printf("error flushing delete pool for table %s: %v", tk.tableName, err)
		}
	}

	//log.Println("deletePoolFlushAll: completed")
}

// writePoolFlushAll flushes all write pools, logging any errors.
func (controller *PoolController) writePoolFlushAll() {
	//log.Println("writePoolFlushAll: started")

	// Collect all tables and keys to flush
	type tableKeys struct {
		tableName string
		keys      []string
	}
	var toFlush []tableKeys

	controller.poolMutex.Lock()
	err := controller.writePool.Range(func(tableName string, keys []string) bool {
		keysCopy := make([]string, len(keys))
		copy(keysCopy, keys)
		toFlush = append(toFlush, tableKeys{tableName: tableName, keys: keysCopy})
		return true
	})
	controller.poolMutex.Unlock()

	if err != nil {
		log.Printf("error ranging write pool: %v", err)
	}

	// Flush outside mutex
	for _, tk := range toFlush {
		if err := controller.writeKeys(tk.tableName, tk.keys); err != nil {
			log.Printf("error flushing write pool for table %s: %v", tk.tableName, err)
		}
	}

	//log.Println("writePoolFlushAll: completed")
}
