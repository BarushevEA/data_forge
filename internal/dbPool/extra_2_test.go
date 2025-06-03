package dbPool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockTableRegister is a mock implementation of dbTypes.ITableRegister for testing purposes.
type mockTableRegister struct {
	mock.Mock
}

// Serialize mocks the Serialize method of ITableRegister, returning a byte slice and an error.
func (m *mockTableRegister) Serialize(key string) ([]byte, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Error(1)
}

func setupTestController(t *testing.T) (*PoolController, context.Context, func()) {
	tempDir, err := os.MkdirTemp("", "test_sqlite_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	ctx := context.Background()

	// Initialize SQLiteDB with file storage.
	opts := db.NewSQLiteOptions(dbPath)
	sqliteDB, err := db.NewSQLiteDB(opts)
	if err != nil {
		t.Fatalf("Failed to create SQLiteDB: %v", err)
	}

	// Configure PoolController with a 100ms flush interval and max pool size of 2.
	// Set SKIP_FLUSH to disable background flush in tests
	os.Setenv("SKIP_FLUSH", "1")
	controller := NewPoolController(sqliteDB, 100*time.Millisecond, 2, true, true).(*PoolController)
	os.Unsetenv("SKIP_FLUSH")

	// Define cleanup function to close the controller and remove temp dir.
	cleanup := func() {
		_ = controller.Close()
		os.RemoveAll(tempDir)
	}

	return controller, ctx, cleanup
}

// TestPoolController_Set verifies the Set method, ensuring keys are added to writePool and writePoolBoofer.
func TestPoolController_Set(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Register a mock ITableRegister.
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Test: Add a key to writePool and writePoolBoofer.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	keys, exists := controller.writePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key1"}, keys)
	value, exists := controller.writePoolBoofer.Get("test_table:key1")
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), value)

	// Test: Trigger flush when maxPoolSize is reached.
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	// Verify flush occurred due to maxPoolSize=2
	_, exists = controller.writePool.Get("test_table")
	assert.False(t, exists) // Verify writePool is cleared after flush
	_, exists = controller.writePoolBoofer.Get("test_table:key1")
	assert.False(t, exists) // Verify writePoolBoofer is cleared after flush
	_, exists = controller.writePoolBoofer.Get("test_table:key2")
	assert.False(t, exists)

	// Verify data in DB
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), data)
}

// TestPoolController_Delete verifies the Delete method, ensuring keys are added to deletePool and removed from writePool.
func TestPoolController_Delete(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Add keys to writePool.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)

	// Test: Add key to deletePool.
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	deleteKeys, exists := controller.deletePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key1"}, deleteKeys)

	// Verify key is removed from writePool and writePoolBoofer.
	_, exists = controller.writePool.Get("test_table")
	assert.False(t, exists) // writePool cleared due to flush
	_, exists = controller.writePoolBoofer.Get("test_table:key1")
	assert.False(t, exists)
}

// TestPoolController_writePoolFlush verifies the writePoolFlush method for both empty and populated pools.
func TestPoolController_writePoolFlush(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Register a mock ITableRegister.
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Test: Flush an empty pool.
	err = controller.writePoolFlush("test_table")
	assert.NoError(t, err)

	// Test: Flush a populated pool.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	// Flush triggered by maxPoolSize
	_, exists := controller.writePool.Get("test_table")
	assert.False(t, exists) // Verify pool is cleared after flush
	_, exists = controller.writePoolBoofer.Get("test_table:key1")
	assert.False(t, exists)
	_, exists = controller.writePoolBoofer.Get("test_table:key2")
	assert.False(t, exists)
}

// TestPoolController_Close verifies the Close method, ensuring all pools are cleared.
func TestPoolController_Close(t *testing.T) {
	controller, _, cleanup := setupTestController(t)
	defer cleanup()

	// Close is called via cleanup; verify pools are empty.
	assert.Equal(t, 0, controller.writePool.Len())
	assert.Equal(t, 0, controller.writePoolBoofer.Len())
	assert.Equal(t, 0, controller.deletePool.Len())
	assert.Equal(t, 0, controller.tables.Len())
}

// TestPoolController_Integration tests the full lifecycle of PoolController operations, including Set, Get, and Delete.
func TestPoolController_Integration(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Register a mock ITableRegister.
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Test: Set keys and trigger flush.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	// Flush triggered by maxPoolSize

	// Verify data persistence.
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), data)

	// Test: Delete a key and verify it's removed.
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	data, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists) // Verify key is not in writePoolBoofer or deletePool
	assert.Nil(t, data)

	// Explicit flush for delete
	err = controller.deletePoolFlush("test_table")
	assert.NoError(t, err)
	_, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists) // Verify key is deleted from DB
}

// TestPoolController_Consistency verifies that Get respects deletePool and writePoolBoofer to prevent desynchronization.
func TestPoolController_extra_2_Consistency(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Register a mock ITableRegister.
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Set a key
	key := "test_key"
	value := []byte("test_value")
	err = controller.Set(ctx, "test_table", key, value)
	assert.NoError(t, err)

	// Verify Get returns value from writePoolBoofer
	data, exists, err := controller.Get(ctx, "test_table", key)
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, value, data)

	// Delete the key
	err = controller.Delete(ctx, "test_table", key)
	assert.NoError(t, err)

	// Verify Get returns not found due to deletePool
	data, exists, err = controller.Get(ctx, "test_table", key)
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, data)

	// Explicit flush
	err = controller.deletePoolFlush("test_table")
	assert.NoError(t, err)
}
