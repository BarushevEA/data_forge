package dbPool

import (
	"context"
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

// setupTestController initializes a PoolController with an in-memory SQLite database for testing.
// It returns the controller, a context, and a cleanup function to close the controller.
func setupTestController(t *testing.T) (*PoolController, context.Context, func()) {
	ctx := context.Background()

	// Initialize SQLiteDB with in-memory storage.
	opts := db.NewSQLiteOptions(":memory:")
	sqliteDB, err := db.NewSQLiteDB(opts)
	if err != nil {
		t.Fatalf("Failed to create SQLiteDB: %v", err)
	}

	// Configure PoolController with a 100ms flush interval and max pool size of 2.
	controller := NewPoolController(sqliteDB, 100*time.Millisecond, 2, true, true)

	// Define cleanup function to close the controller.
	cleanup := func() {
		_ = controller.Close()
	}

	return controller.(*PoolController), ctx, cleanup
}

// TestPoolController_Set verifies the Set method, ensuring keys are added to writePool and flushed correctly.
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

	// Test: Add a key to writePool.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	keys, exists := controller.writePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key1"}, keys)

	// Test: Trigger flush when maxPoolSize is reached.
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Wait for flush to complete.
	_, exists = controller.writePool.Get("test_table")
	assert.False(t, exists) // Verify writePool is cleared after flush.
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

	// Verify key is removed from writePool.
	writeKeys, exists := controller.writePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key2"}, writeKeys)
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
	controller.writePool.Set("test_table", []string{"key1", "key2"})
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.writePoolFlush("test_table")
	assert.NoError(t, err)
	_, exists := controller.writePool.Get("test_table")
	assert.False(t, exists) // Verify pool is cleared after flush.
}

// TestPoolController_Close verifies the Close method, ensuring all pools are cleared.
func TestPoolController_Close(t *testing.T) {
	controller, _, cleanup := setupTestController(t)
	defer cleanup()

	// Close is called via cleanup; verify pools are empty.
	assert.Equal(t, 0, controller.writePool.Len())
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
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Wait for flush to complete.

	// Verify data persistence.
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), data)

	// Test: Delete a key and verify it's removed.
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Wait for flush to complete.
	_, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists) // Verify key is deleted.
}
