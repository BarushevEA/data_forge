package dbPool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ... (mockTableRegister и mockTableDB остаются без изменений из grok_test.go)

// setupTestControllerGet initializes a PoolController with a mock database for testing.
// It returns the controller, a context, and a cleanup function to close the controller.
func setupTestControllerGet(t *testing.T) (*PoolController, context.Context, func()) {
	ctx := context.Background()

	// Initialize mock database.
	dbMock := &mockTableDB{}
	dbMock.On("CreateTable", mock.Anything, mock.Anything).Return(nil)
	dbMock.On("Delete", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	dbMock.On("Close").Return(nil).Maybe()

	// Configure PoolController with a 100ms flush interval and max pool size of 2.
	controller := NewPoolController(dbMock, 100*time.Millisecond, 2, true, true)

	// Define a cleanup function to close the controller.
	cleanup := func() {
		_ = controller.Close()
	}

	return controller.(*PoolController), ctx, cleanup
}

// TestPoolController_Consistency verifies that Get respects deletePool to prevent desynchronization.
func TestPoolController_Consistency(t *testing.T) {
	// Initialize mock database.
	dbMock := &mockTableDB{}
	dbMock.On("CreateTable", mock.Anything, "test_table").Return(nil).Once()
	dbMock.On("Get", mock.Anything, "test_table", "key1").Return([]byte("value1"), true, nil).Maybe()
	dbMock.On("Delete", mock.Anything, "test_table", "key1").Return(nil).Maybe()

	// Initialize the controller with pooling enabled.
	controller := NewPoolController(dbMock, 100*time.Millisecond, 5, true, true).(*PoolController)
	ctx := context.Background()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Delete a key, adding it to deletePool.
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)

	// Verify Get returns not found due to deletePool.
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, data)

	// Ensure the database was not queried for a deleted key.
	dbMock.AssertNotCalled(t, "Get", mock.Anything, "test_table", "key1")
}

// TestPoolController_get_Integration tests the full lifecycle of PoolController operations.
func TestPoolController_get_Integration(t *testing.T) {
	controller, ctx, cleanup := setupTestControllerGet(t)
	defer cleanup()

	// Get the mock database from the controller.
	dbMock := controller.db.(*mockTableDB)

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
	dbMock.On("BatchSet", mock.Anything, "test_table", mock.MatchedBy(func(items map[string][]byte) bool {
		return len(items) == 2 && string(items["key1"]) == "value1" && string(items["key2"]) == "value2"
	})).Return(nil).Once()
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Wait for the flush to complete.

	// Verify data persistence.
	dbMock.On("Get", mock.Anything, "test_table", "key1").Return([]byte("value1"), true, nil).Once()
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), data)

	// Test: Delete a key and verify it's marked as deleted immediately.
	dbMock.On("Delete", mock.Anything, "test_table", "key1").Return(nil).Once()
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	data, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists) // Key is in deletePool, should not be found.
	assert.Nil(t, data)

	// Wait for a flush and verify the key is deleted.
	time.Sleep(200 * time.Millisecond) // Wait for the flush to complete.
	dbMock.On("Get", mock.Anything, "test_table", "key1").Return([]byte{}, false, nil).Once()
	data, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Empty(t, data) // Проверяем, что data — пустой срез
}
