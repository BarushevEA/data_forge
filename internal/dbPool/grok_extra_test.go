package dbPool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestPoolController_Consistency verifies that Get respects deletePool to prevent desynchronization.
func TestPoolController_Extra_Consistency(t *testing.T) {
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
func TestPoolController_extra_get_Integration(t *testing.T) {
	controller, ctx, cleanup := setupTestControllerGet(t)
	defer cleanup()

	// Cast the db to the mock type to configure it.
	dbMock := controller.db.(*mockTableDB)

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Register a mock ITableRegister (if required by your implementation).
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Configure serialization for Set operations.
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()

	// Mock BatchSet to simulate flush behavior.
	dbMock.On("BatchSet", mock.Anything, "test_table", mock.MatchedBy(func(items map[string][]byte) bool {
		return len(items) == 2 && string(items["key1"]) == "value1" && string(items["key2"]) == "value2"
	})).Return(nil).Once()

	// Set keys.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Wait for a flush (100 ms interval + buffer).

	// Configure Get after a flush.
	dbMock.On("Get", mock.Anything, "test_table", "key1").Return([]byte("value1"), true, nil).Once()
	dbMock.On("Get", mock.Anything, "test_table", "key2").Return([]byte("value2"), true, nil).Maybe()

	// Verify data persistence.
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)                  // Should pass now.
	assert.Equal(t, []byte("value1"), data) // Should pass now.

	// Delete a key.
	dbMock.On("Delete", mock.Anything, "test_table", "key1").Return(nil).Once()
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)

	// Immediately after Delete, the key is in deletePool, so Get returns nil.
	data, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, data) // Should pass due to deletePool check.

	// Wait for flushed to complete deletion.
	time.Sleep(200 * time.Millisecond)

	// After flush, configure Get for a deleted key.
	dbMock.On("Get", mock.Anything, "test_table", "key1").Return([]byte{}, false, nil).Once()

	// Verify the key is gone.
	data, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Empty(t, data) // Use asserting.Empty if expecting []byte{}.
	// Alternatively, if db.Get should return nil:
	// assert.Nil(t, data)
}
