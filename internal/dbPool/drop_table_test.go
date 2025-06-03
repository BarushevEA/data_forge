package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

// MockITableDB is a mock implementation of the ITableDB interface for testing purposes
type MockITableDB struct {
	mock.Mock
}

// RegisterTable mocks the registration of a table
func (m *MockITableDB) RegisterTable(tableName string, table dbTypes.ITableRegister) error {
	args := m.Called(tableName, table)
	return args.Error(0)
}

// CreateTable mocks the creation of a table
func (m *MockITableDB) CreateTable(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// DropTable mocks the deletion of a table
func (m *MockITableDB) DropTable(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// Set mocks setting a key-value pair in a table
func (m *MockITableDB) Set(ctx context.Context, tableName, key string, value []byte) error {
	args := m.Called(ctx, tableName, key, value)
	return args.Error(0)
}

// Get mocks retrieving a value by key from a table
func (m *MockITableDB) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	args := m.Called(ctx, tableName, key)
	return args.Get(0).([]byte), args.Bool(1), args.Error(2)
}

// Delete mocks deleting a key from a table
func (m *MockITableDB) Delete(ctx context.Context, tableName, key string) error {
	args := m.Called(ctx, tableName, key)
	return args.Error(0)
}

// BatchSet mocks setting multiple key-value pairs in a table
func (m *MockITableDB) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	args := m.Called(ctx, tableName, items)
	return args.Error(0)
}

// BatchGet mocks retrieving multiple values by keys from a table
func (m *MockITableDB) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	args := m.Called(ctx, tableName, keys)
	return args.Get(0).(map[string][]byte), args.Error(1)
}

// Close mocks closing the database connection
func (m *MockITableDB) Close() error {
	args := m.Called()
	return args.Error(0)
}

// TestPoolController_DropTable tests the DropTable functionality of PoolController
func TestPoolController_DropTable(t *testing.T) {
	// Initialize a mock database
	mockDB := &MockITableDB{}
	// Create a PoolController instance with specified parameters
	pc := NewPoolController(mockDB, time.Second, 10, true, true).(*PoolController)

	// 1. Test table creation
	mockDB.On("CreateTable", mock.Anything, "T").Return(nil).Once()
	err := pc.CreateTable(context.Background(), "T")
	assert.NoError(t, err, "Expected no error when creating table")

	// 2. Test setting data with buffering enabled
	mockDB.On("Set", mock.Anything, "T", "key1", []byte("value1")).Return(nil).Maybe()
	err = pc.Set(context.Background(), "T", "key1", []byte("value1"))
	assert.NoError(t, err, "Expected no error when setting key1")
	// Verify that Set was not called on mockDB (data is buffered)
	mockDB.AssertNotCalled(t, "Set", mock.Anything, "T", "key1", []byte("value1"))

	// Verify that data is stored in the write pool
	keys, exists := pc.writePool.Get("T")
	assert.True(t, exists, "Write pool should contain table T")
	assert.Equal(t, []string{"key1"}, keys, "Write pool should contain key1")
	value, exists := pc.writePoolBoofer.Get("T:key1")
	assert.True(t, exists, "Write pool buffer should contain key1")
	assert.Equal(t, []byte("value1"), value, "Write pool buffer should contain value1")

	// 3. Test dropping the table
	mockDB.On("DropTable", mock.Anything, "T").Return(nil).Once()
	err = pc.DropTable(context.Background(), "T")
	assert.NoError(t, err, "Expected no error when dropping table")

	// 4. Verify that pools are cleared after dropping the table
	_, exists = pc.writePool.Get("T")
	assert.False(t, exists, "Write pool should be cleared after DropTable")
	_, exists = pc.writePoolBoofer.Get("T:key1")
	assert.False(t, exists, "Write pool buffer should be cleared after DropTable")
	_, exists = pc.deletePool.Get("T")
	assert.False(t, exists, "Delete pool should be cleared after DropTable")
	_, exists = pc.tables.Get("T")
	assert.False(t, exists, "Tables cache should be cleared after DropTable")

	// 5. Verify that getting data from a dropped table returns nil, false, nil
	mockDB.On("Get", mock.Anything, "T", "key1").Return([]byte(nil), false, nil).Once()
	data, found, err := pc.Get(context.Background(), "T", "key1")
	assert.NoError(t, err, "Expected no error when getting from dropped table")
	assert.False(t, found, "Expected not found when getting from dropped table")
	assert.Nil(t, data, "Expected nil data when getting from dropped table")

	// 6. Test recreating the table
	mockDB.On("CreateTable", mock.Anything, "T").Return(nil).Once()
	err = pc.CreateTable(context.Background(), "T")
	assert.NoError(t, err, "Expected no error when recreating table")

	// 7. Verify that pools are empty before the first Set operation
	_, exists = pc.writePool.Get("T")
	assert.False(t, exists, "Write pool should be empty before first Set")
	_, exists = pc.writePoolBoofer.Get("T:key2")
	assert.False(t, exists, "Write pool buffer should be empty before first Set")

	// 8. Test setting data for the new table
	mockDB.On("Set", mock.Anything, "T", "key2", []byte("value2")).Return(nil).Maybe()
	err = pc.Set(context.Background(), "T", "key2", []byte("value2"))
	assert.NoError(t, err, "Expected no error when setting key2")
	// Verify that data is added to the write pool
	keys, exists = pc.writePool.Get("T")
	assert.True(t, exists, "Write pool should contain table T after Set")
	assert.Equal(t, []string{"key2"}, keys, "Write pool should contain key2")
	value, exists = pc.writePoolBoofer.Get("T:key2")
	assert.True(t, exists, "Write pool buffer should contain key2")
	assert.Equal(t, []byte("value2"), value, "Write pool buffer should contain value2")

	// 9. Verify that data is written to the database after flushing
	mockDB.On("BatchSet", mock.Anything, "T", mock.MatchedBy(func(items map[string][]byte) bool {
		return len(items) == 1 && string(items["key2"]) == "value2"
	})).Return(nil).Once()
	time.Sleep(time.Second * 2) // Wait for flushing
	mockDB.On("Get", mock.Anything, "T", "key2").Return([]byte("value2"), true, nil).Once()
	data, found, err = pc.Get(context.Background(), "T", "key2")
	assert.NoError(t, err, "Expected no error when getting key2 from new table")
	assert.True(t, found, "Expected key2 to be found in new table")
	assert.Equal(t, []byte("value2"), data, "Expected value2 for key2 in new table")

	// 10. Verify that previous data ("key1") is not present
	mockDB.On("Get", mock.Anything, "T", "key1").Return([]byte(nil), false, nil).Once()
	data, found, err = pc.Get(context.Background(), "T", "key1")
	assert.NoError(t, err, "Expected no error when getting from new table")
	assert.False(t, found, "Expected not found for key1 in new table")
	assert.Nil(t, data, "Expected nil data for key1 in new table")

	// 11. Test setting new data
	mockDB.On("Set", mock.Anything, "T", "key3", []byte("value3")).Return(nil).Maybe()
	mockDB.On("BatchSet", mock.Anything, "T", mock.MatchedBy(func(items map[string][]byte) bool {
		return len(items) == 1 && string(items["key3"]) == "value3"
	})).Return(nil).Maybe()
	err = pc.Set(context.Background(), "T", "key3", []byte("value3"))
	assert.NoError(t, err, "Expected no error when setting key3")

	// Verify that all expected mockDB calls were made
	mockDB.AssertExpectations(t)
}
