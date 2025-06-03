package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockTableDB is a mock implementation of dbTypes.ITableDB for testing PoolController.
type mockTableDB struct {
	mock.Mock
}

// CreateTable mocks the CreateTable method of ITableDB.
func (m *mockTableDB) CreateTable(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *mockTableDB) RegisterTable(tableName string, table dbTypes.ITableRegister) error {
	return nil
}

// DropTable mocks the DropTable method of ITableDB.
func (m *mockTableDB) DropTable(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// Set mocks the Set method of ITableDB.
func (m *mockTableDB) Set(ctx context.Context, tableName, key string, value []byte) error {
	args := m.Called(ctx, tableName, key, value)
	return args.Error(0)
}

// Get mocks the Get method of ITableDB.
func (m *mockTableDB) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	args := m.Called(ctx, tableName, key)
	return args.Get(0).([]byte), args.Bool(1), args.Error(2)
}

// Delete mocks the Delete method of ITableDB.
func (m *mockTableDB) Delete(ctx context.Context, tableName, key string) error {
	args := m.Called(ctx, tableName, key)
	return args.Error(0)
}

// BatchSet mocks the BatchSet method of ITableDB.
func (m *mockTableDB) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	args := m.Called(ctx, tableName, items)
	return args.Error(0)
}

// BatchGet mocks the BatchGet method of ITableDB.
func (m *mockTableDB) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	args := m.Called(ctx, tableName, keys)
	return args.Get(0).(map[string][]byte), args.Error(1)
}

// Close mocks the Close method of ITableDB.
func (m *mockTableDB) Close() error {
	args := m.Called()
	return args.Error(0)
}

// TestPoolController_SetNoWritePool verifies the Set method when write pool flushing is disabled.
func TestPoolController_SetNoWritePool(t *testing.T) {
	// Initialize mock database.
	dbMock := &mockTableDB{}
	dbMock.On("CreateTable", mock.Anything, "test_table").Return(nil).Once()
	dbMock.On("Set", mock.Anything, "test_table", "key1", []byte("value1")).Return(nil).Once()

	// Initialize controller with writing pool disabled.
	controller := NewPoolController(dbMock, 100*time.Millisecond, 2, false, true).(*PoolController)
	ctx := context.Background()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Set a key directly to the database.
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)

	// Verify writePool is empty.
	_, exists := controller.writePool.Get("test_table")
	assert.False(t, exists)

	// Verify mock expectations.
	dbMock.AssertExpectations(t)
}

// TestPoolController_DeleteNoDeletePool verifies the Delete method when delete pool flushing is disabled.
func TestPoolController_DeleteNoDeletePool(t *testing.T) {
	// Initialize mock database.
	dbMock := &mockTableDB{}
	dbMock.On("CreateTable", mock.Anything, "test_table").Return(nil).Once()
	dbMock.On("Delete", mock.Anything, "test_table", "key1").Return(nil).Once()

	// Initialize the controller with delete pool disabled.
	controller := NewPoolController(dbMock, 100*time.Millisecond, 2, true, false).(*PoolController)
	ctx := context.Background()

	// Create a test table.
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Delete a key directly from the database.
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)

	// Verify deletePool is empty.
	_, exists := controller.deletePool.Get("test_table")
	assert.False(t, exists)

	// Verify mock expectations.
	dbMock.AssertExpectations(t)
}
