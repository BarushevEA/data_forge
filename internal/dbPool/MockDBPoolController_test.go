package dbPool

import (
	"context"
	"errors"
	"fmt"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"sync"
	"testing"
	"time"
)

// MockDB implements ITableDB interface for testing
type MockDB struct {
	mu         sync.RWMutex
	data       map[string]map[string][]byte
	createErr  error
	dropErr    error
	setErr     error
	getErr     error
	deleteErr  error
	batchErr   error
	closeCount int
}

func NewMockDB() *MockDB {
	return &MockDB{
		data: make(map[string]map[string][]byte),
	}
}

// Implementation of ITableDB interface for MockDB
func (m *MockDB) RegisterTable(_ string, _ dbTypes.ITableRegister) error { return nil }
func (m *MockDB) CreateTable(_ context.Context, name string) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[name] = make(map[string][]byte)
	return nil
}
func (m *MockDB) DropTable(_ context.Context, name string) error {
	if m.dropErr != nil {
		return m.dropErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, name)
	return nil
}
func (m *MockDB) Set(_ context.Context, tableName, key string, value []byte) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.data[tableName]; ok {
		t[key] = value
	}
	return nil
}
func (m *MockDB) Get(_ context.Context, tableName, key string) ([]byte, bool, error) {
	if m.getErr != nil {
		return nil, false, m.getErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.data[tableName]; ok {
		if v, exists := t[key]; exists {
			return v, true, nil
		}
	}
	return nil, false, nil
}
func (m *MockDB) Delete(_ context.Context, tableName, key string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.data[tableName]; ok {
		delete(t, key)
	}
	return nil
}
func (m *MockDB) BatchSet(_ context.Context, tableName string, items map[string][]byte) error {
	if m.batchErr != nil {
		return m.batchErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.data[tableName]; ok {
		for k, v := range items {
			t[k] = v
		}
	}
	return nil
}
func (m *MockDB) BatchGet(_ context.Context, tableName string, keys []string) (map[string][]byte, error) {
	if m.batchErr != nil {
		return nil, m.batchErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string][]byte)
	if t, ok := m.data[tableName]; ok {
		for _, k := range keys {
			if v, exists := t[k]; exists {
				result[k] = v
			}
		}
	}
	return result, nil
}
func (m *MockDB) Close() error {
	m.closeCount++
	return nil
}

// MockTable implements ITableRegister interface for testing
type MockTable struct {
	serializeErr error
	serializeMap map[string][]byte
}

func NewMockTable() *MockTable {
	return &MockTable{
		serializeMap: make(map[string][]byte),
	}
}

func (m *MockTable) Serialize(key string) ([]byte, error) {
	if m.serializeErr != nil {
		return nil, m.serializeErr
	}
	if v, ok := m.serializeMap[key]; ok {
		return v, nil
	}
	return []byte(key), nil
}

func TestPoolController_BasicOperations(t *testing.T) {
	// Initialize test environment
	mockDB := NewMockDB()
	controller := NewPoolController(mockDB, time.Millisecond*100, 2).(*PoolController)
	defer controller.Close()

	ctx := context.Background()
	tableName := "test_table"
	mockTable := NewMockTable()

	t.Run("create and register table", func(t *testing.T) {
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Errorf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, mockTable); err != nil {
			t.Errorf("RegisterTable failed: %v", err)
		}
	})

	t.Run("set and get operations", func(t *testing.T) {
		// Add value to pool (should not be in DB yet)
		err := controller.Set(ctx, tableName, "key1", []byte("value1"))
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		// Check value is not in DB yet
		_, exists, _ := mockDB.Get(ctx, tableName, "key1")
		if exists {
			t.Error("Value should not be in DB yet")
		}

		// Add second key to trigger flush due to maxPoolSize
		err = controller.Set(ctx, tableName, "key2", []byte("value2"))
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		// Wait for flush operation
		time.Sleep(time.Millisecond * 150)

		// Now values should be in DB
		_, exists, _ = mockDB.Get(ctx, tableName, "key1")
		if !exists {
			t.Error("Value should be in DB after flush")
		}
	})

	t.Run("delete operations", func(t *testing.T) {
		// Add and delete value
		err := controller.Set(ctx, tableName, "key_to_delete", []byte("value"))
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		err = controller.Delete(ctx, tableName, "key_to_delete")
		if err != nil {
			t.Errorf("Delete failed: %v", err)
		}

		// Wait for periodic flush
		time.Sleep(time.Millisecond * 150)

		// Check value is deleted
		_, exists, _ := mockDB.Get(ctx, tableName, "key_to_delete")
		if exists {
			t.Error("Value should be deleted from DB")
		}
	})
}

// More tests including error handling, concurrent operations, etc...
func TestPoolController_ErrorHandling(t *testing.T) {
	mockDB := NewMockDB()
	controller := NewPoolController(mockDB, time.Millisecond*100, 2).(*PoolController)
	defer controller.Close()

	ctx := context.Background()
	tableName := "test_table"
	mockTable := NewMockTable()

	t.Run("handle db errors", func(t *testing.T) {
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Errorf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, mockTable); err != nil {
			t.Errorf("RegisterTable failed: %v", err)
		}

		// Set DB error
		mockDB.setErr = errors.New("db error")

		// Set should not immediately return error
		err := controller.Set(ctx, tableName, "key1", []byte("value1"))
		if err != nil {
			t.Errorf("Set should not return error immediately: %v", err)
		}

		// Wait for flush operation
		time.Sleep(time.Millisecond * 150)
	})

	t.Run("handle serialization errors", func(t *testing.T) {
		mockTable.serializeErr = errors.New("serialization error")

		err := controller.Set(ctx, tableName, "key", []byte("value"))
		if err != nil {
			t.Errorf("Set should not return error immediately: %v", err)
		}

		// Wait for flush operation
		time.Sleep(time.Millisecond * 150)
	})
}

func TestPoolController_ConcurrentOperations(t *testing.T) {
	mockDB := NewMockDB()
	// Увеличим интервал обновления для более стабильной работы
	controller := NewPoolController(mockDB, time.Millisecond*50, 10).(*PoolController)
	defer controller.Close()

	ctx := context.Background()
	tableName := "test_table"
	mockTable := NewMockTable()

	if err := controller.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if err := controller.RegisterTable(tableName, mockTable); err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// Запускаем параллельные операции записи
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				err := controller.Set(ctx, tableName, key, []byte("value"))
				if err != nil {
					t.Errorf("concurrent Set failed: %v", err)
				}
				// Добавляем небольшую задержку между операциями
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	// Увеличиваем время ожидания для гарантированного завершения всех flush операций
	time.Sleep(time.Millisecond * 500)

	// Проверяем записи
	t.Run("verify all writes", func(t *testing.T) {
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", i, j)
				_, exists, err := controller.Get(ctx, tableName, key)
				if err != nil {
					t.Errorf("Get failed for key %s: %v", key, err)
					continue
				}
				if !exists {
					t.Errorf("Key %s should exist in DB", key)
				}
			}
		}
	})
}
