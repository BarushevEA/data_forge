package dbPool

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BarushevEA/data_forge/internal/db"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type TestTable struct {
	data sync.Map
}

type TestData struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

func NewTestTable() *TestTable {
	return &TestTable{}
}

func (t *TestTable) Serialize(key string) ([]byte, error) {
	value, _ := t.data.LoadOrStore(key, TestData{
		ID:    key,
		Value: fmt.Sprintf("value_%s", key),
	})
	return json.Marshal(value)
}

func TestPoolController_WithSQLite(t *testing.T) {
	// Create temp directory for test DB
	tempDir, err := os.MkdirTemp("", "test_sqlite_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	opts := db.NewSQLiteOptions(dbPath)
	sqliteDB, err := db.NewSQLiteDB(opts)
	if err != nil {
		t.Fatalf("Failed to create SQLite DB: %v", err)
	}

	// Create pool controller with small interval and pool size for testing
	controller := NewPoolController(sqliteDB, time.Millisecond*100, 5).(*PoolController)
	defer controller.Close()

	ctx := context.Background()
	tableName := "test_table"
	testTable := NewTestTable()

	t.Run("basic operations", func(t *testing.T) {
		// Create and register table
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, testTable); err != nil {
			t.Fatalf("RegisterTable failed: %v", err)
		}

		// Test Set operation
		err := controller.Set(ctx, tableName, "test1", nil) // value doesn't matter here
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Wait for flush
		time.Sleep(time.Millisecond * 150)

		// Test Get operation
		data, exists, err := controller.Get(ctx, tableName, "test1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !exists {
			t.Fatal("Data should exist in DB")
		}

		var retrievedData TestData
		if err := json.Unmarshal(data, &retrievedData); err != nil {
			t.Fatalf("Failed to unmarshal data: %v", err)
		}

		expectedValue := fmt.Sprintf("value_%s", "test1")
		if retrievedData.Value != expectedValue {
			t.Errorf("Wrong value: got %s, want %s", retrievedData.Value, expectedValue)
		}
	})

	t.Run("batch operations", func(t *testing.T) {
		// Prepare batch data
		batchSize := 10
		items := make(map[string][]byte)
		expectedData := make(map[string]TestData)

		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("batch_key_%d", i)
			testData := TestData{ID: key, Value: fmt.Sprintf("value_%s", key)}
			testTable.data.Store(key, testData) // Сохраняем в TestTable

			serialized, err := json.Marshal(testData)
			if err != nil {
				t.Fatalf("Failed to marshal test data: %v", err)
			}

			items[key] = serialized
			expectedData[key] = testData
		}

		// Test BatchSet
		if err := controller.BatchSet(ctx, tableName, items); err != nil {
			t.Fatalf("BatchSet failed: %v", err)
		}

		// Test BatchGet
		keys := make([]string, 0, len(items))
		for k := range items {
			keys = append(keys, k)
		}

		result, err := controller.BatchGet(ctx, tableName, keys)
		if err != nil {
			t.Fatalf("BatchGet failed: %v", err)
		}

		if len(result) != len(items) {
			t.Errorf("Wrong number of items: got %d, want %d", len(result), len(items))
		}

		// Verify each item
		for key, data := range result {
			var retrievedData TestData
			if err := json.Unmarshal(data, &retrievedData); err != nil {
				t.Errorf("Failed to unmarshal data for key %s: %v", key, err)
				continue
			}

			expected := expectedData[key]
			if retrievedData.ID != expected.ID || retrievedData.Value != expected.Value {
				t.Errorf("Wrong value for key %s: got %+v, want %+v", key, retrievedData, expected)
			}
		}
	})

	t.Run("concurrent operations", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 5
		operationsPerGoroutine := 20

		errorChan := make(chan error, numGoroutines*operationsPerGoroutine)

		// Start concurrent write operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					key := fmt.Sprintf("concurrent_key_%d_%d", id, j)
					if err := controller.Set(ctx, tableName, key, nil); err != nil {
						errorChan <- fmt.Errorf("Set failed for key %s: %v", key, err)
					}
					time.Sleep(time.Millisecond) // Small delay between operations
				}
			}(i)
		}

		wg.Wait()
		close(errorChan)

		// Check for errors during concurrent operations
		for err := range errorChan {
			t.Error(err)
		}

		// Wait for all flushes to complete
		time.Sleep(time.Millisecond * 500)

		// Verify all writes
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent_key_%d_%d", i, j)
				data, exists, err := controller.Get(ctx, tableName, key)
				if err != nil {
					t.Errorf("Get failed for key %s: %v", key, err)
					continue
				}
				if !exists {
					t.Errorf("Data should exist for key %s", key)
					continue
				}

				var retrievedData TestData
				if err := json.Unmarshal(data, &retrievedData); err != nil {
					t.Errorf("Failed to unmarshal data for key %s: %v", key, err)
					continue
				}

				expectedValue := fmt.Sprintf("value_%s", key)
				if retrievedData.Value != expectedValue {
					t.Errorf("Wrong value for key %s: got %s, want %s", key, retrievedData.Value, expectedValue)
				}
			}
		}
	})

	t.Run("delete operations", func(t *testing.T) {
		// Create test data
		key := "to_delete"
		err := controller.Set(ctx, tableName, key, nil)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Wait for initial write
		time.Sleep(time.Millisecond * 150)

		// Delete the data
		if err := controller.Delete(ctx, tableName, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Wait for delete operation
		time.Sleep(time.Millisecond * 150)

		// Verify deletion
		_, exists, err := controller.Get(ctx, tableName, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if exists {
			t.Error("Data should not exist after deletion")
		}
	})
}
