package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func setupTestDB(t *testing.T) (*SQLiteDB, context.Context, func()) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewSQLiteDB(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	ctx := context.Background()

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db.(*SQLiteDB), ctx, cleanup
}

func TestSQLiteDB_CreateAndDropTable(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"

	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := db.DropTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestSQLiteDB_SetAndGet(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testCases := []struct {
		name  string
		key   string
		value []byte
	}{
		{"simple", "key1", []byte("value1")},
		{"empty value", "key2", []byte("")},
		{"binary data", "key3", []byte{0x00, 0x01, 0x02, 0x03}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Тест Set
			if err := db.Set(ctx, tableName, tc.key, tc.value); err != nil {
				t.Fatalf("Failed to set value: %v", err)
			}

			// Тест Get
			got, exists, err := db.Get(ctx, tableName, tc.key)
			if err != nil {
				t.Fatalf("Failed to get value: %v", err)
			}
			if !exists {
				t.Fatal("Value should exist")
			}
			if string(got) != string(tc.value) {
				t.Errorf("Got %v, want %v", got, tc.value)
			}
		})
	}
}

func TestSQLiteDB_Delete(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	key := "test_key"
	value := []byte("test_value")

	if err := db.Set(ctx, tableName, key, value); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	if err := db.Delete(ctx, tableName, key); err != nil {
		t.Fatalf("Failed to delete value: %v", err)
	}

	_, exists, err := db.Get(ctx, tableName, key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if exists {
		t.Error("Value should not exist after deletion")
	}
}

func TestSQLiteDB_BatchSetAndGet(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	items := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	if err := db.BatchSet(ctx, tableName, items); err != nil {
		t.Fatalf("Failed to batch set: %v", err)
	}

	keys := []string{"key1", "key2", "key3", "non_existent_key"}
	results, err := db.BatchGet(ctx, tableName, keys)
	if err != nil {
		t.Fatalf("Failed to batch get: %v", err)
	}

	for key, expectedValue := range items {
		if gotValue, ok := results[key]; !ok {
			t.Errorf("Missing key %s in results", key)
		} else if string(gotValue) != string(expectedValue) {
			t.Errorf("For key %s, got %v, want %v", key, gotValue, expectedValue)
		}
	}

	if _, ok := results["non_existent_key"]; ok {
		t.Error("Non-existent key should not be in results")
	}
}

func TestSQLiteDB_PreparedStatements(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 5; i++ {
		key := "key"
		value := []byte("value")

		if err := db.Set(ctx, tableName, key, value); err != nil {
			t.Fatalf("Failed to set value on iteration %d: %v", i, err)
		}

		if _, _, err := db.Get(ctx, tableName, key); err != nil {
			t.Fatalf("Failed to get value on iteration %d: %v", i, err)
		}
	}
}

func TestSQLiteDB_ConcurrentAccess(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const goroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := []byte(fmt.Sprintf("value_%d_%d", id, j))

				if err := db.Set(ctx, tableName, key, value); err != nil {
					errCh <- fmt.Errorf("Set error: %v", err)
					return
				}

				got, exists, err := db.Get(ctx, tableName, key)
				if err != nil {
					errCh <- fmt.Errorf("Get error: %v", err)
					return
				}
				if !exists {
					errCh <- fmt.Errorf("Value not found for key: %s", key)
					return
				}
				if string(got) != string(value) {
					errCh <- fmt.Errorf("Wrong value for key %s: got %s, want %s", key, got, value)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

func TestSQLiteDB_ErrorHandling(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		operation func() error
		wantErr   bool
	}{
		{
			name: "invalid table name",
			operation: func() error {
				return db.CreateTable(ctx, "invalid.table")
			},
			wantErr: true,
		},
		{
			name: "empty table name",
			operation: func() error {
				return db.CreateTable(ctx, "")
			},
			wantErr: true,
		},
		{
			name: "get from non-existent table",
			operation: func() error {
				_, _, err := db.Get(ctx, "non_existent_table", "key")
				return err
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if (err != nil) != tt.wantErr {
				t.Errorf("wantErr=%v, got error: %v", tt.wantErr, err)
			}
		})
	}
}

func TestSQLiteDB_ContextCancellation(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	cancel()

	if err := db.Set(ctx, tableName, "key", []byte("value")); err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}

	if _, _, err := db.Get(ctx, tableName, "key"); err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}
}

func TestSQLiteDB_ResourceCleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatal(err)
	}

	if err := db.Set(ctx, tableName, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Failed to close database: %v", err)
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("Database file should exist after close: %v", err)
	}

	if err := db.Close(); err == nil {
		t.Error("Second close should return error")
	}
}

func TestSQLiteDB_BatchSetTransactionRollback(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Первая успешная запись
	if err := db.Set(ctx, tableName, "key1", []byte("value1")); err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Попытка batch операции с дублирующимся ключом
	items := map[string][]byte{
		"key2": []byte("value2"),
		"key1": []byte("new_value1"), // Этот ключ уже существует
		"key3": []byte("value3"),
	}

	err := db.BatchSet(ctx, tableName, items)
	if err == nil {
		t.Fatal("Expected error on duplicate key, got nil")
	}

	// Проверяем, что первоначальное значение не изменилось
	value, exists, err := db.Get(ctx, tableName, "key1")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !exists {
		t.Fatal("Value should exist")
	}
	if string(value) != "value1" {
		t.Errorf("Original value changed after failed batch operation: got %s, want value1", string(value))
	}

	// Проверяем, что другие значения не были добавлены
	_, exists, err = db.Get(ctx, tableName, "key2")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if exists {
		t.Error("key2 should not exist after transaction rollback")
	}
}

func TestSQLiteDB_InputBoundaries(t *testing.T) {
	db, ctx, cleanup := setupTestDB(t)
	defer cleanup()

	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	sizes := []int{
		1024,        // 1KB
		64 * 1024,   // 64KB
		256 * 1024,  // 256KB
		512 * 1024,  // 512KB
		1024 * 1024, // 1MB
	}

	tests := []struct {
		name    string
		key     string
		value   []byte
		wantErr bool
	}{
		{
			name:    "very long key",
			key:     strings.Repeat("x", 1000),
			value:   []byte("value"),
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     "",
			value:   []byte("value"),
			wantErr: true,
		},
		{
			name:    "nil value",
			key:     "key",
			value:   nil,
			wantErr: true,
		},
	}

	// Добавляем тесты для разных размеров
	for _, size := range sizes {
		tests = append(tests, struct {
			name    string
			key     string
			value   []byte
			wantErr bool
		}{
			name:    fmt.Sprintf("value_%dKB", size/1024),
			key:     fmt.Sprintf("key_%d", size),
			value:   bytes.Repeat([]byte("x"), size),
			wantErr: false,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Set(ctx, tableName, tt.key, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				got, exists, err := db.Get(ctx, tableName, tt.key)
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if !exists {
					t.Error("Get() value should exist")
				}
				if !bytes.Equal(got, tt.value) {
					t.Errorf("Get() got length = %d, want length = %d", len(got), len(tt.value))
				}
			}
		})
	}
}
