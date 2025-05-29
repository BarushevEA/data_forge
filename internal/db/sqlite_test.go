package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) (*SQLiteDB, context.Context, func()) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")

	opts := NewSQLiteOptions(dbPath)
	db, err := NewSQLiteDB(opts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	ctx := context.Background()

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(tmpDir)
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
	opts := NewSQLiteOptions(dbPath)
	db, err := NewSQLiteDB(opts)
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

func TestSQLiteOptions(t *testing.T) {
	tests := []struct {
		name     string
		basePath string
		opts     SQLiteOptions
		want     SQLiteOptions
	}{
		{
			name:     "default options",
			basePath: "test.db",
			opts:     SQLiteOptions{},
			want: SQLiteOptions{
				Path:              "test.db",
				MaxOpenConns:      10,
				MaxIdleConns:      5,
				ConnMaxLifetime:   time.Hour,
				PageSize:          4096,
				CacheSize:         -2000000,
				JournalMode:       "WAL",
				SyncMode:          "NORMAL",
				TempStore:         "MEMORY",
				BusyTimeout:       5000,
				MMapSize:          30000000000,
				WalAutocheckpoint: 1000,
			},
		},
		{
			name:     "custom options",
			basePath: "test.db",
			opts: SQLiteOptions{
				MaxOpenConns: 20,
				CacheSize:    -4000000,
				SyncMode:     "OFF",
			},
			want: SQLiteOptions{
				Path:              "test.db",
				MaxOpenConns:      20,
				MaxIdleConns:      5,
				ConnMaxLifetime:   time.Hour,
				PageSize:          4096,
				CacheSize:         -4000000,
				JournalMode:       "WAL",
				SyncMode:          "OFF",
				TempStore:         "MEMORY",
				BusyTimeout:       5000,
				MMapSize:          30000000000,
				WalAutocheckpoint: 1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := NewSQLiteOptions(tt.basePath)
			got := base.WithOptions(tt.opts)
			gotOpts := got.(*SQLiteOptions)

			if !reflect.DeepEqual(*gotOpts, tt.want) {
				t.Errorf("WithOptions() = %v, want %v", *gotOpts, tt.want)
			}
		})
	}
}

func TestSQLiteDB_WithCustomOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Тестируем создание БД с кастомными опциями
	opts := NewSQLiteOptions(dbPath).WithOptions(SQLiteOptions{
		MaxOpenConns: 20,
		CacheSize:    -4000000,
		SyncMode:     "OFF",
	})

	db, err := NewSQLiteDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Проверяем базовую функциональность с кастомными настройками
	ctx := context.Background()
	tableName := "test_table"

	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatal(err)
	}

	if err := db.Set(ctx, tableName, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}

	value, exists, err := db.Get(ctx, tableName, "key")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("Value should exist")
	}
	if string(value) != "value" {
		t.Errorf("Got %s, want value", string(value))
	}
}

func TestSQLiteOptions_Validation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*SQLiteOptions)
		wantErr string
	}{
		{
			name: "empty path",
			modify: func(o *SQLiteOptions) {
				o.Path = ""
			},
			wantErr: "database path is required",
		},
		{
			name: "negative max connections",
			modify: func(o *SQLiteOptions) {
				o.Path = "test.db"
				o.MaxOpenConns = -1
			},
			wantErr: "max open connections cannot be negative",
		},
		{
			name: "invalid journal mode",
			modify: func(o *SQLiteOptions) {
				o.Path = "test.db"
				o.JournalMode = "INVALID"
			},
			wantErr: "invalid journal mode",
		},
		{
			name: "invalid sync mode",
			modify: func(o *SQLiteOptions) {
				o.Path = "test.db"
				o.SyncMode = "INVALID"
			},
			wantErr: "invalid sync mode",
		},
		{
			name: "negative busy timeout",
			modify: func(o *SQLiteOptions) {
				o.Path = "test.db"
				o.BusyTimeout = -1
			},
			wantErr: "busy timeout cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewSQLiteOptions("test.db")
			tt.modify(opts.(*SQLiteOptions))

			err := opts.Validate()
			if err == nil && tt.wantErr != "" {
				t.Errorf("Validate() expected error = %v, got nil", tt.wantErr)
			}
			if err != nil && err.Error() != tt.wantErr {
				t.Errorf("Validate() error message = %v, want %v", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSQLiteOptions_Validation_ExtendedCases(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*SQLiteOptions)
		wantErr string
	}{
		{
			name: "too large page size",
			modify: func(o *SQLiteOptions) {
				o.PageSize = 65537
			},
			wantErr: "page size must be a power of 2 between 512 and 65536",
		},
		{
			name: "invalid temp store value",
			modify: func(o *SQLiteOptions) {
				o.TempStore = "INVALID"
			},
			wantErr: "invalid temp store value",
		},
		{
			name: "too small cache size",
			modify: func(o *SQLiteOptions) {
				o.CacheSize = -2147483649
			},
			wantErr: "cache size too small",
		},
		{
			name: "too large wal checkpoint",
			modify: func(o *SQLiteOptions) {
				o.WalAutocheckpoint = 1000001
			},
			wantErr: "wal autocheckpoint must be between 0 and 1000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewSQLiteOptions("test.db")
			tt.modify(opts.(*SQLiteOptions))

			err := opts.Validate()
			if err == nil && tt.wantErr != "" {
				t.Errorf("Validate() expected error = %v, got nil", tt.wantErr)
			}
			if err != nil && err.Error() != tt.wantErr {
				t.Errorf("Validate() error message = %v, want %v", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSQLiteOptions_DSNFormat(t *testing.T) {
	tests := []struct {
		name     string
		opts     SQLiteOptions
		wantDSN  string
		wantPath string
	}{
		{
			name: "simple path",
			opts: SQLiteOptions{
				Path:        "test.db",
				BusyTimeout: 5000,
			},
			wantDSN:  "file:test.db?cache=shared&_journal_mode=WAL&_busy_timeout=5000&_mutex=full",
			wantPath: "test.db",
		},
		{
			name: "path with spaces",
			opts: SQLiteOptions{
				Path:        "test path/db.sqlite",
				BusyTimeout: 5000,
			},
			wantDSN:  "file:test path/db.sqlite?cache=shared&_journal_mode=WAL&_busy_timeout=5000&_mutex=full",
			wantPath: "test path/db.sqlite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewSQLiteOptions(tt.opts.Path)
			if got := opts.ToDSN(); got != tt.wantDSN {
				t.Errorf("ToDSN() = %v, want %v", got, tt.wantDSN)
			}
			if got := opts.GetPath(); got != tt.wantPath {
				t.Errorf("GetPath() = %v, want %v", got, tt.wantPath)
			}
		})
	}
}

func TestSQLiteOptions_PragmaFormat(t *testing.T) {
	opts := SQLiteOptions{
		Path:              "test.db",
		PageSize:          4096,
		CacheSize:         -2000000,
		JournalMode:       "WAL",
		SyncMode:          "NORMAL",
		TempStore:         "MEMORY",
		BusyTimeout:       5000,
		MMapSize:          30000000000,
		WalAutocheckpoint: 1000,
	}

	expected := []string{
		"PRAGMA page_size = 4096",
		"PRAGMA cache_size = -2000000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA mmap_size = 30000000000",
		"PRAGMA wal_autocheckpoint = 1000",
	}

	pragmas := opts.ToPragmas()

	if len(pragmas) != len(expected) {
		t.Errorf("ToPragmas() returned %d items, want %d", len(pragmas), len(expected))
	}

	for i, want := range expected {
		if i >= len(pragmas) {
			t.Errorf("Missing pragma: %s", want)
			continue
		}
		if pragmas[i] != want {
			t.Errorf("Pragma[%d] = %v, want %v", i, pragmas[i], want)
		}
	}
}

func TestSQLiteOptions_ConnectionOptions(t *testing.T) {
	tests := []struct {
		name string
		opts SQLiteOptions
		want ConnectionOptions
	}{
		{
			name: "default values",
			opts: SQLiteOptions{
				Path:            "test.db",
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: time.Hour,
			},
			want: ConnectionOptions{
				MaxOpenConns:    10,
				MaxIdleConns:    5,
				ConnMaxLifetime: time.Hour,
			},
		},
		{
			name: "custom values",
			opts: SQLiteOptions{
				Path:            "test.db",
				MaxOpenConns:    20,
				MaxIdleConns:    10,
				ConnMaxLifetime: 2 * time.Hour,
			},
			want: ConnectionOptions{
				MaxOpenConns:    20,
				MaxIdleConns:    10,
				ConnMaxLifetime: 2 * time.Hour,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opts.GetConnectionOptions()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConnectionOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSQLiteOptions_WithOptions_Immutability(t *testing.T) {
	original := NewSQLiteOptions("test.db")
	originalOpts := original.(*SQLiteOptions)

	// Test successful modification
	validModification := func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Valid modification panicked: %v", r)
			}
		}()

		modified := original.WithOptions(SQLiteOptions{
			MaxOpenConns: 20,
			CacheSize:    -4000000,
		})

		// Check original wasn't modified
		if originalOpts.MaxOpenConns != 10 {
			t.Errorf("Original MaxOpenConns changed: got %v, want 10", originalOpts.MaxOpenConns)
		}

		// Check new values
		modifiedOpts := modified.(*SQLiteOptions)
		if modifiedOpts.MaxOpenConns != 20 {
			t.Errorf("Modified MaxOpenConns wrong: got %v, want 20", modifiedOpts.MaxOpenConns)
		}
	}

	// Test invalid modification
	invalidModification := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Invalid modification should panic")
			}
		}()

		_ = original.WithOptions(SQLiteOptions{
			MaxOpenConns: -1, // Invalid value
		})
	}

	validModification()
	invalidModification()
}

func TestSQLiteDB_InvalidPath(t *testing.T) {
	// Test non-existent directory
	invalidPath := "/nonexistent/dir/test.db"
	opts := NewSQLiteOptions(invalidPath)
	db, err := NewSQLiteDB(opts)
	if err == nil {
		_ = db.Close()
		t.Error("Expected error for non-existent directory")
	}

	// Test empty path
	opts = NewSQLiteOptions("")
	db, err = NewSQLiteDB(opts)
	if err == nil {
		_ = db.Close()
		t.Error("Expected error for empty path")
	}

	// Test path with invalid characters
	invalidPath = "|*?"
	opts = NewSQLiteOptions(invalidPath)
	db, err = NewSQLiteDB(opts)
	if err == nil {
		_ = db.Close()
		t.Error("Expected error for path with invalid characters")
	}
}

func TestSQLiteDB_MaxConnections(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	maxConns := 2

	opts := NewSQLiteOptions(dbPath).WithOptions(SQLiteOptions{
		MaxOpenConns: maxConns,
		MaxIdleConns: 1,
	})

	db, err := NewSQLiteDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create table for testing
	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatal(err)
	}

	// Channel to track active connections
	activeConns := make(chan struct{}, maxConns)
	var wg sync.WaitGroup

	// Try to create maxConns + 1 connections
	for i := 0; i < maxConns+1; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			// Try to acquire connection
			tx, err := db.(*SQLiteDB).db.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSerializable,
			})
			if err != nil {
				if connID >= maxConns {
					// Expected error for connections beyond maxConns
					return
				}
				t.Errorf("Unexpected error on connection %d: %v", connID, err)
				return
			}
			defer tx.Rollback()

			// Successfully acquired connection
			select {
			case activeConns <- struct{}{}:
				// Wait a bit to ensure connection is held
				time.Sleep(100 * time.Millisecond)
				<-activeConns
			default:
				t.Errorf("Connection %d succeeded but exceeded max connections", connID)
			}
		}(i)
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestSQLiteDB_ConnectionLifetime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	shortLifetime := 100 * time.Millisecond

	opts := NewSQLiteOptions(dbPath).WithOptions(SQLiteOptions{
		MaxIdleConns:    2,
		ConnMaxLifetime: shortLifetime,
	})

	db, err := NewSQLiteDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	tableName := "test_table"
	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := db.Set(ctx, tableName, fmt.Sprintf("key_%d", i), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(shortLifetime * 2)

	if err := db.Set(ctx, tableName, "new_key", []byte("value")); err != nil {
		t.Errorf("Failed to execute query after connection lifetime: %v", err)
	}
}

func TestSQLiteDB_ManyTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	opts := NewSQLiteOptions(dbPath)
	db, err := NewSQLiteDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	tableCount := 100

	for i := 0; i < tableCount; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		if err := db.CreateTable(ctx, tableName); err != nil {
			t.Fatalf("Failed to create table %s: %v", tableName, err)
		}

		if err := db.Set(ctx, tableName, "key", []byte("value")); err != nil {
			t.Fatalf("Failed to insert into table %s: %v", tableName, err)
		}
	}

	for i := 0; i < tableCount; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		value, exists, err := db.Get(ctx, tableName, "key")
		if err != nil {
			t.Errorf("Failed to get from table %s: %v", tableName, err)
		}
		if !exists {
			t.Errorf("Value not found in table %s", tableName)
		}
		if string(value) != "value" {
			t.Errorf("Wrong value in table %s: got %s, want value", tableName, string(value))
		}
	}
}

func TestSQLiteDB_DiskSpaceHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	opts := NewSQLiteOptions(dbPath)
	db, err := NewSQLiteDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	tableName := "test_table"

	if err := db.CreateTable(ctx, tableName); err != nil {
		t.Fatal(err)
	}

	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	maxAttempts := 100
	var lastError error

	for i := 0; i < maxAttempts; i++ {
		err := db.Set(ctx, tableName, fmt.Sprintf("key_%d", i), largeValue)
		if err != nil {
			lastError = err
			break
		}
	}

	if lastError != nil {
		value, exists, err := db.Get(ctx, tableName, "key_0")
		if err != nil {
			t.Errorf("Failed to read data after disk space error: %v", err)
		}
		if !exists {
			t.Error("Previously written data should still exist")
		}
		if exists && !bytes.Equal(value, largeValue) {
			t.Error("Data corruption detected")
		}
	}
}
