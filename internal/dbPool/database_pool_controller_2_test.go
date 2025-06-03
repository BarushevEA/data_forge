package dbPool

import (
	"context"
	"fmt"
	"github.com/BarushevEA/data_forge/internal/db"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPoolController_PoolBehavior(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_sqlite_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	testTable := NewTestTable()
	ctx := context.Background()

	createController := func(poolInterval time.Duration, poolSize int) (*PoolController, error) {
		opts := db.NewSQLiteOptions(dbPath)
		sqliteDB, err := db.NewSQLiteDB(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite DB: %v", err)
		}
		return NewPoolController(sqliteDB, poolInterval, poolSize, true, true).(*PoolController), nil
	}

	t.Run("write_pool_size_limit", func(t *testing.T) {
		controller, err := createController(time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer controller.Close()

		tableName := "test_pool_1"
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, testTable); err != nil {
			t.Fatalf("RegisterTable failed: %v", err)
		}

		for i := 0; i < 2; i++ {
			key := fmt.Sprintf("key_%d", i)
			if err := controller.Set(ctx, tableName, key, []byte("value")); err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			poolKeys, ok := controller.writePool.Get(tableName)
			if !ok || len(poolKeys) != i+1 {
				t.Errorf("Write pool size mismatch: got %d, want %d", len(poolKeys), i+1)
			}
		}

		if err := controller.Set(ctx, tableName, "trigger_key", []byte("value")); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		poolKeys, ok := controller.writePool.Get(tableName)
		if ok && len(poolKeys) > 0 {
			t.Errorf("Write pool should be empty after flush, got size: %d", len(poolKeys))
		}
	})

	t.Run("delete_pool_interaction_with_write_pool", func(t *testing.T) {
		controller, err := createController(time.Second, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer controller.Close()

		tableName := "test_pool_2"
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, testTable); err != nil {
			t.Fatalf("RegisterTable failed: %v", err)
		}

		key := "interaction_test"
		if err := controller.Set(ctx, tableName, key, []byte("value")); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		writeKeys, ok := controller.writePool.Get(tableName)
		if !ok || len(writeKeys) == 0 {
			t.Fatal("Key should be in write pool")
		}

		if err := controller.Delete(ctx, tableName, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		deleteKeys, ok := controller.deletePool.Get(tableName)
		if !ok || len(deleteKeys) == 0 {
			t.Fatal("Key should be in delete pool")
		}

		time.Sleep(time.Millisecond * 150)

		writeKeys, ok = controller.writePool.Get(tableName)
		if ok && len(writeKeys) > 0 {
			for _, k := range writeKeys {
				if k == key {
					t.Error("Deleted key should not be in write pool")
				}
			}
		}
	})

	t.Run("pool_periodic_flush", func(t *testing.T) {
		controller, err := createController(time.Millisecond*100, 10)
		if err != nil {
			t.Fatal(err)
		}
		defer controller.Close()

		tableName := "test_pool_3"
		if err := controller.CreateTable(ctx, tableName); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		if err := controller.RegisterTable(tableName, testTable); err != nil {
			t.Fatalf("RegisterTable failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("periodic_key_%d", i)
			if err := controller.Set(ctx, tableName, key, []byte("value")); err != nil {
				t.Fatalf("Set failed: %v", err)
			}
		}

		deleteKey := "periodic_key_0"
		if err := controller.Delete(ctx, tableName, deleteKey); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		writeKeys, _ := controller.writePool.Get(tableName)
		deleteKeys, _ := controller.deletePool.Get(tableName)

		if len(writeKeys) == 0 {
			t.Error("Write pool should not be empty initially")
		}
		if len(deleteKeys) == 0 {
			t.Error("Delete pool should not be empty initially")
		}

		time.Sleep(time.Millisecond * 150)

		writeKeys, writeOk := controller.writePool.Get(tableName)
		deleteKeys, deleteOk := controller.deletePool.Get(tableName)

		if writeOk && len(writeKeys) > 0 {
			t.Error("Write pool should be empty after periodic flush")
		}
		if deleteOk && len(deleteKeys) > 0 {
			t.Error("Delete pool should be empty after periodic flush")
		}

		_, exists, err := controller.Get(ctx, tableName, deleteKey)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if exists {
			t.Error("Deleted key should not exist in DB")
		}

		for i := 1; i < 3; i++ {
			key := fmt.Sprintf("periodic_key_%d", i)
			_, exists, err := controller.Get(ctx, tableName, key)
			if err != nil {
				t.Errorf("Get failed for key %s: %v", key, err)
			}
			if !exists {
				t.Errorf("Key %s should exist in DB", key)
			}
		}
	})
}
