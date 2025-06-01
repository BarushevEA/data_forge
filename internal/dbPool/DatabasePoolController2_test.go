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
	// Создаем временную директорию для тестовой БД
	tempDir, err := os.MkdirTemp("", "test_sqlite_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	testTable := NewTestTable()
	ctx := context.Background()

	// Вспомогательная функция для создания нового контроллера
	createController := func(poolInterval time.Duration, poolSize int) (*PoolController, error) {
		opts := db.NewSQLiteOptions(dbPath)
		sqliteDB, err := db.NewSQLiteDB(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite DB: %v", err)
		}
		return NewPoolController(sqliteDB, poolInterval, poolSize).(*PoolController), nil
	}

	t.Run("write pool size limit", func(t *testing.T) {
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

		// Записываем данные до достижения лимита пула
		for i := 0; i < 2; i++ {
			key := fmt.Sprintf("key_%d", i)
			if err := controller.Set(ctx, tableName, key, nil); err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			// Проверяем размер пула
			poolKeys, ok := controller.writePool.Get(tableName)
			if !ok || len(poolKeys) != i+1 {
				t.Errorf("Write pool size mismatch: got %d, want %d", len(poolKeys), i+1)
			}
		}

		// Добавляем последний ключ, который должен вызвать запись в БД
		if err := controller.Set(ctx, tableName, "trigger_key", nil); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Проверяем, что пул очистился
		poolKeys, ok := controller.writePool.Get(tableName)
		if ok && len(poolKeys) > 0 {
			t.Errorf("Write pool should be empty after flush, got size: %d", len(poolKeys))
		}
	})

	t.Run("delete pool interaction with write pool", func(t *testing.T) {
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

		// Записываем данные в write pool
		key := "interaction_test"
		if err := controller.Set(ctx, tableName, key, nil); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Проверяем наличие ключа в write pool
		writeKeys, ok := controller.writePool.Get(tableName)
		if !ok || len(writeKeys) == 0 {
			t.Fatal("Key should be in write pool")
		}

		// Удаляем ключ
		if err := controller.Delete(ctx, tableName, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Проверяем, что ключ появился в delete pool
		deleteKeys, ok := controller.deletePool.Get(tableName)
		if !ok || len(deleteKeys) == 0 {
			t.Fatal("Key should be in delete pool")
		}

		// Ждем автоматической синхронизации
		time.Sleep(time.Millisecond * 150)

		// Проверяем, что ключ не остался в write pool
		writeKeys, ok = controller.writePool.Get(tableName)
		if ok && len(writeKeys) > 0 {
			for _, k := range writeKeys {
				if k == key {
					t.Error("Deleted key should not be in write pool")
				}
			}
		}
	})

	t.Run("pool periodic flush", func(t *testing.T) {
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

		// Добавляем данные в оба пула
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("periodic_key_%d", i)
			if err := controller.Set(ctx, tableName, key, nil); err != nil {
				t.Fatalf("Set failed: %v", err)
			}
		}

		deleteKey := "periodic_key_0"
		if err := controller.Delete(ctx, tableName, deleteKey); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Проверяем начальное состояние пулов
		writeKeys, _ := controller.writePool.Get(tableName)
		deleteKeys, _ := controller.deletePool.Get(tableName)

		if len(writeKeys) == 0 {
			t.Error("Write pool should not be empty initially")
		}
		if len(deleteKeys) == 0 {
			t.Error("Delete pool should not be empty initially")
		}

		// Ждем автоматического обновления
		time.Sleep(time.Millisecond * 150)

		// Проверяем, что пулы очистились
		writeKeys, writeOk := controller.writePool.Get(tableName)
		deleteKeys, deleteOk := controller.deletePool.Get(tableName)

		if writeOk && len(writeKeys) > 0 {
			t.Error("Write pool should be empty after periodic flush")
		}
		if deleteOk && len(deleteKeys) > 0 {
			t.Error("Delete pool should be empty after periodic flush")
		}

		// Проверяем состояние в БД
		_, exists, err := controller.Get(ctx, tableName, deleteKey)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if exists {
			t.Error("Deleted key should not exist in DB")
		}

		// Проверяем оставшиеся ключи
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
