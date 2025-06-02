package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

// Мок для ITableRegister
type mockTableRegister struct {
	mock.Mock
}

func (m *mockTableRegister) Serialize(key string) ([]byte, error) {
	args := m.Called(key)
	return args.Get(0).([]byte), args.Error(1)
}

func setupTestController(t *testing.T) (*PoolController, context.Context, func()) {
	ctx := context.Background()

	// Создаём SQLiteDB с :memory:
	opts := db.NewSQLiteOptions(":memory:")
	sqliteDB, err := db.NewSQLiteDB(opts)
	if err != nil {
		t.Fatalf("Failed to create SQLiteDB: %v", err)
	}

	// Настраиваем PoolController
	controller := NewPoolController(sqliteDB, 100*time.Millisecond, 2, true, true)

	cleanup := func() {
		_ = controller.Close()
	}

	return controller.(*PoolController), ctx, cleanup
}

func TestPoolController_Set(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Создаём таблицу
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Регистрируем мок для ITableRegister
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Тест: Добавление ключа в writePool
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	keys, exists := controller.writePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key1"}, keys)

	// Тест: Flush при maxPoolSize
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Ожидание flush
	_, exists = controller.writePool.Get("test_table")
	assert.False(t, exists) // Пул очищен после flush
}

func TestPoolController_Delete(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Создаём таблицу
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Добавляем ключи в writePool
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)

	// Тест: Добавление в deletePool
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	deleteKeys, exists := controller.deletePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key1"}, deleteKeys)

	// Проверяем, что ключ удалён из writePool
	writeKeys, exists := controller.writePool.Get("test_table")
	assert.True(t, exists)
	assert.Equal(t, []string{"key2"}, writeKeys)
}

func TestPoolController_writePoolFlush(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Создаём таблицу
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Регистрируем мок для ITableRegister
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Тест: Пустой пул
	err = controller.writePoolFlush("test_table")
	assert.NoError(t, err)

	// Тест: Flush ключей
	controller.writePool.Set("test_table", []string{"key1", "key2"})
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.writePoolFlush("test_table")
	assert.NoError(t, err)
	_, exists := controller.writePool.Get("test_table")
	assert.False(t, exists)
}

func TestPoolController_Close(t *testing.T) {
	controller, _, cleanup := setupTestController(t)
	defer cleanup()

	//err := controller.Close()
	//assert.NoError(t, err)
	assert.Equal(t, 0, controller.writePool.Len())
	assert.Equal(t, 0, controller.deletePool.Len())
	assert.Equal(t, 0, controller.tables.Len())
}

func TestPoolController_Integration(t *testing.T) {
	controller, ctx, cleanup := setupTestController(t)
	defer cleanup()

	// Создаём таблицу
	err := controller.CreateTable(ctx, "test_table")
	assert.NoError(t, err)

	// Регистрируем мок для ITableRegister
	tableMock := &mockTableRegister{}
	err = controller.RegisterTable("test_table", tableMock)
	assert.NoError(t, err)

	// Тест Set с flush
	tableMock.On("Serialize", "key1").Return([]byte("value1"), nil).Once()
	tableMock.On("Serialize", "key2").Return([]byte("value2"), nil).Once()
	err = controller.Set(ctx, "test_table", "key1", []byte("value1"))
	assert.NoError(t, err)
	err = controller.Set(ctx, "test_table", "key2", []byte("value2"))
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Ожидание flush

	// Проверка данных
	data, exists, err := controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("value1"), data)

	// Тест Delete
	err = controller.Delete(ctx, "test_table", "key1")
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Ожидание flush
	_, exists, err = controller.Get(ctx, "test_table", "key1")
	assert.NoError(t, err)
	assert.False(t, exists)
}
