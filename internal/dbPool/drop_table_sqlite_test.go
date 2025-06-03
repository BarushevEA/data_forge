package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPoolController_DropTable_WithSQLite tests the DropTable functionality with SQLite.
func TestPoolController_DropTable_WithSQLite(t *testing.T) {
	// Create a temporary directory for SQLite database
	tempDir, err := os.MkdirTemp("", "test_sqlite_*")
	assert.NoError(t, err, "Failed to create temporary directory")
	defer os.RemoveAll(tempDir) // Clean up temporary directory after test

	// Define path for SQLite database file
	dbPath := filepath.Join(tempDir, "test.db")
	opts := db.NewSQLiteOptions(dbPath)
	sqliteDB, err := db.NewSQLiteDB(opts)
	assert.NoError(t, err, "Failed to create SQLite database")
	defer sqliteDB.Close() // Ensure database connection is closed after test

	// Initialize PoolController with specified parameters
	pc := NewPoolController(sqliteDB, time.Second, 10, true, true).(*PoolController)
	defer pc.Close() // Ensure PoolController is closed after test

	// 1. Create a new table
	err = pc.CreateTable(context.Background(), "T")
	assert.NoError(t, err, "Failed to create table")

	// 2. Set data in the table
	err = pc.Set(context.Background(), "T", "key1", []byte("value1"))
	assert.NoError(t, err, "Failed to set key1")
	// Verify that data is in the write pool
	keys, exists := pc.writePool.Get("T")
	assert.True(t, exists, "Write pool should contain table T")
	assert.Equal(t, []string{"key1"}, keys, "Write pool should contain key1")
	value, exists := pc.writePoolBoofer.Get("T:key1")
	assert.True(t, exists, "Write pool buffer should contain key1")
	assert.Equal(t, []byte("value1"), value, "Write pool buffer should contain value1")

	// 3. Drop the table
	err = pc.DropTable(context.Background(), "T")
	assert.NoError(t, err, "Failed to drop table")

	// 4. Verify that pools and caches are cleared
	_, exists = pc.writePool.Get("T")
	assert.False(t, exists, "Write pool should be cleared after DropTable")
	_, exists = pc.writePoolBoofer.Get("T:key1")
	assert.False(t, exists, "Write pool buffer should be cleared after DropTable")
	_, exists = pc.deletePool.Get("T")
	assert.False(t, exists, "Delete pool should be cleared after DropTable")
	_, exists = pc.tables.Get("T")
	assert.False(t, exists, "Tables cache should be cleared after DropTable")

	// 5. Recreate the table
	err = pc.CreateTable(context.Background(), "T")
	assert.NoError(t, err, "Failed to recreate table")

	// 6. Verify that pools are empty before setting new data
	_, exists = pc.writePool.Get("T")
	assert.False(t, exists, "Write pool should be empty before first Set")
	_, exists = pc.writePoolBoofer.Get("T:key2")
	assert.False(t, exists, "Write pool buffer should be empty before first Set")

	// 7. Set new data for the recreated table
	err = pc.Set(context.Background(), "T", "key2", []byte("value2"))
	assert.NoError(t, err, "Failed to set key2")
	keys, exists = pc.writePool.Get("T")
	assert.True(t, exists, "Write pool should contain table T after Set")
	assert.Equal(t, []string{"key2"}, keys, "Write pool should contain key2")
	value, exists = pc.writePoolBoofer.Get("T:key2")
	assert.True(t, exists, "Write pool buffer should contain key2")
	assert.Equal(t, []byte("value2"), value, "Write pool buffer should contain value2")

	// 8. Verify that data is written to the database after flushing
	time.Sleep(time.Second * 2) // Wait for flush to complete
	data, found, err := pc.Get(context.Background(), "T", "key2")
	assert.NoError(t, err, "Expected no error when getting key2 from new table")
	assert.True(t, found, "Expected key2 to be found in new table")
	assert.Equal(t, []byte("value2"), data, "Expected value2 for key2 in new table")

	// 9. Verify that previous data is no longer present
	data, found, err = pc.Get(context.Background(), "T", "key1")
	assert.NoError(t, err, "Expected no error when getting from new table")
	assert.False(t, found, "Expected not found for key1 in new table")
	assert.Nil(t, data, "Expected nil data for key1 in new table")

	// 10. Verify that new data can be set in the recreated table
	err = pc.Set(context.Background(), "T", "key3", []byte("value3"))
	assert.NoError(t, err, "Failed to set key3 in new table")
}
