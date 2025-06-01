package types

import (
	"context"
	"time"
)

type TableOption[T any] struct {
	TableName    string
	TableType    T
	Context      context.Context
	TTL          time.Duration
	TTLDecrement time.Duration
}

// DBConfig contains database configuration settings
type DBConfig struct {
	// Database type (e.g., "sqlite", "postgres", etc.)
	//  Currently, only "sqlite" is supported
	DBType string

	// Path to a database file (for SQLite)
	DBPath string

	// Interval between writing accumulated data from pool to database
	WritePoolInterval time.Duration

	// Maximum size of the writing pool
	MaxPoolSize int

	IsWritePoolFlushing  bool
	IsDeletePoolFlushing bool

	// Additional database-specific options
	// Can be used for future DBMS support
	Options map[string]interface{}
}

type SQLiteOptions struct {
	// Path to the SQLite database file
	Path string

	// Maximum number of open connections to the database
	MaxOpenConns int

	// Maximum number of idle connections in the connection pool
	MaxIdleConns int

	// Maximum amount of time a connection may be reused
	ConnMaxLifetime time.Duration

	// Size of a database page in bytes (default is 4096)
	PageSize int

	// Number of pages to cache in memory
	CacheSize int

	// SQLite journal mode (e.g., DELETE, WAL, MEMORY, OFF)
	JournalMode string

	// SQLite synchronization mode (e.g., OFF, NORMAL, FULL)
	SyncMode string

	// Storage location for temporary files (DEFAULT, FILE, MEMORY)
	TempStore string

	// Time to wait for a locked resource before giving up, in milliseconds
	BusyTimeout int

	// Size of memory map in bytes (used when using memory-mapped I/O)
	MMapSize int64

	// Number of frames that accumulate in WAL before a checkpoint is automatically performed
	WalAutocheckpoint int
}
