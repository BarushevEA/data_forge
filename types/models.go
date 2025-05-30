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

	// Maximum size of write pool
	MaxPoolSize int

	// Additional database-specific options
	// Can be used for future DBMS support
	Options map[string]interface{}
}
