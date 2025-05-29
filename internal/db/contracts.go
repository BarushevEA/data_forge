package db

import "context"

type ITableDB interface {
	// CreateTable creates a new table with the given name
	CreateTable(ctx context.Context, name string) error

	// DropTable removes the table and all its data
	DropTable(ctx context.Context, name string) error

	// Set stores a blob value with the given key
	Set(ctx context.Context, tableName, key string, value []byte) error

	// Get retrieves a blob value by its key
	// Returns the value and true if found, nil and false otherwise
	Get(ctx context.Context, tableName, key string) ([]byte, bool, error)

	// Delete removes an element by its key
	Delete(ctx context.Context, tableName, key string) error

	// BatchSet stores multiple blob values at once
	BatchSet(ctx context.Context, tableName string, items map[string][]byte) error

	// BatchGet retrieves multiple blob values by their keys
	// Returns a map of found key-value pairs
	BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error)

	// Close closes the database connection
	Close() error
}

// ISQLiteOptions defines interface for SQLite configuration
type ISQLiteOptions interface {
	// GetPath returns database file path
	GetPath() string
	// WithOptions merges current options with provided ones and returns new instance
	WithOptions(opts SQLiteOptions) ISQLiteOptions
	// ToDSN converts options to SQLite connection string
	ToDSN() string
	// ToPragmas converts options to SQLite PRAGMA statements
	ToPragmas() []string
	// GetConnectionOptions returns sql.DB connection settings
	GetConnectionOptions() ConnectionOptions

	Validate() error
}
