package db

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
