package types

import (
	"fmt"
	"time"
)

// DBConfig contains database configuration settings
type DBConfig struct {
	// Database type (e.g., "sqlite", "postgres", etc.)
	// Currently only "sqlite" is supported
	DBType string

	// Path to database file (for SQLite)
	DBPath string

	// Interval between writing accumulated data from pool to database
	WritePoolInterval time.Duration

	// Maximum size of write pool
	MaxPoolSize int

	// Additional database-specific options
	// Can be used for future DBMS support
	Options map[string]interface{}
}

// DefaultDBConfig returns default configuration
func DefaultDBConfig() *DBConfig {
	return &DBConfig{
		DBType:            "sqlite",
		DBPath:            "./data.db",
		WritePoolInterval: 5 * time.Second,
		MaxPoolSize:       1000,
		Options:           make(map[string]interface{}),
	}
}

var config *DBConfig

func SetDBConfig(cfg *DBConfig) {
	config = cfg
}

func GetDBConfig() *DBConfig {
	return config
}

// Validate checks if configuration is valid
func (cfg *DBConfig) Validate() error {
	if cfg.DBType != "sqlite" {
		return fmt.Errorf("unsupported database type: %s", cfg.DBType)
	}

	if cfg.WritePoolInterval < time.Second {
		return fmt.Errorf("write pool interval cannot be less than 1 second")
	}

	return nil
}
