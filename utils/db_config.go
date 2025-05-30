package utils

import (
	"fmt"
	"github.com/BarushevEA/data_forge/types"
	"time"
)

var config *types.DBConfig

// DefaultDBConfig returns default configuration
func DefaultDBConfig() *types.DBConfig {
	return &types.DBConfig{
		DBType:            "sqlite",
		DBPath:            "./data.db",
		WritePoolInterval: 5 * time.Second,
		MaxPoolSize:       1000,
		Options:           make(map[string]interface{}),
	}
}

func SetDBConfig(cfg *types.DBConfig) {
	config = cfg
}

func GetDBConfig() *types.DBConfig {
	return config
}

// ValidateDBConfig checks if the configuration is valid
func ValidateDBConfig(cfg *types.DBConfig) error {
	if cfg.DBType != "sqlite" {
		return fmt.Errorf("unsupported database type: %s", cfg.DBType)
	}

	if cfg.WritePoolInterval < time.Second {
		return fmt.Errorf("write pool interval cannot be less than 1 second")
	}

	return nil
}
