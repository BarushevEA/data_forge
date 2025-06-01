package utils

import (
	"fmt"
	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/BarushevEA/data_forge/internal/dbPool"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	"github.com/BarushevEA/data_forge/types"
	"sync"
)

var dataBase dbTypes.ITableDB
var dbConfig *types.DBConfig
var initOnce sync.Once

func GetDB() (dbTypes.ITableDB, error) {
	var initError error

	initOnce.Do(func() {
		if dataBase != nil {
			return
		}

		if dbConfig == nil {
			dbConfig = GetDBConfig()
			if dbConfig == nil {
				dbConfig = DefaultDBConfig()
			}
		}

		if err := ValidateDBConfig(dbConfig); err != nil {
			initError = err
			return
		}

		switch dbConfig.DBType {
		case "sqlite":
			var err error

			sqLiteDB, err := db.NewSQLiteDB(getSQLiteOptions())
			if err != nil {
				initError = err
			}

			dataBase = dbPool.NewPoolController(
				sqLiteDB,
				dbConfig.WritePoolInterval,
				dbConfig.MaxPoolSize,
				dbConfig.IsWritePoolFlushing,
				dbConfig.IsDeletePoolFlushing)
		default:
			initError = fmt.Errorf("unsupported database type: %s", dbConfig.DBType)
		}
	})

	if initError != nil {
		return nil, initError
	}

	return dataBase, nil
}

func getSQLiteOptions() db.ISQLiteOptions {
	options := db.NewSQLiteOptions(dbConfig.DBPath)

	if dbConfig.Options == nil {
		return options
	}

	var customOptions types.SQLiteOptions

	saved, exists := dbConfig.Options["sqlite"]
	if exists {
		customOptions, exists = saved.(types.SQLiteOptions)
	}

	if exists {
		options.WithOptions(db.SQLiteOptions(customOptions))
	}

	return options
}
