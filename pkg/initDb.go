package pkg

import (
	"fmt"
	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/BarushevEA/data_forge/types"
	"sync"
)

var dataBase db.ITableDB
var dbConfig *types.DBConfig
var initOnce sync.Once

func getDB() (db.ITableDB, error) {
	var initError error

	initOnce.Do(func() {
		if dataBase != nil {
			return
		}

		if dbConfig == nil {
			dbConfig = types.GetDBConfig()
			if dbConfig == nil {
				dbConfig = types.DefaultDBConfig()
			}
		}

		if err := dbConfig.Validate(); err != nil {
			initError = err
			return
		}

		switch dbConfig.DBType {
		case "sqlite":
			var err error
			dataBase, err = db.NewSQLiteDB(db.NewSQLiteOptions(dbConfig.DBPath))
			if err != nil {
				initError = err
			}
		default:
			initError = fmt.Errorf("unsupported database type: %s", dbConfig.DBType)
		}
	})

	if initError != nil {
		return nil, initError
	}

	return dataBase, nil
}
