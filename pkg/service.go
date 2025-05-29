package pkg

import (
	"github.com/BarushevEA/data_forge/internal/controller"
	"github.com/BarushevEA/data_forge/internal/db"
	"github.com/BarushevEA/data_forge/types"
)

var dataBase db.ITableDB
var dbConfig *types.DBConfig

func GetDB() (db.ITableDB, error) {
	var err error

	if dbConfig == nil {
		dbConfig = types.GetDBConfig()

		if dbConfig == nil {
			dbConfig = types.DefaultDBConfig()
		}
	}

	if err = dbConfig.Validate(); err != nil {
		return nil, err
	}

	switch dbConfig.DBType {
	case "sqlite":
		dataBase, err = db.NewSQLiteDB(db.NewSQLiteOptions(dbConfig.DBPath))
		break
	}

	return dataBase, err
}

func CreateTable[T any](options types.TableOption[T]) (types.ITable[T], error) {
	dBase, err := GetDB()
	if err != nil {
		return nil, err
	}

	return controller.NewTableController(options, dBase)
}
