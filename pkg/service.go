package pkg

import (
	"github.com/BarushevEA/data_forge/internal/controller"
	"github.com/BarushevEA/data_forge/types"
	"github.com/BarushevEA/data_forge/utils"
)

func CreateTable[T any](options types.TableOption[T]) (types.ITable[T], error) {
	dBase, err := utils.GetDB()
	if err != nil {
		return nil, err
	}

	return controller.NewTableController(options, dBase)
}
