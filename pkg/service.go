package pkg

import (
	"github.com/BarushevEA/data_forge/internal/controller"
	"github.com/BarushevEA/data_forge/types"
)

func CreateTable[T any](options types.TableOption[T]) (types.ITable[T], error) {
	return controller.NewTableController(options)
}
