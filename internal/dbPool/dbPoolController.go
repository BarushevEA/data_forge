package dbPool

import (
	"context"
	"github.com/BarushevEA/data_forge/internal/dbTypes"
	lib "github.com/BarushevEA/data_forge/types"
	"github.com/BarushevEA/in_memory_cache/pkg"
	"github.com/BarushevEA/in_memory_cache/types"
	"time"
)

type PoolController struct {
	db dbTypes.ITableDB

	// Interval between writing accumulated data from pool to database
	writePoolInterval time.Duration

	// Maximum size of the writing pool
	maxPoolSize int

	writePool  types.ICacheInMemory[[]string]
	deletePool types.ICacheInMemory[[]string]
	tables     types.ICacheInMemory[lib.ITable[any]]
}

func NewPoolController(db dbTypes.ITableDB, writePoolInterval time.Duration, maxPoolSize int) dbTypes.ITableDB {
	stmtsOptions := dbTypes.GetLongDefaultShardedCacheOptions()

	return &PoolController{
		db:                db,
		writePoolInterval: writePoolInterval,
		maxPoolSize:       maxPoolSize,

		writePool: pkg.NewShardedCache[[]string](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		deletePool: pkg.NewShardedCache[[]string](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
		tables: pkg.NewShardedCache[lib.ITable[any]](
			stmtsOptions.Ctx,
			stmtsOptions.Ttl,
			stmtsOptions.TtlDecrement,
		),
	}
}

func (controller *PoolController) RegisterTable(tableName string, tableType lib.ITable[any]) error {
	return controller.tables.Set(tableName, tableType)
}

func (controller *PoolController) CreateTable(ctx context.Context, name string) error {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) DropTable(ctx context.Context, name string) error {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) Set(ctx context.Context, tableName, key string, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) Delete(ctx context.Context, tableName, key string) error {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (controller *PoolController) Close() error {
	//TODO implement me
	panic("implement me")
}
