package types

import (
	"context"
	"time"
)

type TableOption[T any] struct {
	TableName    string
	TableType    T
	Context      context.Context
	TTL          time.Duration
	TTLDecrement time.Duration
}
