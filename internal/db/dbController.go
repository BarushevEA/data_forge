package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/BarushevEA/in_memory_cache/pkg"
	lib "github.com/BarushevEA/in_memory_cache/types"
	_ "modernc.org/sqlite"
	"strings"
	"time"
)

type SQLiteDB struct {
	db    *sql.DB
	stmts lib.ICacheInMemory[*sql.Stmt]
}

func NewSQLiteDB(opts ISQLiteOptions) (ITableDB, error) {
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %v", err)
	}

	if opts.GetPath() == "" {
		return nil, errors.New("database path is required")
	}

	db, err := sql.Open("sqlite", opts.ToDSN())
	if err != nil {
		return nil, err
	}

	connOpts := opts.GetConnectionOptions()
	db.SetMaxOpenConns(connOpts.MaxOpenConns)
	db.SetMaxIdleConns(connOpts.MaxIdleConns)
	db.SetConnMaxLifetime(connOpts.ConnMaxLifetime)

	for _, pragma := range opts.ToPragmas() {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set %s: %v", pragma, err)
		}
	}

	return &SQLiteDB{
		db: db,
		stmts: pkg.NewShardedCache[*sql.Stmt](
			context.Background(),
			1000000*time.Hour, // TTL
			10000*time.Hour,   // TTL decrement
		),
	}, nil
}

// prepareStmt prepares and caches the statement for the table
func (controller *SQLiteDB) prepareStmt(ctx context.Context, tableName, queryType string, query string) (*sql.Stmt, error) {
	key := tableName + ":" + queryType

	stmt, ok := controller.stmts.Get(key)
	if ok {
		return stmt, nil
	}

	stmt, err := controller.db.PrepareContext(ctx, fmt.Sprintf(query, tableName))
	if err != nil {
		return nil, err
	}

	if err := controller.stmts.Set(key, stmt); err != nil {
		_ = stmt.Close()
		return nil, err
	}

	return stmt, nil
}

func (controller *SQLiteDB) CreateTable(ctx context.Context, name string) error {
	stmt, err := controller.prepareStmt(ctx, name, "create", CREATE_TABLE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx)
	return err
}

func (controller *SQLiteDB) DropTable(ctx context.Context, name string) error {
	stmt, err := controller.prepareStmt(ctx, name, "drop", DROP_TABLE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx)
	return err
}

func (controller *SQLiteDB) Set(ctx context.Context, tableName, key string, value []byte) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}
	if value == nil {
		return errors.New("value cannot be nil")
	}

	stmt, err := controller.prepareStmt(ctx, tableName, "set", SET_VALUE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, key, value)
	return err
}

func (controller *SQLiteDB) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	stmt, err := controller.prepareStmt(ctx, tableName, "get", GET_VALUE)
	if err != nil {
		return nil, false, err
	}

	var value []byte
	err = stmt.QueryRowContext(ctx, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

func (controller *SQLiteDB) Close() error {
	// First, close all prepared statements
	err := controller.stmts.Range(func(key string, stmt *sql.Stmt) bool {
		err := stmt.Close()
		if err != nil {
			fmt.Println(err)
		}
		return true
	})
	if err != nil {
		return err
	}

	controller.stmts.Clear()

	// Then close the database connection
	return controller.db.Close()
}

func (controller *SQLiteDB) Delete(ctx context.Context, tableName, key string) error {
	stmt, err := controller.prepareStmt(ctx, tableName, "delete", DELETE_VALUE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, key)
	return err
}

func (controller *SQLiteDB) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	stmt, err := controller.prepareStmt(ctx, tableName, "batch_set", BATCH_SET_VALUE)
	if err != nil {
		return err
	}

	tx, err := controller.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Гарантируем откат при ошибке
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	txStmt := tx.StmtContext(ctx, stmt)
	defer txStmt.Close()

	for key, value := range items {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if value == nil {
			return errors.New("value cannot be nil")
		}

		if _, err = txStmt.ExecContext(ctx, key, value); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (controller *SQLiteDB) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
	// Create a parameterized query with the required number of placeholders
	params := make([]string, len(keys))
	for i := range keys {
		params[i] = "?"
	}

	query := fmt.Sprintf("%s(%s)",
		fmt.Sprintf(BATCH_GET_BASE, tableName),
		strings.Join(params, ","))

	// Convert []string to []interface{} for query arguments
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	// We don't cache this prepared statement as it depends on the number of keys
	stmt, err := controller.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(stmt)

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(rows)

	result := make(map[string][]byte)
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
