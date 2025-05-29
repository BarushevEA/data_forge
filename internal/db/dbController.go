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

func NewSQLiteDB(dbPath string) (ITableDB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
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
func (db *SQLiteDB) prepareStmt(ctx context.Context, tableName, queryType string, query string) (*sql.Stmt, error) {
	key := tableName + ":" + queryType

	stmt, ok := db.stmts.Get(key)
	if ok {
		return stmt, nil
	}

	stmt, err := db.db.PrepareContext(ctx, fmt.Sprintf(query, tableName))
	if err != nil {
		return nil, err
	}

	if err := db.stmts.Set(key, stmt); err != nil {
		_ = stmt.Close()
		return nil, err
	}

	return stmt, nil
}

func (db *SQLiteDB) CreateTable(ctx context.Context, name string) error {
	stmt, err := db.prepareStmt(ctx, name, "create", CREATE_TABLE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx)
	return err
}

func (db *SQLiteDB) DropTable(ctx context.Context, name string) error {
	stmt, err := db.prepareStmt(ctx, name, "drop", DROP_TABLE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx)
	return err
}

func (db *SQLiteDB) Set(ctx context.Context, tableName, key string, value []byte) error {
	stmt, err := db.prepareStmt(ctx, tableName, "set", SET_VALUE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, key, value)
	return err
}

func (db *SQLiteDB) Get(ctx context.Context, tableName, key string) ([]byte, bool, error) {
	stmt, err := db.prepareStmt(ctx, tableName, "get", GET_VALUE)
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

func (db *SQLiteDB) Close() error {
	// First, close all prepared statements
	err := db.stmts.Range(func(key string, stmt *sql.Stmt) bool {
		err := stmt.Close()
		if err != nil {
			fmt.Println(err)
		}
		return true
	})
	if err != nil {
		return err
	}

	// Then close the database connection
	return db.db.Close()
}

func (db *SQLiteDB) Delete(ctx context.Context, tableName, key string) error {
	stmt, err := db.prepareStmt(ctx, tableName, "delete", DELETE_VALUE)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, key)
	return err
}

func (db *SQLiteDB) BatchSet(ctx context.Context, tableName string, items map[string][]byte) error {
	stmt, err := db.prepareStmt(ctx, tableName, "set", SET_VALUE)
	if err != nil {
		return err
	}

	// Start transaction for atomic batch insert
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Prepare statement in transaction context
	txStmt := tx.StmtContext(ctx, stmt)

	for key, value := range items {
		if _, err := txStmt.ExecContext(ctx, key, value); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (db *SQLiteDB) BatchGet(ctx context.Context, tableName string, keys []string) (map[string][]byte, error) {
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
	stmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
