package db

const (
	CREATE_TABLE = `
        CREATE TABLE IF NOT EXISTS %s (
            key TEXT PRIMARY KEY,
            value BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITHOUT ROWID
    `
	DROP_TABLE = `DROP TABLE IF EXISTS %s`
	SET_VALUE  = `
        INSERT INTO %s (key, value, updated_at) 
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET 
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
    `
	BATCH_SET_VALUE = `
        INSERT OR FAIL INTO %s (key, value, updated_at) 
        VALUES (?, ?, CURRENT_TIMESTAMP)
    `
	GET_VALUE      = `SELECT value FROM %s WHERE key = ?`
	DELETE_VALUE   = `DELETE FROM %s WHERE key = ?`
	BATCH_GET_BASE = `SELECT key, value FROM %s WHERE key IN `
)
