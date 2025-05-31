package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SQLiteOptions implements ISQLiteOptions
type SQLiteOptions struct {
	Path              string
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	PageSize          int
	CacheSize         int
	JournalMode       string
	SyncMode          string
	TempStore         string
	BusyTimeout       int
	MMapSize          int64
	WalAutocheckpoint int
}

// NewSQLiteOptions creates options with default values
func NewSQLiteOptions(path string) ISQLiteOptions {
	return &SQLiteOptions{
		Path:              path,
		MaxOpenConns:      10,
		MaxIdleConns:      5,
		ConnMaxLifetime:   time.Hour,
		PageSize:          4096,
		CacheSize:         -2000000,
		JournalMode:       "WAL",
		SyncMode:          "NORMAL",
		TempStore:         "MEMORY",
		BusyTimeout:       5000,
		MMapSize:          30000000000,
		WalAutocheckpoint: 1000,
	}
}

// GetPath returns database file path
func (options *SQLiteOptions) GetPath() string {
	return options.Path
}

// ToDSN converts options to SQLite connection string
func (options *SQLiteOptions) ToDSN() string {
	return fmt.Sprintf("file:%s?cache=shared&_journal_mode=WAL&_busy_timeout=%d&_mutex=full",
		options.Path,
		options.BusyTimeout)
}

// ToPragmas converts options to SQLite PRAGMA statements
func (options *SQLiteOptions) ToPragmas() []string {
	return []string{
		fmt.Sprintf("PRAGMA page_size = %d", options.PageSize),
		fmt.Sprintf("PRAGMA cache_size = %d", options.CacheSize),
		fmt.Sprintf("PRAGMA journal_mode = %s", options.JournalMode),
		fmt.Sprintf("PRAGMA synchronous = %s", options.SyncMode),
		fmt.Sprintf("PRAGMA temp_store = %s", options.TempStore),
		fmt.Sprintf("PRAGMA busy_timeout = %d", options.BusyTimeout),
		fmt.Sprintf("PRAGMA mmap_size = %d", options.MMapSize),
		fmt.Sprintf("PRAGMA wal_autocheckpoint = %d", options.WalAutocheckpoint),
	}
}

// GetConnectionOptions returns sql.DB connection settings
func (options *SQLiteOptions) GetConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		MaxOpenConns:    options.MaxOpenConns,
		MaxIdleConns:    options.MaxIdleConns,
		ConnMaxLifetime: options.ConnMaxLifetime,
	}
}

// WithOptions implements ISQLiteOptions
func (options *SQLiteOptions) WithOptions(opts SQLiteOptions) ISQLiteOptions {
	newOpts := *options

	if opts.Path != "" {
		newOpts.Path = opts.Path
	}
	if opts.MaxOpenConns > 0 {
		newOpts.MaxOpenConns = opts.MaxOpenConns
	}
	if opts.MaxIdleConns > 0 {
		newOpts.MaxIdleConns = opts.MaxIdleConns
	}
	if opts.ConnMaxLifetime > 0 {
		newOpts.ConnMaxLifetime = opts.ConnMaxLifetime
	}
	if opts.PageSize > 0 {
		newOpts.PageSize = opts.PageSize
	}
	if opts.CacheSize != 0 {
		newOpts.CacheSize = opts.CacheSize
	}
	if opts.JournalMode != "" {
		newOpts.JournalMode = opts.JournalMode
	}
	if opts.SyncMode != "" {
		newOpts.SyncMode = opts.SyncMode
	}
	if opts.TempStore != "" {
		newOpts.TempStore = opts.TempStore
	}
	if opts.BusyTimeout > 0 {
		newOpts.BusyTimeout = opts.BusyTimeout
	}
	if opts.MMapSize > 0 {
		newOpts.MMapSize = opts.MMapSize
	}
	if opts.WalAutocheckpoint > 0 {
		newOpts.WalAutocheckpoint = opts.WalAutocheckpoint
	}
	if opts.MaxOpenConns != 0 {
		newOpts.MaxOpenConns = opts.MaxOpenConns
	}
	if err := newOpts.Validate(); err != nil {
		panic(fmt.Sprintf("invalid options: %v", err))
	}

	return &newOpts
}

func (options *SQLiteOptions) Validate() error {
	if options.Path == "" {
		return errors.New("database path is required")
	}

	if options.PageSize != 0 && (options.PageSize < 512 || options.PageSize > 65536 || options.PageSize&(options.PageSize-1) != 0) {
		return errors.New("page size must be a power of 2 between 512 and 65536")
	}

	if options.MaxOpenConns < 0 {
		return errors.New("max open connections cannot be negative")
	}

	if options.MaxIdleConns < 0 {
		return errors.New("max idle connections cannot be negative")
	}

	if options.CacheSize < -2147483648 {
		return errors.New("cache size too small")
	}

	if options.BusyTimeout < 0 {
		return errors.New("busy timeout cannot be negative")
	}

	validJournalModes := map[string]bool{
		"DELETE": true,
		"WAL":    true,
		"MEMORY": true,
		"OFF":    true,
	}
	if !validJournalModes[strings.ToUpper(options.JournalMode)] {
		return errors.New("invalid journal mode")
	}

	validSyncModes := map[string]bool{
		"OFF":    true,
		"NORMAL": true,
		"FULL":   true,
		"EXTRA":  true,
	}
	if !validSyncModes[strings.ToUpper(options.SyncMode)] {
		return errors.New("invalid sync mode")
	}

	validTempStores := map[string]bool{
		"DEFAULT": true,
		"FILE":    true,
		"MEMORY":  true,
	}
	if !validTempStores[strings.ToUpper(options.TempStore)] {
		return errors.New("invalid temp store value")
	}

	if options.WalAutocheckpoint < 0 || options.WalAutocheckpoint > 1000000 {
		return errors.New("wal autocheckpoint must be between 0 and 1000000")
	}

	return nil
}

var DefaultShardedCacheOptions *ShardedCacheOptions
var initOnceShardedCacheOptions sync.Once

type ShardedCacheOptions struct {
	ctx          context.Context
	ttl          time.Duration
	ttlDecrement time.Duration
}

func NewShardedCacheOptions(ctx context.Context, ttl time.Duration, ttlDecrement time.Duration) *ShardedCacheOptions {
	return &ShardedCacheOptions{
		ctx:          ctx,
		ttl:          ttl,
		ttlDecrement: ttlDecrement,
	}
}

func GetLongDefaultShardedCacheOptions() *ShardedCacheOptions {
	initOnceShardedCacheOptions.Do(func() {
		if DefaultShardedCacheOptions == nil {
			DefaultShardedCacheOptions = NewShardedCacheOptions(context.Background(), 1000000*time.Hour, 10000*time.Hour)
		}
	})

	return DefaultShardedCacheOptions
}
