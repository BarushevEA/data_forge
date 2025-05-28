package types

import "github.com/BarushevEA/in_memory_cache/types"

// ITable represents an interface for managing a key-value table with analytics capabilities
type ITable[T any] interface {
	// Get retrieves a value by its key
	// Returns the value and true if found, zero value and false otherwise
	Get(key string) (T, bool)

	// GetTop returns a map of the most frequently accessed elements
	// The number of elements is limited by SetTopLimit and thresholds
	GetTop() (map[string]T, error)

	// BatchGet retrieves multiple values by their keys
	// Returns an array of BatchNode containing results for each key
	BatchGet(keys []string) ([]types.BatchNode[T], error)

	// Range iterates over all elements in the table
	// The iteration stops if callback returns false
	Range(callback func(key string, value T) bool) error

	// RangeCacheMetrics iterates over cache metrics for all elements
	// The iteration stops if the callback returns false
	RangeCacheMetrics(callback func(metric types.Metric[T]) bool) error

	// Set stores a value with the given key
	// Returns error if operation fails
	Set(key string, value T) error

	// BatchSet stores multiple key-value pairs at once
	// Returns error if operation fails
	BatchSet(items map[string]T) error

	// SetTopLimit sets the maximum number of elements that can be returned by GetTop
	// Returns error if the operation fails
	SetTopLimit(limit uint) error

	// SetTopAccessThreshold sets the minimum number of reads required for an element to be considered "top"
	// Returns error if operation fails
	SetTopAccessThreshold(threshold uint) error

	// SetTopWriteThreshold sets the minimum number of writings required for an element to be considered "top"
	// Returns error if operation fails
	SetTopWriteThreshold(threshold uint) error

	// Delete removes an element by its key
	// Returns error if the operation fails
	Delete(key string) error

	// BatchDelete removes multiple elements by their keys
	// Returns error if the operation fails
	BatchDelete(keys []string) error

	// Destroy completely removes the table and all its data
	// Returns error if the operation fails
	Destroy() error

	// Clear removes all elements from the table but keeps the table itself
	// Returns error if operation fails
	Clear() error

	// Len returns the current number of elements in the table
	Len() int
}
