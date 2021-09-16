package kv_store

import "errors"

var (
	// ErrKVStoreKeyNotFound is returned when the given key is not found in the KV store.
	ErrKVStoreKeyNotFound = errors.New("ErrKVStoreKeyNotFound: key not found")

	// ErrKVStoreClosed is returned when the KV store is closed.
	ErrKVStoreClosed = errors.New("ErrKVStoreClosed: kv store is closed")

	// ErrKVStoreBackend is returned when there is some generic kv store error.
	ErrKVStoreBackend = errors.New("ErrKVStoreBackend: kv store backing store error")

	// ErrKVStoreGeneric is returned when there is some generic kv store error.
	ErrKVStoreGeneric = errors.New("ErrKVStoreGeneric: kv store generic error")

	// ErrKVStoreConflict is returned when there is a conflict error. This can happen when there are simultaneous
	// transactions.
	ErrKVStoreConflict = errors.New("ErrKVStoreConflict: kv store conflict error")

	// ErrKVStoreReservedColumnFamilyNames is returned when the CF name provided is reserved.
	ErrKVStoreReservedColumnFamilyNames = errors.New("ErrKVStoreReservedColumnFamilyNames: cannot use " +
		"reserved column family names")

	// ErrKVStoreInvalidColumnFamilyName is returned when the CF name provided is invalid.
	ErrKVStoreInvalidColumnFamilyName = errors.New("ErrKVStoreInvalidColumnFamilyName: invalid CF name")

	// ErrKVStoreColumnFamilyExists is returned when the CF already exists.
	ErrKVStoreColumnFamilyExists = errors.New("ErrKVStoreColumnFamilyExists: column family already exists")

	// ErrKVStoreColumnFamilyNotFound is returned when the CF already exists.
	ErrKVStoreColumnFamilyNotFound = errors.New("ErrKVStoreColumnFamilyNotFound: column family not found")
)
