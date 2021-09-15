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

	// ErrReservedColumnFamilyNames is returned when the CF name provided is reserved.
	ErrReservedColumnFamilyNames = errors.New("ErrReservedColumnFamilyNames: cannot use reserved column " +
		"family names")

	// ErrInvalidColumnFamilyName is returned when the CF name provided is invalid.
	ErrInvalidColumnFamilyName = errors.New("ErrInvalidColumnFamilyName: invalid CF name")

	// ErrColumnFamilyExists is returned when the CF already exists.
	ErrColumnFamilyExists = errors.New("ErrColumnFamilyExists: column family already exists")

	// ErrColumnFamilyNotFound is returned when the CF already exists.
	ErrColumnFamilyNotFound = errors.New("ErrColumnFamilyNotFound: column family not found")
)
