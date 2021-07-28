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
)
