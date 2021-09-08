package kv_store

type KVStore interface {
	// GetDataDir returns the data directory for this store.
	GetDataDir() string
	// Size returns the size of this store in bytes.
	Size() int64 // Size in bytes.
	// Close the store.
	Close() error
	// Get fetches the value for the given key from the store.
	Get(key []byte) (value []byte, err error)
	// Put writes the value for the given key in the store.
	Put(key []byte, value []byte) error
	// Delete removes the key from the store.
	Delete(key []byte) error
	// MultiGet does batch Get.
	MultiGet(keys [][]byte) (values [][]byte, errs []error)
	// BatchPut is the same as Put but in batches.
	BatchPut(keys [][]byte, values [][]byte) error
	// BatchDelete performs batched deletes.
	BatchDelete(keys [][]byte) error
	// Scan scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan(startKey []byte, numMessages int, scanSizeBytes int, reverse bool) (keys [][]byte, values [][]byte,
		nextKey []byte, err error)
	// CreateScanner creates a scanner that can be used to iterate over the underlying store.
	CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner
	// NewTransaction starts a new KV store transaction.
	NewTransaction() Transaction
}

type Scanner interface {
	// Rewind the iterator.
	Rewind()
	// Valid returns true if the iterator is still valid. false otherwise.
	Valid() bool
	// Next iterates to the next value.
	Next()
	// Seek seeks to the desired key.
	Seek(key []byte)
	// GetItem fetches the item.
	GetItem() ([]byte, []byte, error)
	// Close closes the iterator.
	Close()
}

type Transaction interface {
	Get(key []byte) (value []byte, err error)
	// Put and PutS puts the value for the given key in the store.
	Put(key []byte, value []byte) error
	// Delete and DeleteS deletes the key from the store.
	Delete(key []byte) error
	// MultiGet and MultiGetS does batch Get and GetS .
	MultiGet(keys [][]byte) (values [][]byte, errs []error)
	// BatchPut and BatchPutS does batch Put and PutS .
	BatchPut(keys [][]byte, values [][]byte) error
	// BatchDelete and BatchDeleteS does Delete and DeleteS .
	BatchDelete(keys [][]byte) error
	// Scan and ScanS scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan(startKey []byte, numMessages int, scanSizeBytes int, reverse bool) (keys [][]byte, values [][]byte,
		nextKey []byte, err error)
	// CreateScanner creates a scanner that can be used to iterate over the underlying store.
	CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner
	// Commit the transaction.
	Commit() error
	// Discard the transaction.
	Discard()
}
