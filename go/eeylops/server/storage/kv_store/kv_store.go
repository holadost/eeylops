package kv_store

type KVStore interface {
	GetDataDir() string
	Close() error
	// Get and GetS gets the value for the given key from the store.
	Get(key []byte) (value []byte, err error)
	GetS(key string) (value string, err error)

	// Put and PutS puts the value for the given key in the store.
	Put(key []byte, value []byte) error
	PutS(key string, value string) error

	// Delete and DeleteS deletes the key from the store.
	Delete(key []byte) error
	DeleteS(key string) error

	// MultiGet and MultiGetS does batch Get and GetS .
	MultiGet(keys [][]byte) (values [][]byte, errs []error)
	MultiGetS(keys []string) (values []string, errs []error)

	// BatchPut and BatchPutS does batch Put and PutS .
	BatchPut(keys [][]byte, values [][]byte) error
	BatchPutS(keys []string, values []string) error

	// BatchDelete and BatchDeleteS does Delete and DeleteS .
	BatchDelete(keys [][]byte) error
	BatchDeleteS(keys []string) error

	// Scan and ScanS scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan(startKey []byte, numMessages int, scanSizeBytes int, reverse bool) (keys [][]byte, values [][]byte, nextKey []byte, err error)
	ScanS(startKey string, numMessages int, scanSizeBytes int, reverse bool) (keys []string, values []string, nextKey string, err error)

	// CreateScanner creates a scanner that can be used to iterate over the underlying store.
	CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner
}

type Scanner interface {
	Rewind()
	Valid() bool
	Next()
	Seek(key []byte)
	GetItem() ([]byte, []byte, error)
	Close()
}
