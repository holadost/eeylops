package kv_store

type KVStore interface {
	GetDataDir() string
	Close() error
	// Get and GetS gets the value for the given key from the store.
	Get([]byte) ([]byte, error)
	GetS(string) (string, error)

	// Put and PutS puts the value for the given key in the store.
	Put([]byte, []byte) error
	PutS(string, string) error

	// Delete and DeleteS deletes the key from the store.
	Delete([]byte) error
	DeleteS(string) error

	// MultiGet and MultiGetS does batch Get and GetS .
	MultiGet([][]byte) ([][]byte, []error)
	MultiGetS([]string) ([]string, []error)

	// BatchPut and BatchPutS does batch Put and PutS .
	BatchPut([][]byte, [][]byte) error
	BatchPutS([]string, []string) error

	// BatchDelete and BatchDeleteS does Delete and DeleteS .
	BatchDelete([][]byte) error
	BatchDeleteS([]string) error

	// Scan and ScanS scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan([]byte, int, int, bool) ([][]byte, [][]byte, []byte, error)
	ScanS(string, int, int, bool) ([]string, []string, string, error)

	// CreateScanner creates a scanner that can be used to iterate over the underlying store.
	CreateScanner(prefix []byte, startKey []byte, endKey []byte, batchSize uint) Scanner
}

type Scanner interface {
}
