package cf_store

type KVStoreEntry struct {
	Key          []byte // Key.
	Value        []byte // Value.
	ColumnFamily string // Name of the column family.
}

type KVStoreKey struct {
	Key          []byte // Key.
	ColumnFamily string // Name of the column family.
}

type CFStore interface {
	// GetDataDir returns the data directory of the store.
	GetDataDir() string
	// Size returns the total size(in bytes) of the backing store.
	Size() int64
	// Get fetches the given key from the backing store.
	Get(key *KVStoreKey) (*KVStoreEntry, error)
	// Put inserts/updates the given entry in the backing store.
	Put(entry *KVStoreEntry) error
	// Delete deletes the given key from the backing store.
	Delete(key *KVStoreKey) error
	// Scan iterates over a column family from the given startKey and fetches the required number of
	// values. Scans can be in the forward or reverse directions.
	Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
		entries []*KVStoreEntry, nextKey []byte, retErr error)
	// BatchGet multiple keys from the backing store.
	BatchGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error)
	// BatchPut multiple entries in the backing store.
	BatchPut(entries []*KVStoreEntry) error
	// BatchDelete multiple entries from the backing store.
	BatchDelete(keys []*KVStoreKey) error
	// NewScanner returns a new scanner than can be used to iterate over a column family in the backing store.
	NewScanner(cf string, startKey []byte, reverse bool) (Scanner, error)
	// NewTransaction returns a new Transaction.
	NewTransaction() Transaction
}

type Scanner interface {
	// Rewind the scanner/iterator.
	Rewind()
	// Seek to an entry in the scanner.
	Seek(key []byte)
	// GetItem fetches the current item that the iterator is at.
	GetItem() (key []byte, val []byte, err error)
	// Valid returns true if the iterator is still not finished. false otherwise.
	Valid() bool
	// Next moves the iterator to the next item.
	Next()
	// Close the iterator.
	Close()
}

type Transaction interface {
	Get(key *KVStoreKey) (*KVStoreEntry, error)
	// Put and puts the value for the given key in the store.
	Put(entry *KVStoreEntry) error
	// Delete and deletes the key from the store.
	Delete(key *KVStoreKey) error
	// BatchGet and does batch Gets.
	BatchGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error)
	// BatchPut and does batch Puts.
	BatchPut(entries []*KVStoreEntry) error
	// BatchDelete and does Deletes.
	BatchDelete(keys []*KVStoreKey) error
	// Scan and scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
		entries []*KVStoreEntry, nextKey []byte, retErr error)
	// Commit the transaction.
	Commit() error
	// Discard the transaction.
	Discard()
}
