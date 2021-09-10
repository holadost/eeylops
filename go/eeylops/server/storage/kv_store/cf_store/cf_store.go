package cf_store

type Transaction interface {
	Get(key *KVStoreKey) (*KVStoreEntry, error)
	// Put and PutS puts the value for the given key in the store.
	Put(entry *KVStoreEntry) error
	// Delete and DeleteS deletes the key from the store.
	Delete(key *KVStoreKey) error
	// MultiGet and MultiGetS does batch Get and GetS .
	MultiGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error)
	// BatchPut and BatchPutS does batch Put and PutS .
	BatchPut(entries []*KVStoreEntry) error
	// BatchDelete and BatchDeleteS does Delete and DeleteS .
	BatchDelete(keys []*KVStoreKey) error
	// Scan and ScanS scans values from the store starting from the given start key and num keys. Every scan call
	// returns a slice of keys, values, the next key and scan error(if any).
	Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
		entries []*KVStoreEntry, nextKey []byte, retErr error)
	// Commit the transaction.
	Commit() error
	// Discard the transaction.
	Discard()
}
