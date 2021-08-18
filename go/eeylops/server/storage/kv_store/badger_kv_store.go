package kv_store

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
	"os"
)

// BadgerKVStore implements a simple KV store interface using badger.
type BadgerKVStore struct {
	db      *badger.DB
	rootDir string
	closed  bool
}

func NewBadgerKVStore(rootDir string, opts badger.Options) *BadgerKVStore {
	kvStore := new(BadgerKVStore)
	kvStore.rootDir = rootDir
	if err := os.MkdirAll(kvStore.rootDir, 0774); err != nil {
		glog.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	kvStore.initialize(opts)
	return kvStore
}

func NewBadgerKVStoreWithDB(rootDir string, db *badger.DB) *BadgerKVStore {
	kvStore := new(BadgerKVStore)
	kvStore.rootDir = rootDir
	kvStore.db = db
	return kvStore
}

func (kvStore *BadgerKVStore) initialize(opts badger.Options) {
	glog.Infof("Initializing badger KV store located at: %s", kvStore.rootDir)
	var err error
	kvStore.db, err = badger.Open(opts)
	kvStore.closed = false
	if err != nil {
		glog.Fatalf("Unable to open consumer store due to err: %s", err.Error())
	}
}

func (kvStore *BadgerKVStore) GetDataDir() string {
	return kvStore.rootDir
}

// Size returns the size of the KV store in bytes.
func (kvStore *BadgerKVStore) Size() int64 {
	a, b := kvStore.db.Size()
	return a + b
}

// Get gets the value associated with the key.
func (kvStore *BadgerKVStore) Get(key []byte) ([]byte, error) {
	var val []byte
	if kvStore.closed {
		return val, ErrKVStoreClosed
	}
	err := kvStore.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return val, ErrKVStoreKeyNotFound
		} else {
			return val, ErrKVStoreBackend
		}
	}
	return val, err
}

// GetS gets the value associated with the key.
func (kvStore *BadgerKVStore) GetS(key string) (string, error) {
	val, err := kvStore.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Put puts a key value pair in the DB. If the key already exists, it would be updated.
func (kvStore *BadgerKVStore) Put(key []byte, value []byte) error {
	if kvStore.closed {
		glog.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	err := kvStore.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
	if err != nil {
		glog.Errorf("Unable to put key: %v due to err: %s", key, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// PutS puts a key value pair in the DB. If the key already exists, it would be updated.
func (kvStore *BadgerKVStore) PutS(key string, value string) error {
	return kvStore.Put([]byte(key), []byte(value))
}

// Delete deletes a key value pair from the DB.
func (kvStore *BadgerKVStore) Delete(key []byte) error {
	if kvStore.closed {
		glog.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	err := kvStore.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
	if err != nil {
		glog.Errorf("Unable to put key: %v due to err: %s", key, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// DeleteS deletes a key value pair from the DB.
func (kvStore *BadgerKVStore) DeleteS(key string) error {
	return kvStore.Put([]byte(key), []byte(""))
}

// Scan scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
func (kvStore *BadgerKVStore) Scan(startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	keys [][]byte, values [][]byte, nextKey []byte, retErr error) {
	txn := kvStore.db.NewTransaction(false)
	defer txn.Discard()
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()

	// Seek to the correct entry.
	if (startKey == nil) || len(startKey) == 0 {
		itr.Rewind()
	} else {
		itr.Seek(startKey)
	}
	currSizeBytes := 0
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		valSize := int(item.ValueSize())
		if (scanSizeBytes > 0) && (currSizeBytes+valSize > scanSizeBytes) {
			break
		}
		currSizeBytes += valSize
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			glog.Errorf("Unable to scan KV store due to err: %s", err)
			return nil, nil, nil, err
		}
		if numValues >= 0 && len(keys) == numValues {
			return keys, values, key, nil
		}
		keys = append(keys, key)
		values = append(values, val)
	}
	// We have reached the end of the DB and there are no more keys.
	return keys, values, nil, nil
}

// ScanS scans the DB in ascending order from the given start key.
func (kvStore *BadgerKVStore) ScanS(startKey string, numValues int, scanSizeBytes int, reverse bool) (
	keys []string, values []string, nextKey string, retErr error) {
	keysByte, valuesByte, nextKeyByte, retErrByte := kvStore.Scan([]byte(startKey), numValues, scanSizeBytes, reverse)
	if retErrByte != nil {
		return nil, nil, "", retErrByte
	}
	for ii := 0; ii < len(keysByte); ii++ {
		keys = append(keys, string(keysByte[ii]))
		values = append(values, string(valuesByte[ii]))
	}
	nextKey = ""
	if nextKeyByte != nil {
		nextKey = string(nextKeyByte)
	}
	return keys, values, nextKey, nil
}

// MultiGet gets the values associated with multiple keys.
func (kvStore *BadgerKVStore) MultiGet(keys [][]byte) (values [][]byte, errs []error) {
	if kvStore.closed {
		glog.Errorf("KV store is closed")
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, []byte{})
			errs = append(errs, ErrKVStoreClosed)
		}
		return
	}
	kvStore.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					errs = append(errs, ErrKVStoreKeyNotFound)
				} else {
					glog.Errorf("Unable to get key: %v due to err: %s", key, err.Error())
					errs = append(errs, ErrKVStoreGeneric)
				}
				errs = append(errs, err)
				values = append(values, nil)
				continue
			}
			tmpValue, err := item.ValueCopy(nil)
			if err != nil {
				glog.Errorf("Unable to parse value for key: %v due to err: %s", key, err.Error())
				errs = append(errs, ErrKVStoreGeneric)
				values = append(values, nil)
				continue
			}
			errs = append(errs, nil)
			values = append(values, tmpValue)
		}
		return nil
	})
	return
}

// MultiGetS gets the values associated with multiple keys.
func (kvStore *BadgerKVStore) MultiGetS(keys []string) ([]string, []error) {
	var values []string

	// Convert keys to byte slice.
	var bkeys [][]byte
	for ii := 0; ii < len(keys); ii++ {
		bkeys = append(bkeys, []byte(keys[ii]))
	}

	// Fetch data from DB.
	bvals, berrs := kvStore.MultiGet(bkeys)

	// Convert values to byte slice.
	for ii := 0; ii < len(berrs); ii++ {
		if berrs[ii] != nil {
			values = append(values, "")
		} else {
			values = append(values, string(bvals[ii]))
		}
	}
	return values, berrs
}

// BatchPut sets/updates multiple key value pairs in the DB.
func (kvStore *BadgerKVStore) BatchPut(keys [][]byte, values [][]byte) error {
	if kvStore.closed {
		glog.Errorf("KV store is already closed")
		return ErrKVStoreBackend
	}
	wb := kvStore.db.NewWriteBatch()
	for ii := 0; ii < len(keys); ii++ {
		if err := wb.Set(keys[ii], values[ii]); err != nil {
			glog.Errorf("Unable to perform batch put due to err: %s", err.Error())
			return ErrKVStoreBackend
		}
	}
	if err := wb.Flush(); err != nil {
		glog.Errorf("Unable to perform flush after batch put due to err: %s", err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// BatchPutS sets/updates multiple key value pairs in the DB.
func (kvStore *BadgerKVStore) BatchPutS(keys []string, values []string) error {
	var bkeys [][]byte
	var bvalues [][]byte
	for ii := 0; ii < len(keys); ii++ {
		bkeys = append(bkeys, []byte(keys[ii]))
		bvalues = append(bvalues, []byte(values[ii]))
	}
	return kvStore.BatchPut(bkeys, bvalues)
}

// BatchDelete deletes multiple key value pairs from the DB.
func (kvStore *BadgerKVStore) BatchDelete(keys [][]byte) error {
	if kvStore.closed {
		glog.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	err := kvStore.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		glog.Errorf("Unable to batch delete keys: %v due to err: %s", keys, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// BatchDeleteS deletes multiple key value pairs from the DB.
func (kvStore *BadgerKVStore) BatchDeleteS(keys []string) error {
	var bkeys [][]byte
	for ii := 0; ii < len(keys); ii++ {
		bkeys = append(bkeys, []byte(keys[ii]))
	}
	return kvStore.BatchDelete(bkeys)
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (kvStore *BadgerKVStore) Close() error {
	glog.Infof("Closing KV store located at: %s", kvStore.rootDir)
	if kvStore.closed {
		return nil
	}
	err := kvStore.db.Close()
	kvStore.closed = true
	kvStore.db = nil
	return err
}

func (kvStore *BadgerKVStore) CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner {
	return newBadgerScanner(kvStore.db, prefix, startKey, reverse)
}

func generatePrefixKey(key []byte, prefix string) []byte {
	return []byte{}
}

type BadgerScanner struct {
	db       *badger.DB
	iter     *badger.Iterator
	txn      *badger.Txn
	startKey []byte
	prefix   []byte
	reverse  bool
}

func newBadgerScanner(db *badger.DB, prefix []byte, startKey []byte, reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.db = db
	scanner.iter = nil
	scanner.startKey = startKey
	scanner.reverse = reverse
	scanner.prefix = prefix
	scanner.initialize()
	return scanner
}

func (scanner *BadgerScanner) initialize() {
	scanner.txn = scanner.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Reverse = scanner.reverse
	scanner.iter = scanner.txn.NewIterator(opts)
	scanner.Rewind()
}

func (scanner *BadgerScanner) Rewind() {
	scanner.iter.Rewind()
	if len(scanner.startKey) > 0 {
		scanner.iter.Seek(scanner.startKey)
	}
}

func (scanner *BadgerScanner) Valid() bool {
	if scanner.prefix == nil || len(scanner.prefix) == 0 {
		return scanner.iter.Valid()
	}
	return scanner.iter.ValidForPrefix(scanner.prefix)
}

func (scanner *BadgerScanner) Next() {
	scanner.iter.Next()
}

func (scanner *BadgerScanner) GetItem() (key []byte, val []byte, err error) {
	item := scanner.iter.Item()
	key = item.KeyCopy(nil)
	val, err = item.ValueCopy(nil)
	return
}

func (scanner *BadgerScanner) Seek(key []byte) {
	scanner.iter.Seek(key)
}

func (scanner *BadgerScanner) Close() {
	scanner.iter.Close()
	scanner.txn.Discard()
}
