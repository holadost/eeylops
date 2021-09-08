package kv_store

import (
	"eeylops/util/logging"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
	"os"
)

// BadgerKVStore implements a simple KV store interface using badger.
type BadgerKVStore struct {
	db      *badger.DB
	rootDir string
	closed  bool
	logger  *logging.PrefixLogger
}

func NewBadgerKVStore(rootDir string, opts badger.Options) *BadgerKVStore {
	logger := logging.NewPrefixLogger("badger_kv_store")
	return newBadgerKVStore(rootDir, logger, opts)
}

func NewBadgerKVStoreWithLogger(rootDir string, logger *logging.PrefixLogger, opts badger.Options) *BadgerKVStore {
	return newBadgerKVStore(rootDir, logger, opts)
}

func newBadgerKVStore(rootDir string, logger *logging.PrefixLogger, opts badger.Options) *BadgerKVStore {
	kvStore := new(BadgerKVStore)
	kvStore.rootDir = rootDir
	kvStore.logger = logger
	if err := os.MkdirAll(kvStore.rootDir, 0774); err != nil {
		kvStore.logger.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	opts.Logger = logging.NewPrefixLoggerWithParentAndDepth("", logger, 1)
	kvStore.initialize(opts)
	return kvStore
}

func (kvStore *BadgerKVStore) initialize(opts badger.Options) {
	kvStore.logger.Infof("Initializing badger KV store located at: %s", kvStore.rootDir)
	var err error
	kvStore.db, err = badger.Open(opts)
	kvStore.closed = false
	if err != nil {
		kvStore.logger.Fatalf("Unable to open consumer store due to err: %s", err.Error())
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
	if kvStore.closed {
		return nil, ErrKVStoreClosed
	}
	txn := kvStore.db.NewTransaction(false)
	defer txn.Discard()
	return kvStore.internalGet(txn, key)
}

func (kvStore *BadgerKVStore) internalGet(txn *badger.Txn, key []byte) ([]byte, error) {
	var val []byte
	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return val, ErrKVStoreKeyNotFound
		} else {
			return val, ErrKVStoreBackend
		}
	}
	val, err = item.ValueCopy(nil)
	if err != nil {
		return nil, ErrKVStoreClosed
	}
	return val, nil
}

// Put puts a key value pair in the DB. If the key already exists, it would be updated.
func (kvStore *BadgerKVStore) Put(key []byte, value []byte) error {
	if kvStore.closed {
		kvStore.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	txn := kvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := kvStore.internalPut(txn, key, value)
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		kvStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

func (kvStore *BadgerKVStore) internalPut(txn *badger.Txn, key []byte, value []byte) error {
	err := txn.Set(key, value)
	if err != nil {
		kvStore.logger.Errorf("Unable to put key: %v due to err: %s", key, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// Delete deletes a key value pair from the DB.
func (kvStore *BadgerKVStore) Delete(key []byte) error {
	if kvStore.closed {
		kvStore.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	txn := kvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := kvStore.internalDelete(txn, key)
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		kvStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

func (kvStore *BadgerKVStore) internalDelete(txn *badger.Txn, key []byte) error {
	err := txn.Delete(key)
	if err != nil {
		kvStore.logger.Errorf("Unable to delete key: %v due to err: %s", key, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// Scan scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
func (kvStore *BadgerKVStore) Scan(startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	keys [][]byte, values [][]byte, nextKey []byte, retErr error) {
	txn := kvStore.db.NewTransaction(false)
	defer txn.Discard()
	return kvStore.internalScan(txn, startKey, numValues, scanSizeBytes, reverse)
}

func (kvStore *BadgerKVStore) internalScan(txn *badger.Txn, startKey []byte, numValues int, scanSizeBytes int,
	reverse bool) (keys [][]byte, values [][]byte, nextKey []byte, retErr error) {
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()

	// Seek to the correct entry.
	if (startKey == nil) || len(startKey) == 0 {
		itr.Rewind()
	} else {
		itr.Seek(startKey)
	}
	currSizeBytes := int64(0)
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		valSize := item.ValueSize()
		if (scanSizeBytes > 0) && (currSizeBytes+valSize > int64(scanSizeBytes)) {
			break
		}
		currSizeBytes += valSize
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			kvStore.logger.Errorf("Unable to scan KV store due to err: %s", err)
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

// MultiGet gets the values associated with multiple keys.
func (kvStore *BadgerKVStore) MultiGet(keys [][]byte) (values [][]byte, errs []error) {
	if kvStore.closed {
		kvStore.logger.Errorf("KV store is closed")
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, []byte{})
			errs = append(errs, ErrKVStoreClosed)
		}
		return
	}
	txn := kvStore.db.NewTransaction(false)
	defer txn.Discard()
	return kvStore.internalMultiGet(txn, keys)
}

func (kvStore *BadgerKVStore) internalMultiGet(txn *badger.Txn, keys [][]byte) (values [][]byte, errs []error) {
	for _, key := range keys {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				errs = append(errs, ErrKVStoreKeyNotFound)
			} else {
				kvStore.logger.Errorf("Unable to get key: %v due to err: %s", key, err.Error())
				errs = append(errs, ErrKVStoreGeneric)
			}
			values = append(values, nil)
			continue
		}
		tmpValue, err := item.ValueCopy(nil)
		if err != nil {
			kvStore.logger.Errorf("Unable to parse value for key: %v due to err: %s", key, err.Error())
			errs = append(errs, ErrKVStoreGeneric)
			values = append(values, nil)
			continue
		}
		errs = append(errs, nil)
		values = append(values, tmpValue)
	}
	return
}

// BatchPut sets/updates multiple key value pairs in the DB.
func (kvStore *BadgerKVStore) BatchPut(keys [][]byte, values [][]byte) error {
	if kvStore.closed {
		kvStore.logger.Errorf("KV store is already closed")
		return ErrKVStoreBackend
	}
	txn := kvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := kvStore.internalBatchPut(txn, keys, values)
	if err == nil {
		cerr := txn.Commit()
		if cerr != nil {
			kvStore.logger.Errorf("Unable to commit transaction due to err: %s", cerr.Error())
			return ErrKVStoreBackend
		}
		return nil
	}
	return err
}

func (kvStore *BadgerKVStore) internalBatchPut(txn *badger.Txn, keys [][]byte, values [][]byte) error {
	for ii := 0; ii < len(keys); ii++ {
		if err := txn.Set(keys[ii], values[ii]); err != nil {
			kvStore.logger.Errorf("Unable to set keys due to err: %s", err.Error())
			return ErrKVStoreBackend
		}
	}
	return nil
}

// BatchDelete deletes multiple key value pairs from the DB.
func (kvStore *BadgerKVStore) BatchDelete(keys [][]byte) error {
	if kvStore.closed {
		kvStore.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	txn := kvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := kvStore.internalBatchDelete(txn, keys)
	if err != nil {
		return err
	}
	cerr := txn.Commit()
	if cerr != nil {
		kvStore.logger.Errorf("Unable to commit batch delete transaction due to err: %s", cerr.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// BatchDelete deletes multiple key value pairs from the DB.
func (kvStore *BadgerKVStore) internalBatchDelete(txn *badger.Txn, keys [][]byte) error {
	var err error
	for _, key := range keys {
		err := txn.Delete(key)
		if err != nil {
			break
		}
	}
	if err != nil {
		kvStore.logger.Errorf("Unable to batch delete keys: %v due to err: %s", keys, err.Error())
		return ErrKVStoreBackend
	}
	return nil
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (kvStore *BadgerKVStore) Close() error {
	kvStore.logger.Infof("Closing KV store located at: %s", kvStore.rootDir)
	if kvStore.closed {
		return nil
	}
	err := kvStore.db.Close()
	kvStore.closed = true
	kvStore.db = nil
	return err
}

func (kvStore *BadgerKVStore) CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner {
	return newBadgerScannerWithDb(kvStore.db, prefix, startKey, reverse)
}

func (kvStore *BadgerKVStore) NewTransaction() Transaction {
	return newBadgerTransaction(kvStore)
}

type BadgerScanner struct {
	db       *badger.DB
	iter     *badger.Iterator
	txn      *badger.Txn
	localTxn bool
	startKey []byte
	prefix   []byte
	reverse  bool
}

func newBadgerScannerWithDb(db *badger.DB, prefix []byte, startKey []byte, reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.db = db
	scanner.txn = nil
	scanner.iter = nil
	scanner.startKey = startKey
	scanner.reverse = reverse
	scanner.prefix = prefix
	scanner.initialize()
	return scanner
}

func newBadgerScannerWithTxn(txn *badger.Txn, prefix []byte, startKey []byte, reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.txn = txn
	scanner.localTxn = false
	scanner.db = nil
	scanner.iter = nil
	scanner.startKey = startKey
	scanner.reverse = reverse
	scanner.prefix = prefix
	scanner.initialize()
	return scanner
}

func (scanner *BadgerScanner) initialize() {
	if scanner.txn == nil {
		if scanner.db == nil {
			glog.Fatalf("Either db or txn must have been provided")
		}
		scanner.localTxn = true
		scanner.txn = scanner.db.NewTransaction(false)
	}
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
	if scanner.localTxn {
		// Discard the transaction iff we created it. If it was passed from outside, skip discarding.
		scanner.txn.Discard()
	}
}

type BadgerKVStoreTxn struct {
	store *BadgerKVStore
	txn   *badger.Txn
}

func newBadgerTransaction(store *BadgerKVStore) *BadgerKVStoreTxn {
	btxn := &BadgerKVStoreTxn{
		store: store,
		txn:   store.db.NewTransaction(true),
	}
	return btxn
}

// Get gets the value associated with the key.
func (badgerKVTxn *BadgerKVStoreTxn) Get(key []byte) ([]byte, error) {
	if badgerKVTxn.store.closed {
		return nil, ErrKVStoreClosed
	}
	return badgerKVTxn.store.internalGet(badgerKVTxn.txn, key)
}

// Put puts a key value pair in the DB. If the key already exists, it would be updated.
func (badgerKVTxn *BadgerKVStoreTxn) Put(key []byte, value []byte) error {
	if badgerKVTxn.store.closed {
		badgerKVTxn.store.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	return badgerKVTxn.store.internalPut(badgerKVTxn.txn, key, value)
}

// Delete deletes a key value pair from the DB.
func (badgerKVTxn *BadgerKVStoreTxn) Delete(key []byte) error {
	if badgerKVTxn.store.closed {
		badgerKVTxn.store.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	return badgerKVTxn.store.internalDelete(badgerKVTxn.txn, key)
}

// Scan scans values from the DB starting from the given startKey.
func (badgerKVTxn *BadgerKVStoreTxn) Scan(startKey []byte, numMessages int, scanSizeBytes int,
	reverse bool) (keys [][]byte, values [][]byte, nextKey []byte, err error) {
	return badgerKVTxn.store.internalScan(badgerKVTxn.txn, startKey, numMessages, scanSizeBytes, reverse)
}

// MultiGet performs batch gets.
func (badgerKVTxn *BadgerKVStoreTxn) MultiGet(keys [][]byte) ([][]byte, []error) {
	return badgerKVTxn.store.internalMultiGet(badgerKVTxn.txn, keys)
}

// BatchPut does batch Put i.e. combines all the updates into a single write.
func (badgerKVTxn *BadgerKVStoreTxn) BatchPut(keys [][]byte, values [][]byte) error {
	if badgerKVTxn.store.closed {
		badgerKVTxn.store.logger.Errorf("KV store is closed")
		return ErrKVStoreClosed
	}
	return badgerKVTxn.store.internalBatchPut(badgerKVTxn.txn, keys, values)
}

// BatchDelete performs a batch delete operation.
func (badgerKVTxn *BadgerKVStoreTxn) BatchDelete(keys [][]byte) error {
	return badgerKVTxn.store.internalBatchDelete(badgerKVTxn.txn, keys)
}

func (badgerKVTxn *BadgerKVStoreTxn) Commit() error {
	err := badgerKVTxn.txn.Commit()
	if err != nil {
		badgerKVTxn.store.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
		if err == badger.ErrConflict {
			return ErrKVStoreConflict
		}
		return ErrKVStoreBackend
	}
	return nil
}

func (badgerKVTxn *BadgerKVStoreTxn) Discard() {
	badgerKVTxn.txn.Discard()
}

func (badgerKVTxn *BadgerKVStoreTxn) CreateScanner(prefix []byte, startKey []byte, reverse bool) Scanner {
	return newBadgerScannerWithTxn(badgerKVTxn.txn, prefix, startKey, reverse)
}
