package cf_store

import (
	"eeylops/server/storage/kv_store"
	"eeylops/util/logging"
	"github.com/dgraph-io/badger/v2"
	"os"
)

const kMaxKeyLength = 60000
const kInternalMaxKeyLength = kMaxKeyLength + 4000

var separatorBytes = []byte("::")

type KVStoreEntry struct {
	Key          []byte // Key.
	Value        []byte // Value.
	ColumnFamily string // Name of the column family.
}

type KVStoreKey struct {
	Key          []byte // Key.
	ColumnFamily string // Name of the column family.
}

type BadgerCFStore struct {
	internalStore *internalBadgerCFStore
}

func NewBadgerCFStore(rootDir string, opts badger.Options) *BadgerCFStore {
	logger := logging.NewPrefixLogger("badger_cf_store")
	return newBadgerCFStore(rootDir, logger, opts)
}

func NewBadgerCFStoreWithLogger(rootDir string, logger *logging.PrefixLogger,
	opts badger.Options) *BadgerCFStore {
	return newBadgerCFStore(rootDir, logger, opts)
}

func newBadgerCFStore(rootDir string, logger *logging.PrefixLogger, opts badger.Options) *BadgerCFStore {
	kvStore := new(internalBadgerCFStore)
	kvStore.rootDir = rootDir
	kvStore.logger = logger
	if err := os.MkdirAll(kvStore.rootDir, 0774); err != nil {
		kvStore.logger.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	opts.Logger = logging.NewPrefixLoggerWithParentAndDepth("", logger, 1)
	kvStore.initialize(opts)
	return &BadgerCFStore{internalStore: kvStore}
}

func (cfStore *BadgerCFStore) GetDataDir() string {
	return cfStore.internalStore.GetDataDir()
}

// Size returns the size of the KV store in bytes.
func (cfStore *BadgerCFStore) Size() int64 {
	return cfStore.internalStore.Size()
}

func (cfStore *BadgerCFStore) Get(key *KVStoreKey) (*KVStoreEntry, error) {
	return cfStore.internalStore.Get(key)
}

func (cfStore *BadgerCFStore) Put(entry *KVStoreEntry) error {
	return cfStore.internalStore.Put(entry)
}

func (cfStore *BadgerCFStore) Delete(key *KVStoreKey) error {
	return cfStore.internalStore.Delete(key)
}

func (cfStore *BadgerCFStore) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*KVStoreEntry, nextKey []byte, retErr error) {
	return cfStore.internalStore.Scan(cf, startKey, numValues, scanSizeBytes, reverse)
}

func (cfStore *BadgerCFStore) MultiGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error) {
	return cfStore.internalStore.MultiGet(keys)
}

func (cfStore *BadgerCFStore) BatchPut(entries []*KVStoreEntry) error {
	return cfStore.internalStore.BatchPut(entries)
}

func (cfStore *BadgerCFStore) BatchDelete(keys []*KVStoreKey) error {
	return cfStore.internalStore.BatchDelete(keys)
}

func (cfStore *BadgerCFStore) NewScanner(cf string, startKey []byte, reverse bool) kv_store.Scanner {
	return nil
}

func (cfStore *BadgerCFStore) NewTransaction() Transaction {
	return newBadgerCFStoreTransaction(cfStore.internalStore)
}

/*************************************************** INTERNAL CF STORE ************************************************/
type internalBadgerCFStore struct {
	db      *badger.DB
	rootDir string
	closed  bool
	logger  *logging.PrefixLogger
}

func (cfStore *internalBadgerCFStore) initialize(opts badger.Options) {
	cfStore.logger.Infof("Initializing badger KV store located at: %s", cfStore.rootDir)
	var err error
	cfStore.db, err = badger.Open(opts)
	cfStore.closed = false
	if err != nil {
		cfStore.logger.Fatalf("Unable to open consumer store due to err: %s", err.Error())
	}
}

func (cfStore *internalBadgerCFStore) GetDataDir() string {
	return cfStore.rootDir
}

// Size returns the size of the KV store in bytes.
func (cfStore *internalBadgerCFStore) Size() int64 {
	a, b := cfStore.db.Size()
	return a + b
}

// AddColumnFamily adds the column family(if it does not exist) to the KV store.
func (cfStore *internalBadgerCFStore) AddColumnFamily(cf string) error {
	return nil
}

// Get gets the value associated with the key.
func (cfStore *internalBadgerCFStore) Get(key *KVStoreKey) (*KVStoreEntry, error) {
	if cfStore.closed {
		return nil, kv_store.ErrKVStoreClosed
	}
	txn := cfStore.db.NewTransaction(false)
	defer txn.Discard()
	return cfStore.GetWithTxn(txn, key)
}

// GetWithTxn gets the value associated with the key. This method requires a transaction to be passed. Transaction
// commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) GetWithTxn(txn *badger.Txn, key *KVStoreKey) (*KVStoreEntry, error) {
	if cfStore.closed {
		return nil, kv_store.ErrKVStoreClosed
	}
	var val []byte
	item, err := txn.Get(cfStore.buildCFKey(key.ColumnFamily, key.Key))
	var entry KVStoreEntry
	entry.Key = key.Key
	entry.ColumnFamily = key.ColumnFamily
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &entry, kv_store.ErrKVStoreKeyNotFound
		} else {
			return &entry, kv_store.ErrKVStoreBackend
		}
	}
	val, err = item.ValueCopy(nil)
	if err != nil {
		return &entry, kv_store.ErrKVStoreBackend
	}
	entry.Value = val
	return &entry, nil
}

// Put gets the value associated with the key.
func (cfStore *internalBadgerCFStore) Put(entry *KVStoreEntry) error {
	if cfStore.closed {
		return kv_store.ErrKVStoreClosed
	}
	txn := cfStore.db.NewTransaction(true)
	defer txn.Discard()
	var err error
	if err = cfStore.PutWithTxn(txn, entry); err == nil {
		if cErr := txn.Commit(); cErr != nil {
			if cErr == badger.ErrConflict {
				return kv_store.ErrKVStoreConflict
			}
			cfStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
			return kv_store.ErrKVStoreBackend
		}
		return nil
	}
	return err
}

// PutWithTxn puts a key value pair in the DB. If the key already exists, it would be updated. This method
// requires a transaction to be passed. Transaction commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) PutWithTxn(txn *badger.Txn, entry *KVStoreEntry) error {
	if cfStore.closed {
		cfStore.logger.Errorf("KV store is closed")
		return kv_store.ErrKVStoreClosed
	}
	err := txn.Set(cfStore.buildCFKey(entry.ColumnFamily, entry.Key), entry.Value)
	if err != nil {
		cfStore.logger.Errorf("Unable to put key: %v due to err: %s", entry.Key, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// Delete deletes a key value pair from the DB.
func (cfStore *internalBadgerCFStore) Delete(key *KVStoreKey) error {
	if cfStore.closed {
		cfStore.logger.Errorf("KV store is closed")
		return kv_store.ErrKVStoreClosed
	}
	txn := cfStore.db.NewTransaction(true)
	defer txn.Discard()
	err := cfStore.DeleteWithTxn(txn, key)
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		cfStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// DeleteWithTxn removes the given key value pair from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) DeleteWithTxn(txn *badger.Txn, key *KVStoreKey) error {
	err := txn.Delete(cfStore.buildCFKey(key.ColumnFamily, key.Key))
	if err != nil {
		cfStore.logger.Errorf("Unable to delete key: %v due to err: %s", key, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// Scan scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
func (cfStore *internalBadgerCFStore) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*KVStoreEntry, nextKey []byte, retErr error) {
	txn := cfStore.db.NewTransaction(false)
	defer txn.Discard()
	return cfStore.ScanWithTxn(txn, cf, startKey, numValues, scanSizeBytes, reverse)
}

// ScanWithTxn scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
// Transaction commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) ScanWithTxn(txn *badger.Txn, cf string, startKey []byte, numValues int,
	scanSizeBytes int, reverse bool) (entries []*KVStoreEntry, nextKey []byte, retErr error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	itr := txn.NewIterator(opts)
	defer itr.Close()

	// Seek to the correct entry.
	if (startKey == nil) || len(startKey) == 0 {
		itr.Rewind()
		if !reverse {
			itr.Seek(cfStore.buildFirstCFKey(cf))
		} else {
			itr.Seek(cfStore.buildLastCFKey(cf))
		}

	} else {
		itr.Seek(cfStore.buildCFKey(cf, startKey))
	}
	currSizeBytes := int64(0)
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		valSize := item.ValueSize()
		currSizeBytes += valSize
		fullKey := item.KeyCopy(nil)
		key := cfStore.extractUserKey(cf, fullKey)
		val, err := item.ValueCopy(nil)
		if err != nil {
			cfStore.logger.Errorf("Unable to scan KV store due to err: %s", err)
			return nil, nil, err
		}
		if (scanSizeBytes > 0) && (currSizeBytes+valSize > int64(scanSizeBytes)) {
			return entries, key, nil
		}
		if numValues >= 0 && len(entries) == numValues {
			return entries, key, nil
		}
		entries = append(entries, &KVStoreEntry{
			Key:          key,
			Value:        val,
			ColumnFamily: cf,
		})
	}
	// We have reached the end of the DB and there are no more keys.
	return entries, nil, nil
}

// MultiGet fetches multiple keys from the DB.
func (cfStore *internalBadgerCFStore) MultiGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error) {
	if cfStore.closed {
		cfStore.logger.Errorf("KV store is closed")
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, &KVStoreEntry{})
			errs = append(errs, kv_store.ErrKVStoreClosed)
		}
		return
	}
	txn := cfStore.db.NewTransaction(false)
	defer txn.Discard()
	return cfStore.MultiGetWithTxn(txn, keys)
}

// MultiGetWithTxn fetches multiple keys from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) MultiGetWithTxn(txn *badger.Txn, keys []*KVStoreKey) (values []*KVStoreEntry,
	errs []error) {
	for _, key := range keys {
		item, err := txn.Get(cfStore.buildCFKey(key.ColumnFamily, key.Key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				errs = append(errs, kv_store.ErrKVStoreKeyNotFound)
			} else {
				cfStore.logger.Errorf("Unable to get key: %v due to err: %s", key, err.Error())
				errs = append(errs, kv_store.ErrKVStoreGeneric)
			}
			values = append(values, nil)
			continue
		}
		tmpValue, err := item.ValueCopy(nil)
		if err != nil {
			cfStore.logger.Errorf("Unable to parse value for key: %v due to err: %s", key, err.Error())
			errs = append(errs, kv_store.ErrKVStoreGeneric)
			values = append(values, nil)
			continue
		}
		values = append(values, &KVStoreEntry{
			Key:          key.Key,
			Value:        tmpValue,
			ColumnFamily: key.ColumnFamily,
		})
		errs = append(errs, nil)
	}
	return
}

// BatchPut sets/updates multiple key value pairs in the DB.
func (cfStore *internalBadgerCFStore) BatchPut(entries []*KVStoreEntry) error {
	if cfStore.closed {
		cfStore.logger.Errorf("KV store is already closed")
		return kv_store.ErrKVStoreBackend
	}
	txn := cfStore.db.NewTransaction(true)
	defer txn.Discard()
	err := cfStore.BatchPutWithTxn(txn, entries)
	if err == nil {
		cerr := txn.Commit()
		if cerr != nil {
			if cerr == badger.ErrConflict {
				return kv_store.ErrKVStoreConflict
			}
			cfStore.logger.Errorf("Unable to commit transaction due to err: %s", cerr.Error())
			return kv_store.ErrKVStoreBackend
		}
		return nil
	}
	return err
}

// BatchPutWithTxn sets multiple key value pairs in the DB. This method needs a transaction to be passed. Transaction
// commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) BatchPutWithTxn(txn *badger.Txn, entries []*KVStoreEntry) error {
	for _, entry := range entries {
		if err := txn.Set(cfStore.buildCFKey(entry.ColumnFamily, entry.Key), entry.Value); err != nil {
			cfStore.logger.Errorf("Unable to set keys due to err: %s", err.Error())
			return kv_store.ErrKVStoreBackend
		}
	}
	return nil
}

// BatchDelete deletes multiple key value pairs from the DB.
func (cfStore *internalBadgerCFStore) BatchDelete(keys []*KVStoreKey) error {
	if cfStore.closed {
		cfStore.logger.Errorf("KV store is closed")
		return kv_store.ErrKVStoreClosed
	}
	txn := cfStore.db.NewTransaction(true)
	defer txn.Discard()
	err := cfStore.BatchDeleteWithTxn(txn, keys)
	if err != nil {
		return err
	}
	cerr := txn.Commit()
	if cerr != nil {
		cfStore.logger.Errorf("Unable to commit batch delete transaction due to err: %s", cerr.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// BatchDeleteWithTxn deletes multiple key value pairs from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (cfStore *internalBadgerCFStore) BatchDeleteWithTxn(txn *badger.Txn, keys []*KVStoreKey) error {
	var err error
	for _, key := range keys {
		err := txn.Delete(cfStore.buildCFKey(key.ColumnFamily, key.Key))
		if err != nil {
			break
		}
	}
	if err != nil {
		cfStore.logger.Errorf("Unable to batch delete keys: %v due to err: %s", keys, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

func (cfStore *internalBadgerCFStore) NewTransaction() *badger.Txn {
	return cfStore.db.NewTransaction(true)
}

// Close the DB.
func (cfStore *internalBadgerCFStore) Close() error {
	cfStore.logger.Infof("Closing KV store located at: %s", cfStore.rootDir)
	if cfStore.closed {
		return nil
	}
	err := cfStore.db.Close()
	cfStore.closed = true
	cfStore.db = nil
	return err
}

// buildFirstCFKey is a helper method that creates the first key for a column family.
func (cfStore *internalBadgerCFStore) buildFirstCFKey(cf string) []byte {
	cfKey := cfStore.buildCFPrefixBytes(cf)
	return append(cfKey, make([]byte, kInternalMaxKeyLength-len(cfKey))...)
}

// buildLastCFKey is a helper method that creates the last key for a column family.
func (cfStore *internalBadgerCFStore) buildLastCFKey(cf string) []byte {
	cfKey := cfStore.buildCFPrefixBytes(cf)
	remBytes := make([]byte, kInternalMaxKeyLength-len(cfKey))
	for ii := 0; ii < len(remBytes); ii++ {
		remBytes[ii] = byte(255)
	}
	return append(cfKey, remBytes...)
}

// buildCFKey is a helper method that creates the key based on the CF.
func (cfStore *internalBadgerCFStore) buildCFKey(cf string, key []byte) []byte {
	if len(key) == 0 {
		cfStore.logger.Fatalf("Unable to build key since key is empty")
	}
	return append(cfStore.buildCFPrefixBytes(cf), key...)
}

// extractUserKey is a helper method that extracts the user key from the full key which also includes the CF name.
func (cfStore *internalBadgerCFStore) extractUserKey(cf string, key []byte) []byte {
	if len(key) == 0 {
		cfStore.logger.Fatalf("Unable to extract user key since the given full key is empty")
	}
	cfKey := cfStore.buildCFPrefixBytes(cf)
	return key[len(cfKey):]
}

// buildCFPrefixBytes is a helper method that creates the CF prefix bytes.
func (cfStore *internalBadgerCFStore) buildCFPrefixBytes(cf string) []byte {
	if len(cf) == 0 {
		cfStore.logger.Fatalf("Unable to build CF prefix bytes since no CF name is give")
	}
	return append([]byte(cf), separatorBytes...)
}
