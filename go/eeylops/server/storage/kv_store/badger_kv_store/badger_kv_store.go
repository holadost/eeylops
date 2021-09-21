package badger_kv_store

import (
	"bytes"
	"eeylops/server/storage/kv_store"
	"eeylops/util/logging"
	"github.com/dgraph-io/badger/v2"
	"os"
	"path/filepath"
	"sync"
)

const kMaxKeyLength = 60000
const kInternalMaxKeyLength = kMaxKeyLength + 4000
const kMaxColumnFamilyLen = 256
const kDefaultCFName = "default"
const kAllColumnFamiliesCFName = "cf"

var kSeparatorBytes = []byte("::")
var kDefaultColumnNamePrefixBytes = append([]byte("0"), kSeparatorBytes...)

type BadgerKVStore struct {
	internalStore *internalBadgerKVStore
}

func NewBadgerKVStore(rootDir string, opts badger.Options, logger *logging.PrefixLogger) *BadgerKVStore {
	if logger == nil {
		logger = logging.NewPrefixLogger("BadgerKVStore")
		if opts.Logger == nil {
			opts.Logger = logger
		}
	}
	return newBadgerKVStore(rootDir, logger, opts)
}

func newBadgerKVStore(rootDir string, logger *logging.PrefixLogger, opts badger.Options) *BadgerKVStore {
	kvStore := new(internalBadgerKVStore)
	kvStore.rootDir = rootDir
	kvStore.logger = logger
	if err := os.MkdirAll(kvStore.rootDir, 0774); err != nil {
		kvStore.logger.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	opts.Logger = logging.NewPrefixLoggerWithParentAndDepth("", logger, 1)
	kvStore.initialize(opts)
	return &BadgerKVStore{internalStore: kvStore}
}

func (kvStore *BadgerKVStore) GetDataDir() string {
	return kvStore.internalStore.GetDataDir()
}

// Size returns the size of the KV store in bytes.
func (kvStore *BadgerKVStore) Size() int64 {
	return kvStore.internalStore.Size()
}

// Close the KV store.
func (kvStore *BadgerKVStore) Close() error {
	return kvStore.internalStore.Close()
}

// AddColumnFamily adds the column family(if it does not exist) to the KV store.
func (kvStore *BadgerKVStore) AddColumnFamily(cf string) error {
	return kvStore.internalStore.AddColumnFamily(cf)
}

func (kvStore *BadgerKVStore) Get(key *kv_store.KVStoreKey) (*kv_store.KVStoreEntry, error) {
	return kvStore.internalStore.Get(key)
}

func (kvStore *BadgerKVStore) Put(entry *kv_store.KVStoreEntry) error {
	return kvStore.internalStore.Put(entry)
}

func (kvStore *BadgerKVStore) Delete(key *kv_store.KVStoreKey) error {
	return kvStore.internalStore.Delete(key)
}

func (kvStore *BadgerKVStore) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*kv_store.KVStoreEntry, nextKey []byte, retErr error) {
	return kvStore.internalStore.Scan(cf, startKey, numValues, scanSizeBytes, reverse)
}

func (kvStore *BadgerKVStore) BatchGet(keys []*kv_store.KVStoreKey) (values []*kv_store.KVStoreEntry, errs []error) {
	return kvStore.internalStore.BatchGet(keys)
}

func (kvStore *BadgerKVStore) BatchPut(entries []*kv_store.KVStoreEntry) error {
	return kvStore.internalStore.BatchPut(entries)
}

func (kvStore *BadgerKVStore) BatchDelete(keys []*kv_store.KVStoreKey) error {
	return kvStore.internalStore.BatchDelete(keys)
}

func (kvStore *BadgerKVStore) NewScanner(cf string, startKey []byte, reverse bool) (kv_store.Scanner, error) {
	if len(cf) == 0 {
		cf = kDefaultCFName
	}
	if !IsColumnFamilyNameValid(cf) {
		return nil, kv_store.ErrKVStoreInvalidColumnFamilyName
	}
	return newBadgerScanner(kvStore.internalStore, cf, startKey, reverse), nil
}

func (kvStore *BadgerKVStore) NewTransaction() kv_store.Transaction {
	return newBadgerKVStoreTransaction(kvStore.internalStore)
}

func (kvStore *BadgerKVStore) GC() error {
	return kvStore.internalStore.GC()
}

/*************************************************** INTERNAL CF STORE ************************************************/
type internalBadgerKVStore struct {
	db        *badger.DB
	rootDir   string
	closed    bool
	logger    *logging.PrefixLogger
	cfMap     map[string]struct{}
	cfMapLock sync.RWMutex
}

func (ikvStore *internalBadgerKVStore) initialize(opts badger.Options) {
	ikvStore.logger.Infof("Initializing badger KV store located at: %s", ikvStore.rootDir)
	var err error
	ikvStore.cfMap = make(map[string]struct{})
	ikvStore.db, err = badger.Open(opts)
	ikvStore.closed = false
	if err != nil {
		ikvStore.logger.Fatalf("Unable to open consumer store due to err: %s", err.Error())
	}

	// Check if the default column families exist and if not, create them.
	err = ikvStore.db.View(func(txn *badger.Txn) error {
		_, txnerr := txn.Get(BuildFirstCFKey(kDefaultCFName))
		if txnerr != nil {
			return txnerr
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			updateErr := ikvStore.db.Update(func(txn *badger.Txn) error {
				txnerr := txn.Set(BuildFirstCFKey(kDefaultCFName), kSeparatorBytes)
				if txnerr != nil {
					return txnerr
				}
				txnerr = txn.Set(BuildLastCFKey(kDefaultCFName), kSeparatorBytes)
				if txnerr != nil {
					return txnerr
				}
				txnerr = txn.Set(BuildFirstCFKey(kAllColumnFamiliesCFName), kSeparatorBytes)
				if txnerr != nil {
					return txnerr
				}
				txnerr = txn.Set(BuildLastCFKey(kAllColumnFamiliesCFName), kSeparatorBytes)
				if txnerr != nil {
					return txnerr
				}
				return nil
			})
			if updateErr != nil {
				ikvStore.logger.Fatalf("Unable to create default column families due to err: %v", updateErr)
			}
		} else {
			ikvStore.logger.Fatalf("Unexpected error while checking if default column families exist: %v", err)
		}
	}

	// Load column family names into memory.
	txn := ikvStore.db.NewTransaction(false)
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	itr.Seek(BuildFirstCFKey(kAllColumnFamiliesCFName))
	if itr.Valid() {
		itr.Next()
	}
	for ; itr.ValidForPrefix(BuildCFPrefixBytes(kAllColumnFamiliesCFName)); itr.Next() {
		item := itr.Item()
		key := item.KeyCopy(nil)
		if bytes.Compare(key, BuildLastCFKey(kAllColumnFamiliesCFName)) == 0 {
			// We have reached the end. Return
			return
		}
		cfName := string(ExtractUserKey(kAllColumnFamiliesCFName, key))
		ikvStore.cfMap[cfName] = struct{}{}
	}
	ikvStore.logger.Infof("Found %d column families in KV store", len(ikvStore.cfMap))
}

func (ikvStore *internalBadgerKVStore) GC() error {
	err := ikvStore.db.RunValueLogGC(0.5)
	if err != nil {
		// ErrNoRewrite is returned if no space was reclaimed. ErrRejected is returned if a ValueLogGC is already
		// going on. Both can be safely ignored.
		if (err == badger.ErrNoRewrite) || (err == badger.ErrRejected) {
			return nil
		}
		ikvStore.logger.Errorf("Unable to perform value log GC due to err: %v", err)
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

func (ikvStore *internalBadgerKVStore) GetDataDir() string {
	return ikvStore.rootDir
}

// Size returns the size of the KV store in bytes.
func (ikvStore *internalBadgerKVStore) Size() int64 {
	var lsmSize, vlogSize int64
	err := filepath.Walk(ikvStore.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ext := filepath.Ext(path)
		switch ext {
		case ".sst":
			lsmSize += info.Size()
		case ".vlog":
			vlogSize += info.Size()
		}
		return nil
	})
	if err != nil {
		ikvStore.logger.Fatalf("Unable to get size of database due to err: %v", err)
	}
	return lsmSize + vlogSize
}

// AddColumnFamily adds the column family(if it does not exist) to the KV store.
func (ikvStore *internalBadgerKVStore) AddColumnFamily(cf string) error {
	if cf == kDefaultCFName || cf == kAllColumnFamiliesCFName {
		return kv_store.ErrKVStoreReservedColumnFamilyNames
	}
	if !IsColumnFamilyNameValid(cf) {
		return kv_store.ErrKVStoreInvalidColumnFamilyName
	}
	if ikvStore.doesCFExist(cf) {
		return kv_store.ErrKVStoreColumnFamilyExists
	}
	return ikvStore.addColumnFamilyInternal(cf)
}

// Get gets the value associated with the key.
func (ikvStore *internalBadgerKVStore) Get(key *kv_store.KVStoreKey) (*kv_store.KVStoreEntry, error) {
	if ikvStore.closed {
		return nil, kv_store.ErrKVStoreClosed
	}
	txn := ikvStore.db.NewTransaction(false)
	defer txn.Discard()
	return ikvStore.GetWithTxn(txn, key)
}

// GetWithTxn gets the value associated with the key. This method requires a transaction to be passed. Transaction
// commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) GetWithTxn(txn *badger.Txn, key *kv_store.KVStoreKey) (*kv_store.KVStoreEntry, error) {
	if ikvStore.closed {
		return nil, kv_store.ErrKVStoreClosed
	}
	var val []byte
	if !IsKeyValid(key.Key, false) {
		return nil, kv_store.ErrKVStoreInvalidKey
	}
	cf, err := ikvStore.getCFIfExists(key.ColumnFamily)
	if err != nil {
		return nil, err
	}
	item, err := txn.Get(BuildCFKey(cf, key.Key))
	var entry kv_store.KVStoreEntry
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
func (ikvStore *internalBadgerKVStore) Put(entry *kv_store.KVStoreEntry) error {
	if ikvStore.closed {
		return kv_store.ErrKVStoreClosed
	}
	txn := ikvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := ikvStore.PutWithTxn(txn, entry)
	if err == nil {
		if cErr := txn.Commit(); cErr != nil {
			if cErr == badger.ErrConflict {
				return kv_store.ErrKVStoreConflict
			}
			ikvStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
			return kv_store.ErrKVStoreBackend
		}
		return nil
	}
	return err
}

// PutWithTxn puts a key value pair in the DB. If the key already exists, it would be updated. This method
// requires a transaction to be passed. Transaction commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) PutWithTxn(txn *badger.Txn, entry *kv_store.KVStoreEntry) error {
	if ikvStore.closed {
		ikvStore.logger.Errorf("KV store is closed")
		return kv_store.ErrKVStoreClosed
	}
	if !IsKeyValid(entry.Key, false) {
		return kv_store.ErrKVStoreInvalidKey
	}
	cf, err := ikvStore.getCFIfExists(entry.ColumnFamily)
	if err != nil {
		return err
	}
	err = txn.Set(BuildCFKey(cf, entry.Key), entry.Value)
	if err != nil {
		ikvStore.logger.Errorf("Unable to put key: %v due to err: %s", entry.Key, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// Delete deletes a key value pair from the DB.
func (ikvStore *internalBadgerKVStore) Delete(key *kv_store.KVStoreKey) error {
	if ikvStore.closed {
		return kv_store.ErrKVStoreClosed
	}
	txn := ikvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := ikvStore.DeleteWithTxn(txn, key)
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		if err == badger.ErrConflict {
			return kv_store.ErrKVStoreConflict
		}
		ikvStore.logger.Errorf("Unable to commit transaction due to err: %s", err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// DeleteWithTxn removes the given key value pair from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) DeleteWithTxn(txn *badger.Txn, key *kv_store.KVStoreKey) error {
	if ikvStore.closed {
		return kv_store.ErrKVStoreClosed
	}
	if !IsKeyValid(key.Key, false) {
		return kv_store.ErrKVStoreInvalidKey
	}
	cf, err := ikvStore.getCFIfExists(key.ColumnFamily)
	if err != nil {
		return err
	}
	err = txn.Delete(BuildCFKey(cf, key.Key))
	if err != nil {
		ikvStore.logger.Errorf("Unable to delete key: %v due to err: %s", key, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// Scan scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
func (ikvStore *internalBadgerKVStore) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*kv_store.KVStoreEntry, nextKey []byte, retErr error) {
	if ikvStore.closed {
		return nil, nil, kv_store.ErrKVStoreClosed
	}
	txn := ikvStore.db.NewTransaction(false)
	defer txn.Discard()
	return ikvStore.ScanWithTxn(txn, cf, startKey, numValues, scanSizeBytes, reverse)
}

// ScanWithTxn scans the DB in ascending order from the given start key. if numValues is < 0, the entire DB is scanned.
// Transaction commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) ScanWithTxn(txn *badger.Txn, cf string, startKey []byte, numValues int,
	scanSizeBytes int, reverse bool) (entries []*kv_store.KVStoreEntry, nextKey []byte, retErr error) {
	if ikvStore.closed {
		return nil, nil, kv_store.ErrKVStoreClosed
	}
	if !IsKeyValid(startKey, true) {
		return nil, nil, kv_store.ErrKVStoreInvalidKey
	}
	actualCf, err := ikvStore.getCFIfExists(cf)
	if err != nil {
		return nil, nil, err
	}
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	itr := txn.NewIterator(opts)
	defer itr.Close()

	// Seek to the correct entry.
	firstCFKey := BuildFirstCFKey(actualCf)
	lastCFKey := BuildLastCFKey(actualCf)
	if (startKey == nil) || len(startKey) == 0 {
		itr.Rewind()
		if !reverse {
			itr.Seek(firstCFKey)
		} else {
			itr.Seek(lastCFKey)
		}

	} else {
		itr.Seek(BuildCFKey(actualCf, startKey))
	}
	currSizeBytes := int64(0)
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		valSize := item.ValueSize()
		currSizeBytes += valSize
		fullKey := item.KeyCopy(nil)
		if !reverse {
			if bytes.Compare(lastCFKey, fullKey) == 0 {
				return entries, nil, nil
			}
		} else {
			if bytes.Compare(firstCFKey, fullKey) == 0 {
				return entries, nil, nil
			}
		}
		key := ExtractUserKey(actualCf, fullKey)
		val, err := item.ValueCopy(nil)
		if err != nil {
			ikvStore.logger.Errorf("Unable to scan KV store due to err: %s", err)
			return nil, nil, err
		}
		if (scanSizeBytes > 0) && (currSizeBytes+valSize > int64(scanSizeBytes)) {
			return entries, key, nil
		}
		if numValues >= 0 && len(entries) == numValues {
			return entries, key, nil
		}
		entries = append(entries, &kv_store.KVStoreEntry{
			Key:          key,
			Value:        val,
			ColumnFamily: actualCf,
		})
	}
	// We have reached the end of the DB and there are no more keys.
	return entries, nil, nil
}

// BatchGet fetches multiple keys from the DB.
func (ikvStore *internalBadgerKVStore) BatchGet(keys []*kv_store.KVStoreKey) (values []*kv_store.KVStoreEntry, errs []error) {
	if ikvStore.closed {
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, &kv_store.KVStoreEntry{})
			errs = append(errs, kv_store.ErrKVStoreClosed)
		}
		return
	}
	txn := ikvStore.db.NewTransaction(false)
	defer txn.Discard()
	return ikvStore.BatchGetWithTxn(txn, keys)
}

// BatchGetWithTxn fetches multiple keys from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) BatchGetWithTxn(txn *badger.Txn, keys []*kv_store.KVStoreKey) (values []*kv_store.KVStoreEntry,
	errs []error) {
	if ikvStore.closed {
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, &kv_store.KVStoreEntry{})
			errs = append(errs, kv_store.ErrKVStoreClosed)
		}
		return
	}
	for _, key := range keys {
		if !IsKeyValid(key.Key, false) {
			values = append(values, nil)
			errs = append(errs, kv_store.ErrKVStoreInvalidKey)
			continue
		}
		actualCf, err := ikvStore.getCFIfExists(key.ColumnFamily)
		if err != nil {
			values = append(values, nil)
			errs = append(errs, err)
			continue
		}

		item, err := txn.Get(BuildCFKey(actualCf, key.Key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				errs = append(errs, kv_store.ErrKVStoreKeyNotFound)
			} else {
				ikvStore.logger.Errorf("Unable to get key: %v due to err: %s", key, err.Error())
				errs = append(errs, kv_store.ErrKVStoreGeneric)
			}
			values = append(values, nil)
			continue
		}
		tmpValue, err := item.ValueCopy(nil)
		if err != nil {
			ikvStore.logger.Errorf("Unable to parse value for key: %v due to err: %s", key, err.Error())
			errs = append(errs, kv_store.ErrKVStoreGeneric)
			values = append(values, nil)
			continue
		}
		values = append(values, &kv_store.KVStoreEntry{
			Key:          key.Key,
			Value:        tmpValue,
			ColumnFamily: key.ColumnFamily,
		})
		errs = append(errs, nil)
	}
	return
}

// BatchPut sets/updates multiple key value pairs in the DB.
func (ikvStore *internalBadgerKVStore) BatchPut(entries []*kv_store.KVStoreEntry) error {
	if ikvStore.closed {
		return kv_store.ErrKVStoreBackend
	}
	wb := ikvStore.db.NewWriteBatch()
	for ii := 0; ii < len(entries); ii++ {
		cf, err := ikvStore.getCFIfExists(entries[ii].ColumnFamily)
		if err != nil {
			return err
		}
		key := BuildCFKey(cf, entries[ii].Key)
		if err := wb.Set(key, entries[ii].Value); err != nil {
			ikvStore.logger.Errorf("Unable to perform batch put due to err: %s", err.Error())
			return kv_store.ErrKVStoreBackend
		}
	}
	if err := wb.Flush(); err != nil {
		ikvStore.logger.Errorf("Unable to perform flush after batch put due to err: %s", err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// BatchPutWithTxn sets multiple key value pairs in the DB. This method needs a transaction to be passed. Transaction
// commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) BatchPutWithTxn(txn *badger.Txn, entries []*kv_store.KVStoreEntry) error {
	for _, entry := range entries {
		if !IsKeyValid(entry.Key, false) {
			return kv_store.ErrKVStoreInvalidKey
		}
		actualCf, err := ikvStore.getCFIfExists(entry.ColumnFamily)
		if err != nil {
			return err
		}
		if err := txn.Set(BuildCFKey(actualCf, entry.Key), entry.Value); err != nil {
			ikvStore.logger.Errorf("Unable to set keys due to err: %s", err.Error())
			return kv_store.ErrKVStoreBackend
		}
	}
	return nil
}

// BatchDelete deletes multiple key value pairs from the DB.
func (ikvStore *internalBadgerKVStore) BatchDelete(keys []*kv_store.KVStoreKey) error {
	if ikvStore.closed {
		ikvStore.logger.Errorf("KV store is closed")
		return kv_store.ErrKVStoreClosed
	}
	txn := ikvStore.db.NewTransaction(true)
	defer txn.Discard()
	err := ikvStore.BatchDeleteWithTxn(txn, keys)
	if err != nil {
		return err
	}
	cerr := txn.Commit()
	if cerr != nil {
		if cerr == badger.ErrConflict {
			return kv_store.ErrKVStoreConflict
		}
		ikvStore.logger.Errorf("Unable to commit batch delete transaction due to err: %s", cerr.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

// BatchDeleteWithTxn deletes multiple key value pairs from the DB. This method requires a transaction to be passed.
// Transaction commits and rollbacks must be handled by the caller.
func (ikvStore *internalBadgerKVStore) BatchDeleteWithTxn(txn *badger.Txn, keys []*kv_store.KVStoreKey) error {
	var err error
	for _, key := range keys {
		if !IsKeyValid(key.Key, false) {
			return kv_store.ErrKVStoreInvalidKey
		}
		var actualCf string
		actualCf, err = ikvStore.getCFIfExists(key.ColumnFamily)
		if err != nil {
			return err
		}
		err = txn.Delete(BuildCFKey(actualCf, key.Key))
		if err != nil {
			break
		}
	}
	if err != nil {
		ikvStore.logger.Errorf("Unable to batch delete keys: %v due to err: %s", keys, err.Error())
		return kv_store.ErrKVStoreBackend
	}
	return nil
}

func (ikvStore *internalBadgerKVStore) NewTransaction() *badger.Txn {
	return ikvStore.db.NewTransaction(true)
}

// Close the DB.
func (ikvStore *internalBadgerKVStore) Close() error {
	ikvStore.logger.Infof("Closing KV store located at: %s", ikvStore.rootDir)
	if ikvStore.closed {
		return nil
	}
	err := ikvStore.db.Close()
	ikvStore.closed = true
	ikvStore.db = nil
	return err
}

func (ikvStore *internalBadgerKVStore) addColumnFamilyInternal(cf string) error {
	ikvStore.logger.Infof("Adding column family: %s", cf)
	err := ikvStore.db.Update(func(txn *badger.Txn) error {
		txnerr := txn.Set(BuildFirstCFKey(cf), kSeparatorBytes)
		if txnerr != nil {
			return txnerr
		}
		txnerr = txn.Set(BuildLastCFKey(cf), kSeparatorBytes)
		if txnerr != nil {
			return txnerr
		}
		txnerr = txn.Set(BuildCFKey(kAllColumnFamiliesCFName, []byte(cf)), kSeparatorBytes)
		if txnerr != nil {
			return txnerr
		}

		return nil
	})
	if err != nil {
		if err == badger.ErrConflict {
			return kv_store.ErrKVStoreConflict
		}
		ikvStore.logger.Errorf("Unable to add column family due to badger err: %v", err)
		return kv_store.ErrKVStoreBackend
	}
	ikvStore.cfMapLock.Lock()
	defer ikvStore.cfMapLock.Unlock()
	ikvStore.cfMap[cf] = struct{}{}
	return nil
}

func (ikvStore *internalBadgerKVStore) doesCFExist(cf string) bool {
	ikvStore.cfMapLock.RLock()
	defer ikvStore.cfMapLock.RUnlock()
	_, exists := ikvStore.cfMap[cf]
	return exists
}

// sanitizeCFName returns the sanitized CF name. It also returns a bool indicating whether the sanitization was
// successful or not!
func (ikvStore *internalBadgerKVStore) sanitizeCFName(name string) (string, error) {
	var cf string
	if name == "" {
		return kDefaultCFName, nil
	} else {
		cf = name
	}
	if IsColumnFamilyNameValid(cf) {
		return name, nil
	}
	return "", kv_store.ErrKVStoreInvalidColumnFamilyName
}

// sanitizeCFName returns the sanitized CF name. It also returns a bool indicating whether the sanitization was
// successful or not!
func (ikvStore *internalBadgerKVStore) getCFIfExists(name string) (string, error) {
	cf, err := ikvStore.sanitizeCFName(name)
	if err != nil {
		return "", err
	}
	if cf == kDefaultCFName {
		return cf, nil
	}
	if ikvStore.doesCFExist(cf) {
		return cf, nil
	}
	return "", kv_store.ErrKVStoreColumnFamilyNotFound
}
