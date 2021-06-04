package storage

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
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

// Get gets the value associated with the key.
func (kvStore *BadgerKVStore) Get(key []byte) ([]byte, error) {
	var val []byte
	if kvStore.closed {
		return val, fmt.Errorf("kv store is closed")
	}
	err := kvStore.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("unable to get key due to err: %w", err)
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("unable to copy value due to err: %w", err)
		}
		return nil
	})
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
		return fmt.Errorf("kv store is closed")
	}
	err := kvStore.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
	if err != nil {
		return fmt.Errorf("unable to put key due to err: %w", err)
	}
	return nil
}

// PutS puts a key value pair in the DB. If the key already exists, it would be updated.
func (kvStore *BadgerKVStore) PutS(key string, value string) error {
	return kvStore.Put([]byte(key), []byte(value))
}

// Delete deletes a key value pair from the DB.
func (kvStore *BadgerKVStore) Delete(key []byte) error {
	return kvStore.Put(key, []byte(""))
}

// DeleteS deletes a key value pair from the DB.
func (kvStore *BadgerKVStore) DeleteS(key string) error {
	return kvStore.Put([]byte(key), []byte(""))
}

// MultiGet gets the values associated with multiple keys.
func (kvStore *BadgerKVStore) MultiGet(keys [][]byte) (values [][]byte, errs []error) {
	if kvStore.closed {
		err := fmt.Errorf("kv store is closed")
		for ii := 0; ii < len(keys); ii++ {
			values = append(values, []byte{})
			errs = append(errs, err)
		}
		return
	}
	kvStore.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				errs = append(errs, err)
				values = append(values, nil)
				continue
			}
			tmpValue, err := item.ValueCopy(nil)
			if err != nil {
				errs = append(errs, err)
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
		return fmt.Errorf("kv store is closed")
	}
	wb := kvStore.db.NewWriteBatch()
	for ii := 0; ii < len(keys); ii++ {
		if err := wb.Set(keys[ii], values[ii]); err != nil {
			return fmt.Errorf("unable to put batch due to err: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("unable to put batch due to flush err: %w", err)
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
	var values [][]byte
	for ii := 0; ii < len(keys); ii++ {
		// Put empty string to delete the value. Badger will delete this eventually.
		values = append(values, []byte(""))
	}
	return kvStore.BatchPut(keys, values)
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

func generatePrefixKey(key []byte, prefix string) []byte {
	return []byte{}
}
