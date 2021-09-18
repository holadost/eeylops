package cf_store

import (
	"bytes"
	"eeylops/server/storage/kv_store"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
)

// BadgerScanner allows the user to scan a column family from the KV store.
type BadgerScanner struct {
	store         *internalBadgerKVStore // Internal store.
	iter          *badger.Iterator       // Badger iterator.
	txn           *badger.Txn            // Badger transaction.
	localTxn      bool                   // Flag to indicate whether scanner is using a local transaction.
	startKey      []byte                 // Start key for scan.
	cf            string                 // Name of the column family.
	reverse       bool                   // Reverse flag.
	fkBytes       []byte                 // First CF marker key.
	lkBytes       []byte                 // Last CF marker key.
	cfPrefixBytes []byte                 // CF prefix bytes.
	currEntry     *CFStoreEntry          // Current entry.
	currErr       error                  // Current error.
}

func newBadgerScanner(store *internalBadgerKVStore, cf string, startKey []byte, reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.store = store
	scanner.iter = nil
	scanner.reverse = reverse
	scanner.cf = cf
	if startKey == nil || len(startKey) == 0 {
		scanner.startKey = nil
	} else {
		scanner.startKey = BuildCFKey(scanner.cf, startKey)
	}
	scanner.initialize()
	return scanner
}

func newBadgerScannerWithTxn(store *internalBadgerKVStore, txn *badger.Txn, cf string, startKey []byte,
	reverse bool) (*BadgerScanner, error) {
	scanner := new(BadgerScanner)
	scanner.store = store
	scanner.txn = txn
	scanner.localTxn = false // set local transaction to false since another global transaction has been passed.
	scanner.iter = nil
	scanner.cf = cf
	scanner.reverse = reverse
	if startKey == nil || len(startKey) == 0 {
		scanner.startKey = nil
	} else {
		scanner.startKey = BuildCFKey(scanner.cf, startKey)
	}
	scanner.initialize()
	return scanner, nil
}

func (scanner *BadgerScanner) initialize() {
	if scanner.txn == nil {
		if scanner.store == nil {
			glog.Fatalf("Either db or badgerTxn must have been provided")
		}
		scanner.localTxn = true
		scanner.txn = scanner.store.db.NewTransaction(false)
	}
	scanner.cfPrefixBytes = BuildCFPrefixBytes(scanner.cf)
	scanner.fkBytes = BuildFirstCFKey(scanner.cf)
	scanner.lkBytes = BuildLastCFKey(scanner.cf)
	opts := badger.DefaultIteratorOptions
	opts.Reverse = scanner.reverse
	scanner.iter = scanner.txn.NewIterator(opts)
	scanner.Rewind()
}

func (scanner *BadgerScanner) Rewind() {
	scanner.iter.Rewind()
	if len(scanner.startKey) > 0 {
		scanner.iter.Seek(scanner.startKey)
	} else {
		if scanner.reverse {
			scanner.iter.Seek(scanner.lkBytes)
		} else {
			scanner.iter.Seek(scanner.fkBytes)
		}
		// Skip CF marker key.
		scanner.iter.Next()
	}
	scanner.mayBeFetchNextItem()
}

func (scanner *BadgerScanner) Valid() bool {
	if scanner.currEntry == nil {
		return false
	}
	return true
}

func (scanner *BadgerScanner) Next() {
	scanner.currEntry = nil
	scanner.currErr = nil
	scanner.iter.Next()
	if !scanner.iter.ValidForPrefix(scanner.cfPrefixBytes) {
		return
	}
	scanner.mayBeFetchNextItem()
}

func (scanner *BadgerScanner) GetItem() (key []byte, val []byte, err error) {
	if scanner.currEntry != nil {
		return scanner.currEntry.Key, scanner.currEntry.Value, scanner.currErr
	}
	return nil, nil, nil
}

func (scanner *BadgerScanner) Seek(key []byte) {
	scanner.iter.Seek(BuildCFKeyWithCFPrefixBytes(scanner.cfPrefixBytes, key))
	scanner.mayBeFetchNextItem()
}

func (scanner *BadgerScanner) Close() {
	scanner.iter.Close()
	if scanner.localTxn {
		// Discard the transaction iff we created it. If it was passed from outside, skip discarding.
		scanner.txn.Discard()
	}
}

func (scanner *BadgerScanner) mayBeFetchNextItem() {
	scanner.currEntry = nil
	scanner.currErr = nil
	item := scanner.iter.Item()
	key := item.KeyCopy(nil)
	var cmpKey []byte
	if scanner.reverse {
		cmpKey = scanner.fkBytes
	} else {
		cmpKey = scanner.lkBytes
	}
	if bytes.Compare(key, cmpKey) == 0 {
		// We have reached the last key. Nothing more to fetch.
		return
	}
	var entry CFStoreEntry
	scanner.currEntry = &entry
	scanner.currErr = nil
	entry.Key = ExtractUserKey(scanner.cf, key)
	entry.ColumnFamily = scanner.cf
	val, err := item.ValueCopy(nil)
	if err != nil {
		glog.Errorf("Unable to read value due to err: %v", err)
		scanner.currErr = kv_store.ErrKVStoreBackend
		return
	}
	entry.Value = val
}
