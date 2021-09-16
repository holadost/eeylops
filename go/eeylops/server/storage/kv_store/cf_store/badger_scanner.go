package cf_store

import (
	"bytes"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
)

type BadgerScanner struct {
	store         *internalBadgerKVStore
	iter          *badger.Iterator
	txn           *badger.Txn
	localTxn      bool
	startKey      []byte
	cf            string
	reverse       bool
	fkBytes       []byte
	lkBytes       []byte
	cfPrefixBytes []byte
}

func newBadgerScanner(store *internalBadgerKVStore, cf string, startKey []byte, reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.store = store
	scanner.iter = nil
	scanner.reverse = reverse
	scanner.cf = cf
	scanner.startKey = BuildCFKey(scanner.cf, startKey)
	scanner.initialize()
	return scanner
}

func newBadgerScannerWithTxn(store *internalBadgerKVStore, txn *badger.Txn, cf string, startKey []byte,
	reverse bool) *BadgerScanner {
	scanner := new(BadgerScanner)
	scanner.store = store
	scanner.txn = txn
	scanner.localTxn = false // set local transaction to false since another global transaction has been passed.
	scanner.iter = nil
	scanner.cf = cf
	scanner.startKey = BuildCFKey(scanner.cf, startKey)
	scanner.reverse = reverse
	scanner.initialize()
	return scanner
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
	}
}

func (scanner *BadgerScanner) Valid() bool {
	if scanner.iter.ValidForPrefix(BuildCFPrefixBytes(scanner.cf)) {
		item := scanner.iter.Item()
		key := item.KeyCopy(nil)
		var cmpKey []byte
		if scanner.reverse {
			cmpKey = scanner.fkBytes
		} else {
			cmpKey = scanner.lkBytes
		}
		if bytes.Compare(key, cmpKey) == 0 {
			// We have reached the last key. The scanner is no longer valid.
			return false
		}
		return true
	}
	return false
}

func (scanner *BadgerScanner) Next() {
	scanner.iter.Next()
}

func (scanner *BadgerScanner) GetItem() (key []byte, val []byte, err error) {
	item := scanner.iter.Item()
	key = ExtractUserKey(scanner.cf, item.KeyCopy(nil))
	val, err = item.ValueCopy(nil)
	return
}

func (scanner *BadgerScanner) Seek(key []byte) {
	scanner.iter.Seek(BuildCFKeyWithCFPrefixBytes(scanner.cfPrefixBytes, key))
}

func (scanner *BadgerScanner) Close() {
	scanner.iter.Close()
	if scanner.localTxn {
		// Discard the transaction iff we created it. If it was passed from outside, skip discarding.
		scanner.txn.Discard()
	}
}
