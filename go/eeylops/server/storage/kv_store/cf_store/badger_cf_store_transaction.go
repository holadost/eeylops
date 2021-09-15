package cf_store

import (
	"github.com/dgraph-io/badger/v2"
)

type BadgerCFStoreTransaction struct {
	store     *internalBadgerKVStore
	badgerTxn *badger.Txn
}

func newBadgerCFStoreTransaction(store *internalBadgerKVStore) *BadgerCFStoreTransaction {
	btxn := &BadgerCFStoreTransaction{
		store:     store,
		badgerTxn: store.NewTransaction(),
	}
	return btxn
}

func (txn *BadgerCFStoreTransaction) Get(key *CFStoreKey) (*CFStoreEntry, error) {
	return txn.store.GetWithTxn(txn.badgerTxn, key)
}

func (txn *BadgerCFStoreTransaction) Put(entry *CFStoreEntry) error {
	return txn.store.PutWithTxn(txn.badgerTxn, entry)
}

func (txn *BadgerCFStoreTransaction) Delete(key *CFStoreKey) error {
	return txn.store.DeleteWithTxn(txn.badgerTxn, key)
}

func (txn *BadgerCFStoreTransaction) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*CFStoreEntry, nextKey []byte, retErr error) {
	return txn.store.ScanWithTxn(txn.badgerTxn, cf, startKey, numValues, scanSizeBytes, reverse)
}

func (txn *BadgerCFStoreTransaction) BatchGet(keys []*CFStoreKey) (values []*CFStoreEntry, errs []error) {
	return txn.store.BatchGetWithTxn(txn.badgerTxn, keys)
}

func (txn *BadgerCFStoreTransaction) BatchPut(entries []*CFStoreEntry) error {
	return txn.store.BatchPutWithTxn(txn.badgerTxn, entries)
}

func (txn *BadgerCFStoreTransaction) BatchDelete(keys []*CFStoreKey) error {
	return txn.store.BatchDeleteWithTxn(txn.badgerTxn, keys)
}

func (txn *BadgerCFStoreTransaction) NewScanner(cf string, startKey []byte, reverse bool) Scanner {
	return newBadgerScannerWithTxn(txn.store, txn.badgerTxn, cf, startKey, reverse)
}

func (txn *BadgerCFStoreTransaction) Commit() error {
	return txn.badgerTxn.Commit()
}
func (txn *BadgerCFStoreTransaction) Discard() {
	txn.badgerTxn.Discard()
}
