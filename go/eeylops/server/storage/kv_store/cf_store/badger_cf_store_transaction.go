package cf_store

import (
	"github.com/dgraph-io/badger/v2"
)

type BadgerCFStoreTransaction struct {
	store *internalBadgerKVStore
	txn   *badger.Txn
}

func newBadgerCFStoreTransaction(store *internalBadgerKVStore) *BadgerCFStoreTransaction {
	btxn := &BadgerCFStoreTransaction{
		store: store,
		txn:   store.NewTransaction(),
	}
	return btxn
}

func (txn *BadgerCFStoreTransaction) Get(key *KVStoreKey) (*KVStoreEntry, error) {
	return txn.store.GetWithTxn(txn.txn, key)
}

func (txn *BadgerCFStoreTransaction) Put(entry *KVStoreEntry) error {
	return txn.store.PutWithTxn(txn.txn, entry)
}

func (txn *BadgerCFStoreTransaction) Delete(key *KVStoreKey) error {
	return txn.store.DeleteWithTxn(txn.txn, key)
}

func (txn *BadgerCFStoreTransaction) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*KVStoreEntry, nextKey []byte, retErr error) {
	return txn.store.ScanWithTxn(txn.txn, cf, startKey, numValues, scanSizeBytes, reverse)
}

func (txn *BadgerCFStoreTransaction) MultiGet(keys []*KVStoreKey) (values []*KVStoreEntry, errs []error) {
	return txn.store.BatchGetWithTxn(txn.txn, keys)
}

func (txn *BadgerCFStoreTransaction) BatchPut(entries []*KVStoreEntry) error {
	return txn.store.BatchPutWithTxn(txn.txn, entries)
}

func (txn *BadgerCFStoreTransaction) BatchDelete(keys []*KVStoreKey) error {
	return txn.store.BatchDeleteWithTxn(txn.txn, keys)
}

func (txn *BadgerCFStoreTransaction) Commit() error {
	return txn.txn.Commit()
}
func (txn *BadgerCFStoreTransaction) Discard() {
	txn.txn.Discard()
}
