package badger_kv_store

import (
	"eeylops/server/storage/kv_store"
	"github.com/dgraph-io/badger/v2"
)

type BadgerKVStoreTransaction struct {
	store     *internalBadgerKVStore
	badgerTxn *badger.Txn
}

func newBadgerKVStoreTransaction(store *internalBadgerKVStore) *BadgerKVStoreTransaction {
	btxn := &BadgerKVStoreTransaction{
		store:     store,
		badgerTxn: store.NewTransaction(),
	}
	return btxn
}

func (txn *BadgerKVStoreTransaction) Get(key *kv_store.KVStoreKey) (*kv_store.KVStoreEntry, error) {
	return txn.store.GetWithTxn(txn.badgerTxn, key)
}

func (txn *BadgerKVStoreTransaction) Put(entry *kv_store.KVStoreEntry) error {
	return txn.store.PutWithTxn(txn.badgerTxn, entry)
}

func (txn *BadgerKVStoreTransaction) Delete(key *kv_store.KVStoreKey) error {
	return txn.store.DeleteWithTxn(txn.badgerTxn, key)
}

func (txn *BadgerKVStoreTransaction) Scan(cf string, startKey []byte, numValues int, scanSizeBytes int, reverse bool) (
	entries []*kv_store.KVStoreEntry, nextKey []byte, retErr error) {
	return txn.store.ScanWithTxn(txn.badgerTxn, cf, startKey, numValues, scanSizeBytes, reverse)
}

func (txn *BadgerKVStoreTransaction) BatchGet(keys []*kv_store.KVStoreKey) (values []*kv_store.KVStoreEntry,
	errs []error) {
	return txn.store.BatchGetWithTxn(txn.badgerTxn, keys)
}

func (txn *BadgerKVStoreTransaction) BatchPut(entries []*kv_store.KVStoreEntry) error {
	return txn.store.BatchPutWithTxn(txn.badgerTxn, entries)
}

func (txn *BadgerKVStoreTransaction) BatchDelete(keys []*kv_store.KVStoreKey) error {
	return txn.store.BatchDeleteWithTxn(txn.badgerTxn, keys)
}

func (txn *BadgerKVStoreTransaction) NewScanner(cf string, startKey []byte, reverse bool) (kv_store.Scanner, error) {
	return newBadgerScannerWithTxn(txn.store, txn.badgerTxn, cf, startKey, reverse)
}

func (txn *BadgerKVStoreTransaction) Commit() error {
	return txn.badgerTxn.Commit()
}
func (txn *BadgerKVStoreTransaction) Discard() {
	txn.badgerTxn.Discard()
}
