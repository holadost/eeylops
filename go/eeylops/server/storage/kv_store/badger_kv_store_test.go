package kv_store

import (
	"bytes"
	"eeylops/util/testutil"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
	"math/rand"
	"testing"
	"time"
)

func TestBadgerKVStore(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStore")
	testDir := testutil.CreateTestDir(t, "TestBadgerKVStore")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerKVStore(testDir, opts)
	batchSize := 10
	numIters := 20
	// Batch write values
	glog.Infof("Testing Batch Put")
	for iter := 0; iter < numIters; iter++ {
		if iter%5 == 0 {
			err := store.Close()
			if err != nil {
				glog.Fatalf("Hit an unexpected error while closing store. Err: %s", err.Error())
				return
			}
			store = NewBadgerKVStore(testDir, opts)
		}
		var keys [][]byte
		var values [][]byte
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			keys = append(keys, []byte(fmt.Sprintf("key-%03d", meraVal)))
			values = append(values, []byte(fmt.Sprintf("value-%03d", meraVal)))
		}
		err := store.BatchPut(keys, values)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
	}

	// Batch read and verify values
	glog.Infof("Testing BatchGet")
	for iter := 0; iter < numIters; iter++ {
		var keys [][]byte
		var values [][]byte
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			keys = append(keys, []byte(fmt.Sprintf("key-%03d", meraVal)))
		}
		values, errs := store.MultiGet(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != nil {
				glog.Fatalf("Hit an unexpected error: %s", errs[ii].Error())
				return
			}
			exVal := []byte(fmt.Sprintf("value-%03d", iter*batchSize+ii))
			val := values[ii]
			if bytes.Compare(exVal, val) != 0 {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", exVal, val)
				return
			}
		}
	}

	glog.Infof("Testing scanner")
	scanner := store.CreateScanner(nil, nil, false)
	// defer scanner.Close()
	for iter := 0; iter < numIters; iter++ {
		var keys []string
		var values []string
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			keys = append(keys, fmt.Sprintf("key-%03d", meraVal))
			values = append(values, fmt.Sprintf("value-%03d", meraVal))
		}
		cnt := 0
		for ; scanner.Valid(); scanner.Next() {
			key, item, err := scanner.GetItem()
			if err != nil {
				glog.Fatalf("Failed to scan store due to err: %s", err.Error())
			}
			if string(key) != keys[cnt] || string(item) != values[cnt] {
				glog.Fatalf("Mismatch: Expected Key: %s, Expected Val: %s, Got Key: %s, Got Val: %s",
					keys[cnt], values[cnt], string(key), string(item))
			}
			cnt += 1
			if cnt == batchSize {
				break
			}
		}
		if scanner.Valid() {
			scanner.Next()
		}
	}
	scanner.Close()

	glog.Infof("Testing put and get")
	singleKey := []byte("singleKey")
	singleVal := []byte("singleVal")
	err := store.Put(singleKey, singleVal)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while loading single key. Err: %s", err.Error())
		return
	}
	val, err := store.Get(singleKey)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while reading single key. Err: %s", err.Error())
		return
	}
	if bytes.Compare(val, singleVal) != 0 {
		glog.Fatalf("Value mismatch. Expected: %s, Got: %s", singleVal, val)
	}
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerKVStoreTxn(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreTxn")
	testDir := testutil.CreateTestDir(t, "TestBadgerKVStoreTxn")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	opts.SyncWrites = true
	store := NewBadgerKVStore(testDir, opts)

	// Testing discard transaction.
	glog.Infof("Testing transaction discards")
	txn := store.NewTransaction()
	failure_key := "failure_key"
	failure_value := "failure_value"
	var keys [][]byte
	var values [][]byte
	keys = append(keys, []byte(failure_key))
	values = append(values, []byte(failure_value))

	err := txn.BatchPut(keys, values)
	if err != nil {
		glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
		return
	}
	txn.Discard()

	_, errs := store.MultiGet(keys)
	if errs[0] != ErrKVStoreKeyNotFound {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}

	glog.Infof("Testing Batch Put")
	keys = nil
	values = nil
	toBeDeletedKey := "to_be_deleted_key"
	toBeDeletedValue := "to_be_deleted_value"
	keys = append(keys, []byte(toBeDeletedKey))
	values = append(values, []byte(toBeDeletedValue))
	txn = store.NewTransaction()
	err = txn.BatchPut(keys, values)
	if err != nil {
		glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
		return
	}
	if txn.Commit() != nil {
		glog.Fatalf("Failed to commit transaction due to err")
	}
	txn.Discard()

	fetchedVals, errs := store.MultiGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0]) != toBeDeletedValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0]))
	}

	// Test puts and delete in a single transaction and then discard it before committing.
	glog.Infof("Testing batch put and delete in transaction failure")
	successKey := "successKey"
	successValue := "successValue"
	var skeys [][]byte
	var svalues [][]byte
	skeys = append(skeys, []byte(successKey))
	svalues = append(svalues, []byte(successValue))
	testBatchPutAndDelete := func(txn Transaction) {
		glog.Infof("Testing Batch Put and batch delete")
		err = txn.BatchPut(skeys, svalues)
		if err != nil {
			glog.Fatalf("Batch put failed with error: %s", err)
		}
		if err := txn.BatchDelete(keys); err != nil {
			glog.Fatalf("Unable to batch delete due to err: %s", err.Error())
		}
	}
	txn = store.NewTransaction()
	testBatchPutAndDelete(txn)
	txn.Discard()
	// Check if values are correct.
	_, errs = store.MultiGet(skeys)
	if errs[0] != ErrKVStoreKeyNotFound {
		glog.Fatalf("Expected to not find toBeDeletedKey: %s", successKey)
	}
	fetchedVals, errs = store.MultiGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0]) != toBeDeletedValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0]))
	}

	// Test puts and delete in a single transaction and then commit it.
	glog.Infof("Testing batch put and delete in transaction success")
	txn = store.NewTransaction()
	testBatchPutAndDelete(txn)
	if err := txn.Commit(); err != nil {
		glog.Fatalf("Unable to commit transaction due to error")
	}
	txn.Discard()
	fetchedVals, errs = store.MultiGet(skeys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0]) != successValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0]))
	}
	_, errs = store.MultiGet(keys)
	if errs[0] != ErrKVStoreKeyNotFound {
		glog.Fatalf("Expected to not find toBeDeletedKey: %s", successKey)
	}

	glog.Infof("Testing Put and Delete and Get")
	successKey2 := "successKey2"
	successValue2 := "successValue2"
	keys = nil
	values = nil
	keys = append(keys, []byte(successKey2))
	values = append(values, []byte(successValue2))
	txn = store.NewTransaction()
	if err := txn.Put([]byte(successKey2), []byte(successValue2)); err != nil {
		glog.Fatalf("Unable to put values due to err: %v", err)
	}
	if err := txn.Delete([]byte(successKey)); err != nil {
		glog.Fatalf("Unable to delete value due to err: %v", err)
	}
	txn.Commit()
	txn.Discard()
	txn = store.NewTransaction()
	fetchedVals, errs = txn.MultiGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Unable to get value due to err: %v", errs[0])
	}
	if string(fetchedVals[0]) != successValue2 {
		glog.Fatalf("Value mismatch. Expected: %s, got: %s", successValue2, string(fetchedVals[0]))
	}
	_, err = txn.Get([]byte(successKey))
	if err != ErrKVStoreKeyNotFound {
		glog.Fatalf("Got unexpected error: %v", err)
	}
	txn.Discard()
}

func TestBadgerKVStore_BatchPut(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStore_BatchPut")
	testDir := testutil.CreateTestDir(t, "TestBadgerKVStore_BatchPut")
	opts := badger.DefaultOptions(testDir)
	opts.NumMemtables = 3
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	store := NewBadgerKVStore(testDir, opts)
	batchSize := 10
	numIters := 1000
	// Batch write values
	token := make([]byte, 1024*1024)
	rand.Read(token)
	glog.Infof("Testing Batch Put")
	start := time.Now()
	var values [][]byte
	for ii := 0; ii < batchSize; ii++ {
		values = append(values, token)
	}
	for iter := 0; iter < numIters; iter++ {
		var keys [][]byte
		for ii := 0; ii < batchSize; ii++ {
			key := make([]byte, 16)
			rand.Read(key)
			keys = append(keys, key)
		}
		err := store.BatchPut(keys, values)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
	}
	elapsed := time.Since(start)
	glog.Infof("Total time: %v, average time: %v", elapsed, elapsed/time.Duration(numIters))
}
