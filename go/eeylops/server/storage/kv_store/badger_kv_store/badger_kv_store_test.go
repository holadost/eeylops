package badger_kv_store

import (
	"bytes"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/testutil"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBadgerKVStoreDefault(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreDefault")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreDefault")
	doStoreSingleActorIO(testDir, "")
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerKVStoreAddColumnFamily(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreAddColumnFamily")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreAddColumnFamily")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerKVStore(testDir, opts, nil)
	if err := store.AddColumnFamily("cf1"); err != nil {
		glog.Fatalf("Unable to add column family due to err: %v", err)
	}
	err := store.AddColumnFamily("cf1")
	if err != kv_store.ErrKVStoreColumnFamilyExists {
		glog.Fatalf("Tried to create column family again but it succeeded. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerKVStore(testDir, opts, nil)
	err = store.AddColumnFamily(kDefaultCFName)
	if err != kv_store.ErrKVStoreReservedColumnFamilyNames {
		glog.Fatalf("Tried to create reserved column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerKVStore(testDir, opts, nil)
	err = store.AddColumnFamily(kAllColumnFamiliesCFName)
	if err != kv_store.ErrKVStoreReservedColumnFamilyNames {
		glog.Fatalf("Tried to create reserved column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerKVStore(testDir, opts, nil)
	err = store.AddColumnFamily("default-1") // - not allowed in CF names. Only letters, digits and underscores.
	if err != kv_store.ErrKVStoreInvalidColumnFamilyName {
		glog.Fatalf("Tried to create invalid column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}
}

func TestBadgerKVStoreNewCF(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreNewCF")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreNewCF")
	cfName := "cf_2"
	createColumnFamily(testDir, cfName)
	doStoreSingleActorIO(testDir, cfName)
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerKVStoreMultiCF(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreMultiCF")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreMultiCF")
	doStoreMultiConcurrentActorIO(testDir, 8)
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerKVStoreTxn(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreTxn")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreTxn")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	opts.SyncWrites = true
	store := NewBadgerKVStore(testDir, opts, nil)

	// Testing discard transaction.
	glog.Infof("Testing batch put failures")
	txn := store.NewTransaction()
	failureKey := "failureKey"
	failureValue := "failureValue"
	entry := kv_store.KVStoreEntry{
		Key:          []byte(failureKey),
		Value:        []byte(failureValue),
		ColumnFamily: "",
	}
	failureKVStoreKey := kv_store.KVStoreKey{
		Key:          []byte(failureKey),
		ColumnFamily: "",
	}
	var entries []*kv_store.KVStoreEntry
	var keys []*kv_store.KVStoreKey
	entries = append(entries, &entry)
	keys = append(keys, &failureKVStoreKey)
	err := txn.BatchPut(entries)
	if err != nil {
		glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
		return
	}
	txn.Discard()

	_, errs := store.BatchGet(keys)
	if errs[0] != kv_store.ErrKVStoreKeyNotFound {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}

	glog.Infof("Testing Batch Put success")
	keys = nil
	entries = nil
	toBeDeletedKey := "to_be_deleted_key"
	toBeDeletedValue := "to_be_deleted_value"
	keys = append(keys, &kv_store.KVStoreKey{
		Key:          []byte(toBeDeletedKey),
		ColumnFamily: "",
	})
	entries = append(entries, &kv_store.KVStoreEntry{
		Key:          []byte(toBeDeletedKey),
		Value:        []byte(toBeDeletedValue),
		ColumnFamily: "",
	})
	txn = store.NewTransaction()
	err = txn.BatchPut(entries)
	if err != nil {
		glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
		return
	}
	if txn.Commit() != nil {
		glog.Fatalf("Failed to commit transaction due to err")
	}
	txn.Discard()

	fetchedVals, errs := store.BatchGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0].Value) != toBeDeletedValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0].Value))
	}

	// Test puts and delete in a single transaction and then discard it before committing.
	glog.Infof("Testing batch put and delete in transaction failure")
	successKey := "successKey"
	successValue := "successValue"
	var skeys []*kv_store.KVStoreKey
	var sentries []*kv_store.KVStoreEntry
	skeys = append(skeys, &kv_store.KVStoreKey{
		Key:          []byte(successKey),
		ColumnFamily: "",
	})
	sentries = append(sentries, &kv_store.KVStoreEntry{
		Key:          []byte(successKey),
		Value:        []byte(successValue),
		ColumnFamily: "",
	})
	testBatchPutAndDelete := func(txn kv_store.Transaction) {
		glog.Infof("Testing Batch Put and batch delete")
		err = txn.BatchPut(sentries)
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
	_, errs = store.BatchGet(skeys)
	if errs[0] != kv_store.ErrKVStoreKeyNotFound {
		glog.Fatalf("Expected to not find toBeDeletedKey: %s", successKey)
	}
	fetchedVals, errs = store.BatchGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0].Value) != toBeDeletedValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0].Value))
	}

	// Test puts and delete in a single transaction and then commit it.
	glog.Infof("Testing batch put and delete in transaction success")
	txn = store.NewTransaction()
	testBatchPutAndDelete(txn)
	if err := txn.Commit(); err != nil {
		glog.Fatalf("Unable to commit transaction due to error")
	}
	txn.Discard()
	fetchedVals, errs = store.BatchGet(skeys)
	if errs[0] != nil {
		glog.Fatalf("Hit an unexpected error: %v", errs[0])
	}
	if string(fetchedVals[0].Value) != successValue {
		glog.Fatalf("Data mismatch. Expected: %s, got: %s", toBeDeletedValue, string(fetchedVals[0].Value))
	}
	_, errs = store.BatchGet(keys)
	if errs[0] != kv_store.ErrKVStoreKeyNotFound {
		glog.Fatalf("Expected to not find toBeDeletedKey: %s", successKey)
	}

	glog.Infof("Testing Put and Delete and Get")
	successKey2 := "successKey2"
	successValue2 := "successValue2"
	keys = nil
	entries = nil
	keys = append(keys, &kv_store.KVStoreKey{
		Key:          []byte(successKey2),
		ColumnFamily: "",
	})
	entries = append(entries, &kv_store.KVStoreEntry{
		Key:          []byte(successKey2),
		Value:        []byte(successValue2),
		ColumnFamily: "",
	})
	txn = store.NewTransaction()
	if err := txn.Put(entries[0]); err != nil {
		glog.Fatalf("Unable to put values due to err: %v", err)
	}
	if err := txn.Delete(skeys[0]); err != nil {
		glog.Fatalf("Unable to delete value due to err: %v", err)
	}
	txn.Commit()
	txn.Discard()
	txn = store.NewTransaction()
	fetchedVals, errs = txn.BatchGet(keys)
	if errs[0] != nil {
		glog.Fatalf("Unable to get value due to err: %v", errs[0])
	}
	if string(fetchedVals[0].Value) != successValue2 {
		glog.Fatalf("Value mismatch. Expected: %s, got: %s", successValue2, string(fetchedVals[0].Value))
	}
	_, err = txn.Get(skeys[0])
	if err != kv_store.ErrKVStoreKeyNotFound {
		glog.Fatalf("Got unexpected error: %v", err)
	}
	txn.Discard()

	txn = store.NewTransaction()
	entries[0].ColumnFamily = "cf2"
	keys[0].ColumnFamily = "cf2"
	err = txn.Put(entries[0])
	if err != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	err = txn.BatchPut(entries)
	if err != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	err = txn.BatchDelete(keys)
	if err != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	err = txn.Delete(keys[0])
	if err != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	_, err = txn.Get(keys[0])
	if err != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	_, errs = txn.BatchGet(keys)
	if errs[0] != kv_store.ErrKVStoreColumnFamilyNotFound {
		glog.Fatalf("Hit an unexpected exception while attempting to put to non existent CF: %v", err)
	}
	txn.Discard()
}

func TestBadgerKVStoreTxnIO(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreTxnIO")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreTxnIO")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	opts.SyncWrites = true
	cfName := "cf1"
	createColumnFamily(testDir, cfName)
	store := NewBadgerKVStore(testDir, opts, nil)
	doTransactionIO(store, cfName)
}

func TestBadgerKVStoreConcurrentTxnIO(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreConcurrentTxnIO")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreConcurrentTxnIO")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	opts.SyncWrites = true
	var cfNames []string
	store := NewBadgerKVStore(testDir, opts, nil)
	numWorkers := 8
	for ii := 0; ii < numWorkers; ii++ {
		cfName := fmt.Sprintf("cf_%d", ii)
		createColumnFamilyWithStore(store, cfName)
		cfNames = append(cfNames, cfName)
	}
	wg := sync.WaitGroup{}
	workload := func(cf string) {
		doTransactionIO(store, cf)
		wg.Done()
	}
	for _, cf := range cfNames {
		wg.Add(1)
		go workload(cf)
	}
	glog.Infof("Started all workers. Waiting for them to finish")
	wg.Wait()
}

func TestBadgerKVStoreTxnConflict(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStoreTxnConflict")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStoreTxnConflict")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	opts.SyncWrites = true
	store := NewBadgerKVStore(testDir, opts, nil)
	ax := []byte("x")
	ay := []byte("y")

	// Set balance to $100 in each account.
	txn := store.NewTransaction()
	defer txn.Discard()
	val := []byte(strconv.Itoa(100))
	require.NoError(t, txn.Put(&kv_store.KVStoreEntry{
		Key:          ax,
		Value:        val,
		ColumnFamily: "",
	}))
	require.NoError(t, txn.Put(&kv_store.KVStoreEntry{
		Key:          ay,
		Value:        val,
		ColumnFamily: "",
	}))
	require.NoError(t, txn.Commit())

	getBal := func(txn kv_store.Transaction, key []byte) (bal int) {
		storeKey := &kv_store.KVStoreKey{
			Key:          key,
			ColumnFamily: "",
		}
		val, err := txn.Get(storeKey)
		require.NoError(t, err)
		bal, err = strconv.Atoi(string(val.Value))
		require.NoError(t, err)
		return bal
	}

	// Start two transactions, each would read both accounts and deduct from one account.
	txn1 := store.NewTransaction()

	sum := getBal(txn1, ax)
	sum += getBal(txn1, ay)
	require.Equal(t, 200, sum)
	require.NoError(t, txn1.Put(&kv_store.KVStoreEntry{
		Key:          ax,
		Value:        []byte("0"),
		ColumnFamily: "",
	})) // Deduct 100 from ax.

	// Let's read this back.
	sum = getBal(txn1, ax)
	require.Equal(t, 0, sum)
	sum += getBal(txn1, ay)
	require.Equal(t, 100, sum)
	// Don't commit yet.

	txn2 := store.NewTransaction()

	sum = getBal(txn2, ax)
	sum += getBal(txn2, ay)
	require.Equal(t, 200, sum)
	require.NoError(t, txn2.Put(&kv_store.KVStoreEntry{
		Key:          ay,
		Value:        []byte("0"),
		ColumnFamily: "",
	})) // Deduct 100 from ay.

	// Let's read this back.
	sum = getBal(txn2, ax)
	require.Equal(t, 100, sum)
	sum += getBal(txn2, ay)
	require.Equal(t, 100, sum)

	// Commit both now.
	require.NoError(t, txn1.Commit())
	require.Error(t, txn2.Commit()) // This should fail.
}

func TestBadgerKVStore_BatchPutAndScan(t *testing.T) {
	testutil.LogTestMarker("TestBadgerKVStore_BatchPutAndScan")
	testDir := testutil.CreateFreshTestDir("TestBadgerKVStore_BatchPutAndScan")
	opts := badger.DefaultOptions(testDir)
	opts.SyncWrites = true
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2  // Use 2 compactors.
	opts.IndexCacheSize = 0
	opts.Compression = options.None
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	opts.CompactL0OnClose = false
	opts.LoadBloomsOnOpen = false
	var store kv_store.KVStore
	store = NewBadgerKVStore(testDir, opts, nil)
	cfName := "offset"
	err := store.AddColumnFamily(cfName)
	if err != nil {
		glog.Fatalf("Unable to add column family due to err: %v", err)
	}
	batchSize := 4
	numIters := 100
	// Batch write values
	token := make([]byte, 1024*1024)
	rand.Read(token)
	glog.Infof("Benchmarking batch puts")
	var values [][]byte
	for ii := 0; ii < batchSize; ii++ {
		values = append(values, token)
	}
	start := time.Now()
	var allKeys [][]byte
	// Benchmark batch puts!
	for iter := 0; iter < numIters; iter++ {
		var entries []*kv_store.KVStoreEntry
		for ii := 0; ii < batchSize; ii++ {
			var entry kv_store.KVStoreEntry
			key := util.UintToBytes(uint64(iter*batchSize + ii))
			allKeys = append(allKeys, key)
			entry.Key = key
			entry.Value = values[ii]
			entry.ColumnFamily = cfName
			entries = append(entries, &entry)
		}
		err := store.BatchPut(entries)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
	}
	elapsed := time.Since(start)
	glog.Infof("Total put time: %v, average put time: %v", elapsed, elapsed/time.Duration(numIters))
	store.Close()

	// Benchmark batch gets!
	glog.Infof("Benchmarking batch gets")
	store = NewBadgerKVStore(testDir, opts, nil)
	var sk []byte
	startTime := time.Now()
	for ii := 0; ii < numIters; ii++ {
		var keys []*kv_store.KVStoreKey
		for jj := 0; jj < batchSize; jj++ {
			keys = append(keys, &kv_store.KVStoreKey{
				Key:          allKeys[ii*batchSize+jj],
				ColumnFamily: cfName,
			})
		}
		_, errs := store.BatchGet(keys)
		for jj := 0; jj < len(errs); jj++ {
			if errs[jj] != nil {
				glog.Fatalf("Failure while fetching key: %v, %v", keys[jj], err)
			}
		}
	}
	elapsed = time.Since(startTime)
	glog.Infof("Total batch get time: %v, average batch get time: %v", elapsed, elapsed/time.Duration(numIters))
	store.Close()

	// Benchmark scans!
	glog.Infof("Benchmarking scans")
	store = NewBadgerKVStore(testDir, opts, nil)
	startTime = time.Now()
	for ii := 0; ii < numIters; ii++ {
		scanner, err := store.NewScanner(cfName, sk, false)
		if err != nil {
			glog.Fatalf("Unable to create new scanner due to err: %v", err)
		}
		numValues := 0
		for ; scanner.Valid(); scanner.Next() {
			key, _, err := scanner.GetItem()
			if err != nil {
				glog.Fatalf("Failure while scanning KV store: %v", err)
			}
			numValues++
			sk = key
			if numValues == batchSize {
				break
			}
		}
	}
	elapsed = time.Since(startTime)
	glog.Infof("Total scan time: %v, average scan time: %v", elapsed, elapsed/time.Duration(numIters))
}

/*********************************** Helper functions ********************************************/
func doStoreSingleActorIO(testDir string, cf string) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerKVStore(testDir, opts, nil)
	defer store.Close()
	doStoreIO(store, cf)
}

func doStoreMultiConcurrentActorIO(testDir string, numWorkers int) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerKVStore(testDir, opts, nil)
	defer store.Close()
	wg := sync.WaitGroup{}
	workload := func(idx int) {
		cfName := fmt.Sprintf("column_family_%d", idx)
		err := store.AddColumnFamily(cfName)
		if err != nil {
			if err != kv_store.ErrKVStoreColumnFamilyExists {
				glog.Fatalf("Unable to add column family due to err: %v", err)
			}
		}
		doStoreIO(store, cfName)
		wg.Done()
	}
	for ii := 0; ii < numWorkers; ii++ {
		wg.Add(1)
		go workload(ii)
	}
	glog.Infof("Started all workers. Waiting for them to finish")
	wg.Wait()
}

func doStoreIO(store kv_store.KVStore, cf string) {
	batchSize := 10
	numIters := 20
	// Batch write values
	glog.Infof("Testing Batch Put")
	for iter := 0; iter < numIters; iter++ {
		var entries []*kv_store.KVStoreEntry
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var entry kv_store.KVStoreEntry
			entry.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			entry.Value = []byte(fmt.Sprintf("value-%03d", meraVal))
			entry.ColumnFamily = cf
			entries = append(entries, &entry)
		}
		err := store.BatchPut(entries)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
	}

	// Batch read and verify values
	glog.Infof("Testing BatchGet")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		entries, errs := store.BatchGet(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != nil {
				glog.Fatalf("Hit an unexpected error: %s", errs[ii].Error())
				return
			}
			exVal := []byte(fmt.Sprintf("value-%03d", iter*batchSize+ii))
			val := entries[ii].Value
			if bytes.Compare(exVal, val) != 0 {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", exVal, val)
				return
			}
			if entries[ii].ColumnFamily != cf {
				glog.Fatalf("CF mismatch. Expected: %s, Got: %s", cf, entries[ii].ColumnFamily)
			}
		}
	}

	// Scan using scanner and verify values
	glog.Infof("Testing forward scanner!")
	scanner, err := store.NewScanner(cf, nil, false)
	if err != nil {
		glog.Fatalf("Unable to initialize scanner due to err: %v", err)
	}
	count := -1
	for scanner.Rewind(); scanner.Valid(); scanner.Next() {
		count++
		if count == numIters*batchSize {
			glog.Fatalf("Scan is continuing even when it should have finished!")
		}
		key, val, err := scanner.GetItem()
		if err != nil {
			glog.Fatalf("Unable to scan item due to err: %v", err)
		}
		expectedKey := []byte(fmt.Sprintf("key-%03d", count))
		expectedVal := []byte(fmt.Sprintf("value-%03d", count))
		if bytes.Compare(key, expectedKey) != 0 {
			glog.Fatalf("Key mismatch. Expected: %s, Got: %s", string(expectedKey), string(key))
		}
		if bytes.Compare(val, expectedVal) != 0 {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", string(expectedVal), string(val))
		}
	}
	scanner.Close()

	glog.Infof("Testing forward scanner with start key")
	count = 4
	scanner, err = store.NewScanner(cf, []byte(fmt.Sprintf("key-%03d", count+1)), false)
	if err != nil {
		glog.Fatalf("Unable to initialize scanner due to err: %v", err)
	}
	for scanner.Rewind(); scanner.Valid(); scanner.Next() {
		count++
		if count == numIters*batchSize {
			glog.Fatalf("Scan is continuing even when it should have finished!")
		}
		key, val, err := scanner.GetItem()
		if err != nil {
			glog.Fatalf("Unable to scan item due to err: %v", err)
		}
		expectedKey := []byte(fmt.Sprintf("key-%03d", count))
		expectedVal := []byte(fmt.Sprintf("value-%03d", count))
		if bytes.Compare(key, expectedKey) != 0 {
			glog.Fatalf("Key mismatch. Expected: %s, Got: %s", string(expectedKey), string(key))
		}
		if bytes.Compare(val, expectedVal) != 0 {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", string(expectedVal), string(val))
		}
	}
	scanner.Close()

	// Scan using scanner in reverse and verify values
	glog.Infof("Testing reverse scanner!")
	scanner, err = store.NewScanner(cf, nil, true)
	if err != nil {
		glog.Fatalf("Unable to initialize scanner due to err: %v", err)
	}
	count = numIters * batchSize
	for scanner.Rewind(); scanner.Valid(); scanner.Next() {
		count--
		if count < 0 {
			glog.Fatalf("Scan is continuing even when it should have finished!")
		}
		key, val, err := scanner.GetItem()
		if err != nil {
			glog.Fatalf("Unable to scan item due to err: %v", err)
		}
		expectedKey := []byte(fmt.Sprintf("key-%03d", count))
		expectedVal := []byte(fmt.Sprintf("value-%03d", count))
		if bytes.Compare(key, expectedKey) != 0 {
			glog.Fatalf("Key mismatch. Expected: %s, Got: %s", string(expectedKey), string(key))
		}
		if bytes.Compare(val, expectedVal) != 0 {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", string(expectedVal), string(val))
		}
	}
	scanner.Close()

	// Batch delete keys.
	glog.Infof("Testing BatchDelete")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		err := store.BatchDelete(keys)
		if err != nil {
			glog.Fatalf("Unable to delete keys due to err: %v", err)
		}
	}

	// Batch read and verify values
	glog.Infof("Testing BatchGet")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		_, errs := store.BatchGet(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != kv_store.ErrKVStoreKeyNotFound {
				glog.Fatalf("Hit an unexpected error: %v", errs[ii])
				return
			}
		}
	}

	// Test single put, get, delete.
	glog.Infof("Testing single put, get and delete")
	singleKey := []byte("singleKey")
	singleVal := []byte("singleVal")
	var entry kv_store.KVStoreEntry
	entry.Key = singleKey
	entry.Value = singleVal
	entry.ColumnFamily = cf
	var key kv_store.KVStoreKey
	key.Key = singleKey
	key.ColumnFamily = cf
	err = store.Put(&entry)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while loading single key. Err: %s", err.Error())
		return
	}
	val, err := store.Get(&key)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while reading single key. Err: %s", err.Error())
		return
	}
	if bytes.Compare(val.Value, singleVal) != 0 {
		glog.Fatalf("Value mismatch. Expected: %s, Got: %s", singleVal, val)
	}

	err = store.Delete(&key)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while deleting single key. Err: %s", err.Error())
		return
	}
	_, err = store.Get(&key)
	if err != kv_store.ErrKVStoreKeyNotFound {
		glog.Fatalf("Hit an unexpected error while reading single key. Err: %v", err)
		return
	}
}

func doTransactionIO(store kv_store.KVStore, cf string) {
	batchSize := 10
	numIters := 20
	// Batch write values
	glog.Infof("Testing Batch Put")
	for iter := 0; iter < numIters; iter++ {
		var entries []*kv_store.KVStoreEntry
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var entry kv_store.KVStoreEntry
			entry.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			entry.Value = []byte(fmt.Sprintf("value-%03d", meraVal))
			entry.ColumnFamily = cf
			entries = append(entries, &entry)
		}
		txn := store.NewTransaction()
		err := txn.BatchPut(entries)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
		err = txn.Commit()
		if err != nil {
			glog.Fatalf("Unable to commit transaction due to err: %v", err)
		}
		txn.Discard()
	}

	// Batch read and verify values
	glog.Infof("Testing BatchGet")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		txn := store.NewTransaction()
		entries, errs := txn.BatchGet(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != nil {
				glog.Fatalf("Hit an unexpected error: %s", errs[ii].Error())
				return
			}
			exVal := []byte(fmt.Sprintf("value-%03d", iter*batchSize+ii))
			val := entries[ii].Value
			if bytes.Compare(exVal, val) != 0 {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", exVal, val)
				return
			}
			if entries[ii].ColumnFamily != cf {
				glog.Fatalf("CF mismatch. Expected: %s, Got: %s", cf, entries[ii].ColumnFamily)
			}
		}
		txn.Discard()
	}

	// Batch delete keys.
	glog.Infof("Testing BatchDelete")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		txn := store.NewTransaction()
		err := txn.BatchDelete(keys)
		if err != nil {
			glog.Fatalf("Unable to delete keys due to err: %v", err)
		}
		err = txn.Commit()
		if err != nil {
			glog.Fatalf("Unable to commit transaction due to err: %v", err)
		}
		txn.Discard()
	}

	// Batch read and verify values
	glog.Infof("Testing BatchGet")
	for iter := 0; iter < numIters; iter++ {
		var keys []*kv_store.KVStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key kv_store.KVStoreKey
			key.Key = []byte(fmt.Sprintf("key-%03d", meraVal))
			key.ColumnFamily = cf
			keys = append(keys, &key)
		}
		txn := store.NewTransaction()
		_, errs := txn.BatchGet(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != kv_store.ErrKVStoreKeyNotFound {
				glog.Fatalf("Hit an unexpected error: %v", errs[ii])
			}
		}
		txn.Discard()
	}
}

func createColumnFamily(testDir string, cfname string) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerKVStore(testDir, opts, nil)
	defer store.Close()
	if err := store.AddColumnFamily(cfname); err != nil {
		glog.Fatalf("Unable to add column family due to err: %v", err)
	}
}

func createColumnFamilyWithStore(store kv_store.KVStore, cfname string) {
	if err := store.AddColumnFamily(cfname); err != nil {
		glog.Fatalf("Unable to add column family due to err: %v", err)
	}
}
