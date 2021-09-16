package cf_store

import (
	"bytes"
	"eeylops/server/storage/kv_store"
	"eeylops/util/testutil"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
	"sync"
	"testing"
)

func TestBadgerCFStoreDefault(t *testing.T) {
	testutil.LogTestMarker("TestBadgerCFStore")
	testDir := testutil.CreateTestDir(t, "TestBadgerCFStore")
	doSingleActorIO(testDir, "")
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerCFStoreAddColumnFamily(t *testing.T) {
	testutil.LogTestMarker("TestBadgerCFStoreAddColumnFamily")
	testDir := testutil.CreateTestDir(t, "TestBadgerCFStoreAddColumnFamily")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerCFStore(testDir, opts)
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

	store = NewBadgerCFStore(testDir, opts)
	err = store.AddColumnFamily(kDefaultCFName)
	if err != kv_store.ErrKVStoreReservedColumnFamilyNames {
		glog.Fatalf("Tried to create reserved column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerCFStore(testDir, opts)
	err = store.AddColumnFamily(kAllColumnFamiliesCFName)
	if err != kv_store.ErrKVStoreReservedColumnFamilyNames {
		glog.Fatalf("Tried to create reserved column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerCFStore(testDir, opts)
	err = store.AddColumnFamily("default-1") // - not allowed in CF names. Only letters, digits and underscores.
	if err != kv_store.ErrKVStoreInvalidColumnFamilyName {
		glog.Fatalf("Tried to create invalid column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}
}

func TestBadgerCFStoreNewCF(t *testing.T) {
	testutil.LogTestMarker("TestBadgerCFStoreNewCF")
	testDir := testutil.CreateTestDir(t, "TestBadgerCFStoreNewCF")
	cfName := "cf_2"
	createColumnFamily(testDir, cfName)
	doSingleActorIO(testDir, cfName)
	glog.Infof("Badger KV store test finished successfully")
}

func TestBadgerCFStoreMultiCF(t *testing.T) {
	testutil.LogTestMarker("TestBadgerCFStoreMultiCF")
	testDir := testutil.CreateTestDir(t, "TestBadgerCFStoreMultiCF")
	doConcurrentIO(testDir, 8)
	glog.Infof("Badger KV store test finished successfully")
}

func doSingleActorIO(testDir string, cf string) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerCFStore(testDir, opts)
	defer store.Close()
	doStoreIO(store, cf)
}

func doConcurrentIO(testDir string, numWorkers int) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerCFStore(testDir, opts)
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

func doStoreIO(store CFStore, cf string) {
	batchSize := 10
	numIters := 20
	// Batch write values
	glog.Infof("Testing Batch Put")
	for iter := 0; iter < numIters; iter++ {
		var entries []*CFStoreEntry
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var entry CFStoreEntry
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
		var keys []*CFStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key CFStoreKey
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

	// Scan and verify values
	glog.Infof("Testing Scan")
	for iter := 0; iter < numIters; iter++ {
		meraVal := iter * batchSize
		expectedNextKey := []byte(fmt.Sprintf("key-%03d", (iter+1)*batchSize))
		sk := []byte(fmt.Sprintf("key-%03d", meraVal))
		entries, nk, err := store.Scan(cf, sk, batchSize, 1024*1024, false)
		if err != nil {
			glog.Fatalf("Unexpected error while scanning: %v", err)
		}
		for ii := 0; ii < len(entries); ii++ {
			meraSingleVal := meraVal + ii
			expectedKey := []byte(fmt.Sprintf("key-%03d", meraSingleVal))
			expectedVal := []byte(fmt.Sprintf("value-%03d", meraSingleVal))
			if bytes.Compare(expectedKey, entries[ii].Key) != 0 {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", string(expectedKey),
					string(entries[ii].Key))
				return
			}
			if bytes.Compare(expectedVal, entries[ii].Value) != 0 {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", string(expectedVal),
					string(entries[ii].Value))
				return
			}
		}
		if iter == numIters-1 {
			if nk != nil {
				glog.Fatalf("Next key is not nil!")
			}
		} else {
			if bytes.Compare(nk, expectedNextKey) != 0 {
				glog.Fatalf("Unexpected next key: %s", string(nk))
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
		var keys []*CFStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key CFStoreKey
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
		var keys []*CFStoreKey
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			var key CFStoreKey
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
	var entry CFStoreEntry
	entry.Key = singleKey
	entry.Value = singleVal
	entry.ColumnFamily = cf
	var key CFStoreKey
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

func createColumnFamily(testDir string, cfname string) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerCFStore(testDir, opts)
	defer store.Close()
	if err := store.AddColumnFamily(cfname); err != nil {
		glog.Fatalf("Unable to add column family due to err: %v", err)
	}
}
