package kv_store

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createBadgerKVStoreTestDir(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/badger_kv_store_test/%s", testName)
	err := os.RemoveAll(dataDir)
	if err != nil {
		t.Fatalf("Unable to delete test directory: %s", dataDir)
	}
	err = os.MkdirAll(dataDir, 0774)
	if err != nil {
		glog.Fatalf("Unable to create test dir: %s", dataDir)
	}
	return dataDir
}

func TestBadgerKVStore(t *testing.T) {
	glog.Infof("*******************************************************************************************\n\n")
	glog.Infof("Starting TestBadgerKVStore")
	testDir := createBadgerKVStoreTestDir(t, "TestBadgerKVStore")
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 1
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
		var keys []string
		var values []string
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			keys = append(keys, fmt.Sprintf("key-%03d", meraVal))
			values = append(values, fmt.Sprintf("value-%03d", meraVal))
		}
		err := store.BatchPutS(keys, values)
		if err != nil {
			glog.Fatalf("Unable to batch put values due to err: %s", err.Error())
			return
		}
	}

	// Batch read and verify values
	glog.Infof("Testing MultiGet")
	for iter := 0; iter < numIters; iter++ {
		var keys []string
		var values []string
		for ii := 0; ii < batchSize; ii++ {
			meraVal := iter*batchSize + ii
			keys = append(keys, fmt.Sprintf("key-%03d", meraVal))
		}
		values, errs := store.MultiGetS(keys)
		for ii := 0; ii < len(keys); ii++ {
			if errs[ii] != nil {
				glog.Fatalf("Hit an unexpected error: %s", errs[ii].Error())
				return
			}
			exVal := fmt.Sprintf("value-%03d", iter*batchSize+ii)
			val := values[ii]
			if exVal != val {
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

	glog.Infof("Testing put and get")
	singleKey := "singleKey"
	singleVal := "singleVal"
	err := store.PutS(singleKey, singleVal)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while loading single key. Err: %s", err.Error())
		return
	}
	val, err := store.GetS(singleKey)
	if err != nil {
		glog.Fatalf("Hit an unexpected error while reading single key. Err: %s", err.Error())
		return
	}
	if val != singleVal {
		glog.Fatalf("Value mismatch. Expected: %s, Got: %s", singleVal, val)
	}
	glog.Infof("Badger KV store test finished successfully")
}
