package cf_store

import (
	"bytes"
	"eeylops/server/storage/kv_store"
	"eeylops/util/testutil"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/glog"
	"testing"
)

func TestBadgerCFStoreDefault(t *testing.T) {
	testutil.LogTestMarker("TestBadgerCFStore")
	testDir := testutil.CreateTestDir(t, "TestBadgerCFStore")
	doPutsAndGets(testDir, "")
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
	if err != kv_store.ErrColumnFamilyExists {
		glog.Fatalf("Tried to create column family again but it succeeded. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerCFStore(testDir, opts)
	err = store.AddColumnFamily("default")
	if err != kv_store.ErrReservedColumnFamilyNames {
		glog.Fatalf("Tried to create reserved column family. Got err: %v", err)
	}
	err = store.Close()
	if err != nil {
		glog.Fatalf("Unable to close the store due to err: %v", err)
	}

	store = NewBadgerCFStore(testDir, opts)
	err = store.AddColumnFamily("default-1")
	if err != kv_store.ErrInvalidColumnFamilyName {
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
	doPutsAndGets(testDir, cfName)
	glog.Infof("Badger KV store test finished successfully")
}

func doPutsAndGets(testDir string, cf string) {
	opts := badger.DefaultOptions(testDir)
	opts.MaxLevels = 7
	opts.NumMemtables = 2
	opts.SyncWrites = true
	opts.VerifyValueChecksum = true
	opts.NumCompactors = 2
	store := NewBadgerCFStore(testDir, opts)
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
			store = NewBadgerCFStore(testDir, opts)
		}
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
	store.Close()
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
