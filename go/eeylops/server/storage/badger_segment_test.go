package storage

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createTestDir(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/badger_segment_test/%s", testName)
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

func TestBadgerSegment(t *testing.T) {
	dataDir := createTestDir(t, "TestBadgerSegment")
	bds, err := NewBadgerSegment(dataDir)
	if err != nil {
		t.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	batchSize := 10
	numIters := 20
	for iter := 0; iter < numIters; iter++ {
		if iter%5 == 0 {
			err = bds.Close()
			if err != nil {
				t.Fatalf("Failed to close segment due to err: %s", err.Error())
			}
			bds, err = NewBadgerSegment(dataDir)
			if err != nil {
				t.Fatalf("Unable to create badger segment due to err: %s", err.Error())
			}
		}

		glog.Infof("Starting iteration: %d", iter)
		var values [][]byte
		for ii := 0; ii < batchSize; ii++ {
			values = append(values, []byte(fmt.Sprintf("value-%d", ii)))
		}
		err = bds.Append(values)
		if err != nil {
			t.Fatalf("Unable to append values to segment due to err: %s", err.Error())
		}
		vals, errs := bds.Scan(uint64(iter*batchSize), uint64(batchSize))
		for ii := 0; ii < batchSize; ii++ {
			if errs[ii] != nil {
				t.Fatalf("Received error while scanning message: %d. Error: %s", ii, errs[ii].Error())
			}
			value := string(vals[ii])
			expectedVal := fmt.Sprintf("value-%d", ii)
			if value != expectedVal {
				t.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, value)
			}
		}
	}
	metadata := bds.Metadata()
	glog.Infof("Metadata: %v", metadata)
	bds.Close()
}
