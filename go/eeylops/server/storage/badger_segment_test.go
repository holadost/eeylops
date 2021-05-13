package storage

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
	"time"
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

func checkMetadata(t *testing.T, got *SegmentMetadata, expected *SegmentMetadata) {
	glog.Infof("Metadata: %s", got.ToString())
	if got.ID != expected.ID {
		t.Fatalf("ID mismatch. Expected: %d, Got: %d", expected.ID, got.ID)
	}
	if got.StartOffset != expected.StartOffset {
		t.Fatalf("Start offset mismatch. Expected: %d, Got: %d",
			expected.StartOffset, got.StartOffset)
	}
	if got.EndOffset != expected.EndOffset {
		t.Fatalf("End offset mismatch. Expected: %d, Got: %d",
			expected.EndOffset, got.EndOffset)
	}
	if !got.CreatedTimestamp.Equal(expected.CreatedTimestamp) {
		t.Fatalf("Time mismatch. Expected: %v, Got: %v",
			expected.CreatedTimestamp, got.CreatedTimestamp)
	}
	if got.Immutable != expected.Immutable {
		t.Fatalf("Immutable mismatch. Expected: %v, Got: %v",
			expected.Immutable, got.Immutable)
	}
	if got.Expired != expected.Expired {
		t.Fatalf("Expired mismatch. Expected: %v, Got: %v",
			expected.Expired, got.Expired)
	}
}

func TestBadgerSegment(t *testing.T) {
	dataDir := createTestDir(t, "TestBadgerSegment")
	initialMeta := SegmentMetadata{
		ID:               100,
		Immutable:        false,
		StartOffset:      10000,
		EndOffset:        0,
		CreatedTimestamp: time.Now(),
		ImmutableReason:  0,
	}
	bds, err := NewBadgerSegment(dataDir)
	if err != nil {
		t.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.SetMetadata(initialMeta)
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
			got := bds.GetMetadata()
			checkMetadata(t, &got, &initialMeta)
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

	// Mark segment as immutable and expired and check metadata again.
	now := time.Now()
	bds.MarkImmutable()
	bds.MarkExpired()
	expected := &SegmentMetadata{
		ID:                 initialMeta.ID,
		Immutable:          true,
		Expired:            true,
		StartOffset:        initialMeta.StartOffset,
		EndOffset:          initialMeta.StartOffset + bds.nextOffSet - 1,
		CreatedTimestamp:   initialMeta.CreatedTimestamp,
		ImmutableTimestamp: time.Time{},
		ImmutableReason:    0,
	}
	metadata := bds.GetMetadata()
	checkMetadata(t, &metadata, expected)
	if !now.Before(metadata.ExpiredTimestamp) {
		t.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ImmutableTimestamp) {
		t.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}

	err = bds.Close()
	if err != nil {
		t.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
	bds, err = NewBadgerSegment(dataDir)
	if err != nil {
		t.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	metadata = bds.GetMetadata()
	checkMetadata(t, &metadata, expected)
	if !now.Before(metadata.ImmutableTimestamp) {
		t.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ExpiredTimestamp) {
		t.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	err = bds.Close()
	if err != nil {
		t.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
}
