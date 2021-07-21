package segments

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"testing"
	"time"
)

func checkMetadata(t *testing.T, got *SegmentMetadata, expected *SegmentMetadata) {
	glog.Infof("Metadata: %s", got.ToString())
	if got.ID != expected.ID {
		glog.Fatalf("ID mismatch. Expected: %d, Got: %d", expected.ID, got.ID)
	}
	if got.StartOffset != expected.StartOffset {
		glog.Fatalf("Start offset mismatch. Expected: %d, Got: %d",
			expected.StartOffset, got.StartOffset)
	}
	if !got.CreatedTimestamp.Equal(expected.CreatedTimestamp) {
		glog.Fatalf("Created Time mismatch. Expected: %v, Got: %v",
			expected.CreatedTimestamp, got.CreatedTimestamp)
	}
	if got.Immutable != expected.Immutable {
		glog.Fatalf("Immutable mismatch. Expected: %v, Got: %v",
			expected.Immutable, got.Immutable)
	}
	if got.Expired != expected.Expired {
		glog.Fatalf("Expired mismatch. Expected: %v, Got: %v",
			expected.Expired, got.Expired)
	}
}

func TestBadgerSegment(t *testing.T) {
	fmt.Println("Started badger tests!")
	util.LogTestMarker("TestBadgerSegment")
	dataDir := util.CreateTestDir(t, "TestBadgerSegment")
	initialMeta := SegmentMetadata{
		ID:               100,
		Immutable:        false,
		StartOffset:      10000,
		EndOffset:        -1,
		CreatedTimestamp: time.Now(),
		ImmutableReason:  0,
	}
	opts := BadgerSegmentOpts{
		RootDir:       dataDir,
		ParentLogger:  nil,
		Topic:         "topic1",
		PartitionID:   1,
		ScanSizeBytes: 16 * (1024 * 1024), // 16MB
	}
	bds, err := NewBadgerSegment(&opts)
	if err != nil {
		glog.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.SetMetadata(initialMeta)
	batchSize := 10
	numIters := 20
	lastRLogIdx := int64(0)
	startTs := time.Now().UnixNano()
	for iter := 0; iter < numIters; iter++ {
		if iter%5 == 0 {
			err = bds.Close()
			if err != nil {
				glog.Fatalf("Failed to close segment due to err: %s", err.Error())
			}
			bds, err = NewBadgerSegment(&opts)
			if err != nil {
				glog.Fatalf("Unable to create badger segment due to err: %s", err.Error())
			}
			got := bds.GetMetadata()
			checkMetadata(t, &got, &initialMeta)
			ex := base.Offset(0)
			if iter == 0 {
				ex = initialMeta.StartOffset
			} else {
				ex = bds.nextOffSet - 1
			}
			if got.EndOffset != ex {
				glog.Fatalf("End offset mismatch. Expected: %d, Got: %d", ex, got.EndOffset)
			}
		}

		glog.Infof("Starting iteration: %d", iter)
		var values [][]byte
		for ii := 0; ii < batchSize; ii++ {
			values = append(values, []byte(fmt.Sprintf("value-%04d", ii)))
		}
		lastRLogIdx++
		arg := sbase.AppendEntriesArg{
			Entries:   values,
			Timestamp: time.Now().UnixNano(),
			RLogIdx:   lastRLogIdx,
		}

		ret := bds.Append(context.Background(), &arg)
		if ret.Error != nil {
			glog.Fatalf("Unable to append values to segment due to err: %s", ret.Error.Error())
		}
		sarg := sbase.ScanEntriesArg{
			StartOffset:    base.Offset(iter * batchSize),
			NumMessages:    uint64(batchSize),
			StartTimestamp: -1,
			EndTimestamp:   -1,
		}
		sret := bds.Scan(context.Background(), &sarg)
		if sret.Error != nil {
			glog.Fatalf("Received error while scanning message. Error: %s", sret.Error.Error())
		}
		for ii := 0; ii < batchSize; ii++ {
			value := string(sret.Values[ii].Value)
			expectedVal := fmt.Sprintf("value-%04d", ii)
			if value != expectedVal {
				glog.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, value)
			}
		}
	}
	endTs := time.Now().UnixNano()
	// Mark segment as immutable and expired and check metadata again.
	now := time.Now()
	bds.MarkImmutable()
	bds.MarkExpired()
	expected := &SegmentMetadata{
		ID:                 initialMeta.ID,
		Immutable:          true,
		Expired:            true,
		StartOffset:        initialMeta.StartOffset,
		EndOffset:          bds.nextOffSet - 1,
		CreatedTimestamp:   initialMeta.CreatedTimestamp,
		ImmutableTimestamp: time.Time{},
		ImmutableReason:    0,
	}
	metadata := bds.GetMetadata()
	checkMetadata(t, &metadata, expected)
	if !now.Before(metadata.ExpiredTimestamp) {
		glog.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ImmutableTimestamp) {
		glog.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}

	err = bds.Close()
	if err != nil {
		glog.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
	bds, err = NewBadgerSegment(&opts)
	if err != nil {
		glog.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	metadata = bds.GetMetadata()
	checkMetadata(t, &metadata, expected)
	if !now.Before(metadata.ImmutableTimestamp) {
		glog.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ExpiredTimestamp) {
		glog.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	fts, lts := bds.GetMsgTimestampRange()
	if !((fts > startTs) && (fts < endTs)) {
		glog.Fatalf("First message timestamp(%d) must have been in the range (%d, %d)", fts, startTs, endTs)
	}
	if !((lts > startTs) && (lts < endTs) && (lts > fts)) {
		glog.Fatalf("last message timestamp(%d) must have been in the range (%d, %d) and greater than %d",
			lts, startTs, endTs, fts)
	}
	err = bds.Close()
	if err != nil {
		glog.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
	if metadata.EndOffset != expected.EndOffset {
		glog.Fatalf("End offset mismatch. Expected: %d, Got: %d", expected.EndOffset, metadata.EndOffset)
	}
}
