package segments

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"testing"
	"time"
)

var logger = logging.NewPrefixLogger("Test")

func checkMetadata(t *testing.T, got *SegmentMetadata, expected *SegmentMetadata) {
	logger.Infof("Metadata: %s", got.ToString())
	if got.ID != expected.ID {
		logger.Fatalf("ID mismatch. Expected: %d, Got: %d", expected.ID, got.ID)
	}
	if got.StartOffset != expected.StartOffset {
		logger.Fatalf("Start offset mismatch. Expected: %d, Got: %d",
			expected.StartOffset, got.StartOffset)
	}
	if !got.CreatedTimestamp.Equal(expected.CreatedTimestamp) {
		logger.Fatalf("Created Time mismatch. Expected: %v, Got: %v",
			expected.CreatedTimestamp, got.CreatedTimestamp)
	}
	if got.Immutable != expected.Immutable {
		logger.Fatalf("Immutable mismatch. Expected: %v, Got: %v",
			expected.Immutable, got.Immutable)
	}
	if got.Expired != expected.Expired {
		logger.Fatalf("Expired mismatch. Expected: %v, Got: %v",
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
		logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
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
				logger.Fatalf("Failed to close segment due to err: %s", err.Error())
			}
			bds, err = NewBadgerSegment(&opts)
			if err != nil {
				logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
			}
			bds.Open()
			got := bds.GetMetadata()
			checkMetadata(t, &got, &initialMeta)
			ex := base.Offset(0)
			if iter == 0 {
				ex = -1
			} else {
				ex = bds.nextOffSet - 1
			}
			if got.EndOffset != ex {
				logger.Fatalf("End offset mismatch. Expected: %d, Got: %d", ex, got.EndOffset)
			}
		}

		logger.Infof("Starting iteration: %d", iter)
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
			logger.Fatalf("Unable to append values to segment due to err: %s", ret.Error.Error())
		}
		sarg := sbase.ScanEntriesArg{
			StartOffset:    base.Offset(iter * batchSize),
			NumMessages:    uint64(batchSize),
			StartTimestamp: -1,
			EndTimestamp:   -1,
		}
		sret := bds.Scan(context.Background(), &sarg)
		if sret.Error != nil {
			logger.Fatalf("Received error while scanning message. Error: %s", sret.Error.Error())
		}
		for ii := 0; ii < batchSize; ii++ {
			value := string(sret.Values[ii].Value)
			expectedVal := fmt.Sprintf("value-%04d", ii)
			if value != expectedVal {
				logger.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, value)
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
		logger.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ImmutableTimestamp) {
		logger.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	err = bds.Close()
	if err != nil {
		logger.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
	bds, err = NewBadgerSegment(&opts)
	if err != nil {
		logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.Open()
	metadata = bds.GetMetadata()
	checkMetadata(t, &metadata, expected)
	if !now.Before(metadata.ImmutableTimestamp) {
		logger.Fatalf("Immutable timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	if !now.Before(metadata.ExpiredTimestamp) {
		logger.Fatalf("Expired timestamp mismatch. Now: %v, Immutable TS: %v", now, metadata.ImmutableTimestamp)
	}
	fts, lts := bds.GetMsgTimestampRange()
	if !((fts > startTs) && (fts < endTs)) {
		logger.Fatalf("First message timestamp(%d) must have been in the range (%d, %d)", fts, startTs, endTs)
	}
	if !((lts > startTs) && (lts < endTs) && (lts > fts)) {
		logger.Fatalf("last message timestamp(%d) must have been in the range (%d, %d) and greater than %d",
			lts, startTs, endTs, fts)
	}
	err = bds.Close()
	if err != nil {
		logger.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
	if metadata.EndOffset != expected.EndOffset {
		logger.Fatalf("End offset mismatch. Expected: %d, Got: %d", expected.EndOffset, metadata.EndOffset)
	}
}
