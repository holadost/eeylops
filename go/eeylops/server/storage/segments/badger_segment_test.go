package segments

import (
	"bytes"
	"context"
	"eeylops/server/base"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"github.com/golang/glog"
	"math/rand"
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

type testWorkload struct {
	lastOffsetAppended base.Offset
}

func newTestWorkload() *testWorkload {
	var tw testWorkload
	tw.lastOffsetAppended = -1
	return &tw
}

func (tw *testWorkload) appendMessages(startOffset base.Offset, seg *BadgerSegment, numMessages int, payloadSize int,
	timestamp int64, rlogIdx int64) {
	if startOffset <= tw.lastOffsetAppended {
		glog.Fatalf("Incorrect start offset provided. Last offset appended to segment: %d, provided: %d",
			tw.lastOffsetAppended, startOffset)
	}
	msg := make([]byte, payloadSize)
	rand.Read(msg)
	var values [][]byte
	for ii := startOffset; ii < startOffset+base.Offset(numMessages); ii++ {
		value := append(append([]byte("value-"), util.UintToBytes(uint64(ii))...), msg...)
		values = append(values, value)
	}
	arg := &AppendEntriesArg{}
	arg.Entries = values
	arg.Timestamp = timestamp
	arg.RLogIdx = rlogIdx
	ret := seg.Append(context.Background(), arg)
	if ret.Error != nil {
		glog.Fatalf("Got error while appending: %s", ret.Error.Error())
	}
	tw.lastOffsetAppended = startOffset + base.Offset(len(values)) - 1
}

func (tw *testWorkload) scanMessagesByTimestamp(seg *BadgerSegment, numMessages int, startTs int64, endTs int64, expectedStartOffset base.Offset, expectedNumMessages int) {
	var arg ScanEntriesArg
	arg.StartTimestamp = startTs
	arg.StartOffset = -1
	arg.EndTimestamp = endTs
	arg.NumMessages = uint64(numMessages)
	ret := seg.Scan(context.Background(), &arg)
	if ret.Error != nil {
		glog.Fatalf("Got error while scanning: %s", ret.Error.Error())
	}
	if len(ret.Values) > numMessages {
		glog.Fatalf("Expected <= %d messages, got: %d", numMessages, len(ret.Values))
	}
	for ii, value := range ret.Values {
		if !((value.Timestamp >= startTs) && (value.Timestamp < endTs)) {
			glog.Fatalf("Expected value timestamp: %d to fall between [%d, %d]", value.Timestamp, startTs, endTs)
		}
		if ii == 0 && expectedStartOffset >= 0 {
			if value.Offset != expectedStartOffset {
				glog.Fatalf("Start offset mismatch. Expected: %d, got: %d", expectedStartOffset, value.Offset)
			}
		}
		if value.Offset > tw.lastOffsetAppended {
			glog.Fatalf("Incorrect offset. Expected offset <= %d, got: %d", tw.lastOffsetAppended, value.Offset)
		}
		valueBytes, offsetNumBytes := value.Value[0:6], value.Value[6:14]
		if bytes.Compare(valueBytes, []byte("value-")) != 0 {
			glog.Fatalf("Value mismatch. Expected: value-, got: %s", string(valueBytes))
		}
		valOffset := base.Offset(util.BytesToUint(offsetNumBytes))
		if valOffset != value.Offset {
			glog.Fatalf("Offset mismatch. Expected offset: %d, got from value: %d", value.Offset, valOffset)
		}
	}
}

func (tw *testWorkload) scanExpiredMessagesByTimestamp(seg *BadgerSegment, numMessages int, startTs int64, endTs int64) {
	var arg ScanEntriesArg
	arg.StartTimestamp = startTs
	arg.StartOffset = -1
	arg.EndTimestamp = endTs
	arg.NumMessages = uint64(numMessages)
	ret := seg.Scan(context.Background(), &arg)
	if ret.Error != nil {
		glog.Fatalf("Got error while scanning: %s", ret.Error.Error())
	}
	if len(ret.Values) != 0 {
		glog.Fatalf("Expected 0 messages, got: %d", len(ret.Values))
	}
}

func (tw *testWorkload) scanMessagesByOffset(seg *BadgerSegment, numMessages int, startOffset base.Offset, endTs int64, expectedStartOffset base.Offset, expectedNumMessages int) {
	var arg ScanEntriesArg
	arg.StartOffset = startOffset
	arg.StartTimestamp = -1
	arg.EndTimestamp = endTs
	arg.NumMessages = uint64(numMessages)
	ret := seg.Scan(context.Background(), &arg)
	if ret.Error != nil {
		glog.Fatalf("Got error while scanning: %s", ret.Error.Error())
	}
	if expectedNumMessages >= 0 {
		if len(ret.Values) != expectedNumMessages {
			glog.Fatalf("Num messages mismatch. Expected %d messages, got: %d. Next offset to scan: %d", expectedNumMessages, len(ret.Values), ret.NextOffset)
		}
	}
	for ii, value := range ret.Values {
		if ii == 0 && expectedStartOffset >= 0 {
			if value.Offset != expectedStartOffset {
				glog.Fatalf("Start offset mismatch. Expected: %d, got: %d", expectedStartOffset, value.Offset)
			}
		}
		if value.Offset > tw.lastOffsetAppended {
			glog.Fatalf("Incorrect offset. Expected offset <= %d, got: %d", tw.lastOffsetAppended, value.Offset)
		}
		valueBytes, offsetNumBytes := value.Value[0:6], value.Value[6:14]
		if bytes.Compare(valueBytes, []byte("value-")) != 0 {
			glog.Fatalf("Value mismatch. Expected: value-, got: %s", string(valueBytes))
		}
		valOffset := base.Offset(util.BytesToUint(offsetNumBytes))
		if valOffset != value.Offset {
			glog.Fatalf("Offset mismatch. Expected offset: %d, got from value: %d", value.Offset, valOffset)
		}
	}
}

func TestBadgerSegment(t *testing.T) {
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
		RootDir:     dataDir,
		Logger:      nil,
		Topic:       "topic1",
		PartitionID: 1,
		TTLSeconds:  86400,
	}
	bds, err := NewBadgerSegment(&opts)
	if err != nil {
		logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.SetMetadata(initialMeta)
	batchSize := 10
	numIters := 20
	lastRLogIdx := int64(0)
	firstMsgTs := int64(0)
	lastMsgTs := int64(0)
	startTs := time.Now().UnixNano()
	for iter := 0; iter < numIters; iter++ {
		if iter%3 == 0 {
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
				ex = bds.nextOffset - 1
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
		now := time.Now().UnixNano()
		if iter == 0 {
			firstMsgTs = now
		}
		if iter == numIters-1 {
			lastMsgTs = now
		}
		var arg AppendEntriesArg
		{
		}
		arg.Entries = values
		arg.Timestamp = now
		arg.RLogIdx = lastRLogIdx

		ret := bds.Append(context.Background(), &arg)
		if ret.Error != nil {
			logger.Fatalf("Unable to append values to segment due to err: %s", ret.Error.Error())
		}
		sarg := ScanEntriesArg{}
		sarg.StartOffset = base.Offset(iter*batchSize) + initialMeta.StartOffset
		sarg.NumMessages = uint64(batchSize)
		sarg.StartTimestamp = -1
		sarg.EndTimestamp = -1

		logger.Infof("Successfully appended messages. Now scanning messages using arg: %v", sarg)
		sret := bds.Scan(context.Background(), &sarg)
		if sret.Error != nil {
			logger.Fatalf("Received error while scanning message. Error: %s", sret.Error.Error())
		}
		for ii := 0; ii < batchSize; ii++ {
			value := string(sret.Values[ii].Value)
			offset := sret.Values[ii].Offset
			expectedVal := fmt.Sprintf("value-%04d", ii)
			expectedOffset := base.Offset(iter*batchSize+ii) + initialMeta.StartOffset
			if value != expectedVal {
				logger.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, value)
			}
			if offset != expectedOffset {
				logger.Fatalf("Offset mismatch. Expected offset: %d, Got Offset: %d", expectedOffset, offset)
			}
		}
		logger.Infof("Successfully scanned messages")
	}
	endTs := time.Now().UnixNano()
	f, l := bds.GetMsgTimestampRange()
	if f != firstMsgTs {
		logger.Fatalf("First message timestamp mismatch. Expected: %d, Got: %d", firstMsgTs, f)
	}
	if l != lastMsgTs {
		logger.Fatalf("Last message timestamp mismatch. Expected: %d, Got: %d", lastMsgTs, l)
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
		EndOffset:          bds.nextOffset - 1,
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
	if bds.lastRLogIdx != int64(numIters) {
		logger.Fatalf("Replicated log index mismatch. Expected: %d, got: %d", numIters, bds.lastRLogIdx)
	}
	if metadata.EndOffset != expected.EndOffset {
		logger.Fatalf("End offset mismatch. Expected: %d, Got: %d", expected.EndOffset, metadata.EndOffset)
	}
	f, l = bds.GetMsgTimestampRange()
	if f != firstMsgTs {
		logger.Fatalf("First message timestamp mismatch. Expected: %d, Got: %d", firstMsgTs, f)
	}
	if l != lastMsgTs {
		logger.Fatalf("Last message timestamp mismatch. Expected: %d, Got: %d", lastMsgTs, l)
	}
	err = bds.Close()
	if err != nil {
		logger.Fatalf("Failed to close segment due to err: %s", err.Error())
	}
}

func TestBadgerSegment_Scan(t *testing.T) {
	util.LogTestMarker("TestBadgerSegment_Scan")
	dataDir := util.CreateTestDir(t, "TestBadgerSegment_Scan")
	initialMeta := SegmentMetadata{
		ID:               1,
		Immutable:        false,
		StartOffset:      0,
		EndOffset:        -1,
		CreatedTimestamp: time.Now(),
		ImmutableReason:  0,
	}
	opts := BadgerSegmentOpts{
		RootDir:     dataDir,
		Logger:      nil,
		Topic:       "topic1",
		PartitionID: 1,
		TTLSeconds:  86400,
	}
	bds, err := NewBadgerSegment(&opts)
	if err != nil {
		logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.SetMetadata(initialMeta)
	bds.Open()
	batchSize := 5
	numIters := 5
	payloadSize := 1024 * 1024
	var timestamps []int64
	lastRLogIdx := int64(0)
	tw := newTestWorkload()
	testStart := time.Now().UnixNano()
	for ii := 0; ii < numIters; ii++ {
		now := time.Now().UnixNano()
		tw.appendMessages(base.Offset(ii*batchSize), bds, batchSize, payloadSize, now, lastRLogIdx)
		lastRLogIdx++
		timestamps = append(timestamps, now)
		time.Sleep(time.Millisecond * 100)
	}
	glog.Infof("Timestamps: %v", timestamps)
	glog.Infof("Testing Timestamp scans!")
	for ii := 0; ii < numIters-1; ii++ {
		tw.scanMessagesByTimestamp(bds, batchSize, timestamps[ii], timestamps[ii+1], base.Offset(ii*batchSize), batchSize)
	}
	totalSize := int64((numIters * batchSize) * payloadSize)
	totalIndexes := int64(len(bds.timestampIndex))
	expectedIndexes := totalSize / kIndexEveryNBytes
	if !((totalIndexes == expectedIndexes) || (totalIndexes == expectedIndexes+1) || (totalIndexes == expectedIndexes-1)) {
		glog.Fatalf("Expected around %d indexes. Got: %d", expectedIndexes, totalIndexes)
	}
	if err := bds.Close(); err != nil {
		glog.Fatalf("Error while closing segment: %s", err.Error())
	}
	opts.TTLSeconds = 2
	bds, err = NewBadgerSegment(&opts)
	if err != nil {
		glog.Fatalf("Error while initializing segment: %s", err.Error())
	}
	bds.Open()
	time.Sleep(time.Millisecond*100*time.Duration(numIters) + time.Second*time.Duration(int64(opts.TTLSeconds)))
	totalIndexes = int64(len(bds.timestampIndex))
	if !((totalIndexes == expectedIndexes) || (totalIndexes == expectedIndexes+1) || (totalIndexes == expectedIndexes-1)) {
		glog.Fatalf("Expected around %d indexes. Got: %d", expectedIndexes, totalIndexes)
	}
	// All values should now have expired. Scan the segment and ensure that we don't get any values back.
	for ii := 0; ii < numIters-1; ii++ {
		tw.scanExpiredMessagesByTimestamp(bds, batchSize, timestamps[ii], timestamps[ii+1])
	}
	timestamps = nil
	lastRLogIdx++
	// Write some more messages now and see if these are now scanable. Set the start timestamp to a ts even before
	// we appended the first message. We should still not see the expired messages and segment must start from the
	// first unexpired message.
	for ii := 0; ii < numIters; ii++ {
		now := time.Now().UnixNano()
		tw.appendMessages(tw.lastOffsetAppended+1, bds, batchSize, 1024*1024, now, lastRLogIdx)
		lastRLogIdx++
		timestamps = append(timestamps, now)
	}
	testEnd := time.Now().UnixNano()
	tw.scanMessagesByTimestamp(bds, batchSize*numIters, testStart, testEnd, base.Offset(numIters*batchSize), batchSize*numIters)
	tw.scanMessagesByTimestamp(bds, 1, testStart, testEnd, base.Offset(numIters*batchSize), 1)

	glog.Infof("Testing Offset Scans!")
	tw.scanMessagesByOffset(bds, batchSize, base.Offset(numIters*batchSize), -1, base.Offset(numIters*batchSize), batchSize)
	tw.scanMessagesByOffset(bds, batchSize, base.Offset((numIters-2)*batchSize), -1, base.Offset(numIters*batchSize), batchSize)
	tw.scanMessagesByOffset(bds, batchSize, base.Offset((numIters+1)*batchSize), -1, base.Offset((numIters+1)*batchSize), batchSize)
	tw.scanMessagesByOffset(bds, batchSize*5, base.Offset((numIters-1)*batchSize), timestamps[len(timestamps)-1], base.Offset(numIters*batchSize), (numIters-1)*batchSize)
	tw.scanMessagesByOffset(bds, 1, base.Offset(((numIters+1)*batchSize)+1), -1, base.Offset(((numIters+1)*batchSize)+1), 1)

	totalIndexes = int64(len(bds.timestampIndex))
	expectedIndexes *= 2
	if !((totalIndexes == expectedIndexes) || (totalIndexes == expectedIndexes+1) || (totalIndexes == expectedIndexes-1)) {
		glog.Fatalf("Expected around %d indexes. Got: %d", expectedIndexes, totalIndexes)
	}
}

func TestBadgerSegment_Append(t *testing.T) {
	util.LogTestMarker("TestBadgerSegment_Append")
	dataDir := util.CreateTestDir(t, "TestBadgerSegment_Append")
	initialMeta := SegmentMetadata{
		ID:               100,
		Immutable:        false,
		StartOffset:      10000,
		EndOffset:        -1,
		CreatedTimestamp: time.Now(),
		ImmutableReason:  0,
	}
	opts := BadgerSegmentOpts{
		RootDir:     dataDir,
		Logger:      nil,
		Topic:       "topic1",
		PartitionID: 1,
		TTLSeconds:  86400,
	}
	bds, err := NewBadgerSegment(&opts)
	if err != nil {
		logger.Fatalf("Unable to create badger segment due to err: %s", err.Error())
	}
	bds.SetMetadata(initialMeta)
	bds.Open()
	batchSize := 10
	numIters := 100
	lastRLogIdx := int64(0)
	token := make([]byte, 1024*4)
	rand.Read(token)
	var values [][]byte
	for ii := 0; ii < batchSize; ii++ {
		values = append(values, token)
	}
	now := time.Now().UnixNano()
	var arg AppendEntriesArg
	arg.Entries = values
	arg.Timestamp = now
	start := time.Now()
	for iter := 0; iter < numIters; iter++ {
		lastRLogIdx++
		arg.RLogIdx = lastRLogIdx
		ret := bds.Append(context.Background(), &arg)
		if ret.Error != nil {
			logger.Fatalf("Unable to append values to segment due to err: %s", ret.Error.Error())
		}
	}
	elapsed := time.Since(start)
	glog.Infof("Total time: %v, average time: %v", elapsed, elapsed/time.Duration(numIters))
}
