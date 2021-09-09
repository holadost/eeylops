package storage

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/segments"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

// testOnlyFetchSegments fetches the segments between startIdx and endIdx. This method is for test only purposes.
func (p *Partition) testOnlyFetchSegments(startIdx int, endIdx int) []segments.Segment {
	var segs []segments.Segment
	if startIdx == -1 {
		return segs
	}
	segs = append(segs, p.segments[startIdx])
	if startIdx == endIdx {
		return segs
	}
	if endIdx == -1 {
		endIdx = len(p.segments) - 1
	}
	for ii := startIdx + 1; ii <= endIdx; ii++ {
		segs = append(segs, p.segments[ii])
	}
	return segs
}

func singleProduce(startIdx int, p *Partition, numValues int) {
	var values [][]byte
	for ii := 0; ii < numValues; ii++ {
		values = append(values, []byte(fmt.Sprintf("value-%d", startIdx)))
		startIdx += 1
	}
	ts := time.Now().UnixNano()
	parg := sbase.AppendEntriesArg{
		Entries:   values,
		Timestamp: ts,
		RLogIdx:   ts / (1000),
	}
	glog.Infof("Appending %d values to partition", numValues)
	pret := p.Append(context.Background(), &parg)
	if pret.Error != nil {
		glog.Fatalf("Append failed due to err: %s", pret.Error.Error())
	}
}

func loadDataBasic(p *Partition, numValues int, startIdx int, numIterations int) {
	for ii := 0; ii < numIterations; ii++ {
		iterStartIdx := (ii * numValues) + startIdx
		glog.Infof("Loading %d records. Iteration: %d. Start ID: %d", numValues, ii, iterStartIdx)
		singleProduce(iterStartIdx, p, numValues)
	}
}

func loadDataBasicWithNewSegments(p *Partition, numSegments int, numValuesPerSegment int) {
	for jj := 0; jj < numSegments; jj++ {
		// Produce values.
		startIdx := jj * numValuesPerSegment
		singleProduce(startIdx, p, numValuesPerSegment)
		if numSegments != 1 {
			// Create new segment.
			p.createNewSegmentSafe()
		}
	}
}

func TestPartitionInitialize(t *testing.T) {
	testutil.LogTestMarker("TestPartitionInitialize")
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  "/tmp/eeylops/TestPartitionInitialize",
		ExpiredSegmentPollIntervalSecs: 0,
		LiveSegmentPollIntervalSecs:    0,
		NumRecordsPerSegmentThreshold:  0,
		MaxScanSizeBytes:               0,
		TTLSeconds:                     86400 * 7,
	}
	p := NewPartition(opts)
	glog.Infof("Partition ID: %d", p.partitionID)
	p.Close()
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionInitialize")
	glog.Infof("TestPartitionInitialize finished successfully")
}

func TestPartitionReInitialize(t *testing.T) {
	testutil.LogTestMarker("TestPartitionReInitialize")
	for ii := 0; ii < 5; ii++ {
		glog.Infof("\n\n\nIteration: %d", ii)
		opts := PartitionOpts{
			TopicName:                      "topic1",
			PartitionID:                    1,
			RootDirectory:                  "/tmp/eeylops/TestPartitionReInitialize",
			ExpiredSegmentPollIntervalSecs: 0,
			LiveSegmentPollIntervalSecs:    0,
			NumRecordsPerSegmentThreshold:  0,
			MaxScanSizeBytes:               0,
			TTLSeconds:                     86400 * 7,
		}
		p := NewPartition(opts)
		glog.Infof("Partition ID: %d", p.partitionID)
		p.Close()
	}
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionReInitialize")
	glog.Infof("TestPartitionReInitialize finished successfully")
}

func TestPartitionAppend(t *testing.T) {
	testutil.LogTestMarker("TestPartitionAppend")
	testDir := "/tmp/eeylops/TestPartitionAppend"
	defer func() { _ = os.RemoveAll(testDir) }()
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  testDir,
		ExpiredSegmentPollIntervalSecs: 0,
		LiveSegmentPollIntervalSecs:    0,
		NumRecordsPerSegmentThreshold:  0,
		MaxScanSizeBytes:               0,
		TTLSeconds:                     86400 * 7,
	}
	p := NewPartition(opts)
	loadDataBasic(p, 25, 0, 5)
	glog.Infof("TestPartitionAppend finished successfully")
}

func TestPartitionNewSegmentCreation(t *testing.T) {
	testutil.LogTestMarker("TestPartitionNewSegmentCreation")
	testDir := "/tmp/eeylops/TestPartitionNewSegmentCreation"
	_ = os.RemoveAll(testDir)
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  testDir,
		ExpiredSegmentPollIntervalSecs: 0,
		LiveSegmentPollIntervalSecs:    0,
		NumRecordsPerSegmentThreshold:  0,
		MaxScanSizeBytes:               0,
		TTLSeconds:                     86400 * 7,
	}
	p := NewPartition(opts)
	loadDataBasicWithNewSegments(p, 10, 100)
	segs := p.testOnlyFetchSegments(p.getSegmentsByOffset(10, 50))
	if segs == nil || len(segs) != 1 {
		glog.Fatalf("Expected 1 segment, Got: %d", len(segs))
		return
	}

	// Check if we get a single segment correctly.
	glog.Infof("Testing single getSegment")
	for _, seg := range p.segments {
		meta := seg.GetMetadata()
		glog.Infof("Metadata: %s", meta.ToString())
	}
	tmpMeta := segs[0].GetMetadata()
	id := tmpMeta.ID
	if id != 1 {
		glog.Fatalf("Expected segment ID: 1, got: %d. Metadata: %s", id, tmpMeta.ToString())
	}

	// Test two segments.
	glog.Infof("Testing two getSegment")
	segs = p.testOnlyFetchSegments(p.getSegmentsByOffset(121, 221))
	if segs == nil || len(segs) != 2 {
		glog.Fatalf("Expected 2 segment, Got: %d", len(segs))
		return
	}
	id0 := segs[0].GetMetadata().ID
	id1 := segs[1].GetMetadata().ID
	if id0 != 2 {
		glog.Fatalf("Expected segment ID: 2, got: %d", id0)
	}
	if id1 != 3 {
		glog.Fatalf("Expected segment ID: 3, got: %d", id1)
	}

	// Test multiple segments.
	glog.Infof("Testing multiple getSegment")
	segs = p.testOnlyFetchSegments(p.getSegmentsByOffset(121, 800))
	if segs == nil || len(segs) != 8 {
		glog.Fatalf("Expected 8 segment, Got: %d", len(segs))
		return
	}

	for ii, seg := range segs {
		id = seg.GetMetadata().ID
		expectedID := uint64(ii + 2)
		if id != expectedID {
			glog.Fatalf("Expected segment ID: %d, got: %d", expectedID, id)
		}
	}

	// Test multiple segments with end offset greater than max offset in partition so far(999).
	glog.Infof("Testing multiple getSegment with higher endOffset")
	segs = p.testOnlyFetchSegments(p.getSegmentsByOffset(121, 1200))
	if segs == nil || len(segs) != 10 {
		glog.Fatalf("Expected 10 segments, Got: %d", len(segs))
		return
	}

	for ii, seg := range segs {
		id = seg.GetMetadata().ID
		expectedID := uint64(ii + 2)
		if id != expectedID {
			glog.Fatalf("Expected segment ID: %d, got: %d", expectedID, id)
		}
	}

	// Test start offset greater than any so far.
	glog.Infof("Testing non existent start offset")
	segs = p.testOnlyFetchSegments(p.getSegmentsByOffset(1100, 1200))
	if !(segs == nil || len(segs) == 0) {
		glog.Fatalf("Got %d segment(s) even though it does not contain our record", len(segs))
	}
	glog.Infof("TestPartitionNewSegmentCreation finished successfully")
}

func TestPartitionScan(t *testing.T) {
	testutil.LogTestMarker("TestPartitionScan")
	testDir := testutil.CreateTestDir(t, "TestPartitionScan")
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  testDir,
		ExpiredSegmentPollIntervalSecs: 0,
		LiveSegmentPollIntervalSecs:    0,
		NumRecordsPerSegmentThreshold:  0,
		MaxScanSizeBytes:               0,
		TTLSeconds:                     86400 * 7,
	}
	p := NewPartition(opts)
	numSegs := 10
	numValsPerSeg := 100
	loadDataBasicWithNewSegments(p, numSegs, numValsPerSeg)
	var sarg sbase.ScanEntriesArg
	defCtx := context.Background()
	sarg.StartOffset = 0
	sarg.NumMessages = 1
	sret := p.Scan(defCtx, &sarg)
	if sret.Error != nil {
		glog.Fatalf("Unable to scan first offset due to err: %s", sret.Error.Error())
	}
	if len(sret.Values) != 1 {
		glog.Fatalf("Mismatch. Expected 1 value, got: %d", len(sret.Values))
	}
	gotVal := string(sret.Values[0].Value)
	gotOffset := sret.Values[0].Offset
	if gotVal != "value-0" {
		glog.Fatalf("Value mismatch. Expected: value-0, got: %s", gotVal)
	}
	if gotOffset != 0 {
		glog.Fatalf("Offset mismatch. Expected: 0, got: %d", gotOffset)
	}
	if sret.NextOffset != 1 {
		glog.Fatalf("Offset mismatch. Expected Next Offset: 1, got: %d", gotOffset)
	}

	sarg.StartOffset = 121
	sarg.NumMessages = 100
	sarg.StartTimestamp = -1
	sarg.EndTimestamp = -1
	sret = p.Scan(defCtx, &sarg)
	if sret.Error != nil {
		glog.Fatalf("Unexpected scan error: %s", sret.Error.Error())
	}
	if len(sret.Values) != int(sarg.NumMessages) {
		glog.Fatalf("Expected: %d values, got: %d", int(sarg.NumMessages), len(sret.Values))
	}
	for ii, val := range sret.Values {
		expectedOffset := base.Offset(ii) + sarg.StartOffset
		expectedVal := fmt.Sprintf("value-%d", expectedOffset)
		gotVal = string(val.Value)
		if expectedVal != gotVal {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, gotVal)
		}
		if expectedOffset != val.Offset {
			glog.Fatalf("Offset mismatch. Expected: %d, got: %d", expectedOffset, val.Offset)
		}
	}
	expectedNextOffset := sarg.StartOffset + base.Offset(sarg.NumMessages)
	if sret.NextOffset != expectedNextOffset {
		glog.Fatalf("Offset mismatch. Expected next offset: %d, got: %d", expectedNextOffset, sret.NextOffset)
	}
	glog.Infof("Finished scanning 121 to 221. Starting again from 221")

	sarg.StartOffset = 221
	sarg.NumMessages = uint64(numSegs * numValsPerSeg)
	expectedNumMsgs := (numSegs * numValsPerSeg) - int(sarg.StartOffset)
	sret = p.Scan(defCtx, &sarg)
	if sret.Error != nil {
		glog.Fatalf("Unexpected error while scanning. Error: %s", sret.Error.Error())
	}
	if len(sret.Values) != expectedNumMsgs {
		glog.Fatalf("Num messages mismatch. Expected: %d, got: %d", expectedNumMsgs, len(sret.Values))
	}
	for ii, val := range sret.Values {
		expectedOffset := base.Offset(ii) + sarg.StartOffset
		expectedVal := fmt.Sprintf("value-%d", expectedOffset)
		gotVal = string(val.Value)
		if expectedVal != gotVal {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, gotVal)
		}
		if expectedOffset != val.Offset {
			glog.Fatalf("Offset mismatch. Expected: %d, got: %d", expectedOffset, val.Offset)
		}
	}
	if sret.NextOffset != base.Offset(numSegs*numValsPerSeg) {
		glog.Fatalf("Expected next offset: %d, got: %d", base.Offset(numSegs*numValsPerSeg), sret.NextOffset)
	}
	glog.Infof("Finished scanning 221 as well. Up next, scanning non existent offsets")

	// Scan non existent offsets.
	sarg.StartOffset = base.Offset(numSegs * numValsPerSeg)
	sarg.NumMessages = 10
	sret = p.Scan(defCtx, &sarg)
	if sret.Error != nil {
		glog.Fatalf("Unexpected error while scanning. Error: %s", sret.Error.Error())
	}
	if len(sret.Values) != 0 {
		glog.Fatalf("Num messages mismatch. Expected: 0, got: %d", len(sret.Values))
	}
	if sret.NextOffset != -1 {
		glog.Fatalf("Expected next offset: -1, got: %d", sret.NextOffset)
	}

	// Scan the last offset.
	sarg.StartOffset = base.Offset(numSegs*numValsPerSeg) - 1
	sarg.NumMessages = 10
	sret = p.Scan(defCtx, &sarg)
	if sret.Error != nil {
		glog.Fatalf("Unexpected error while scanning. Error: %s", sret.Error.Error())
	}
	if len(sret.Values) != 1 {
		glog.Fatalf("Num messages mismatch. Expected: 1, got: %d", len(sret.Values))
	}
	if sret.NextOffset != base.Offset(numSegs*numValsPerSeg) {
		glog.Fatalf("Expected next offset: %d, got: %d", base.Offset(numSegs*numValsPerSeg), sret.NextOffset)
	}
	glog.Infof("TestPartitionScan finished successfully")
}

func TestPartitionManager(t *testing.T) {
	testutil.LogTestMarker("TestPartitionManager")
	testDir := testutil.CreateTestDir(t, "TestPartitionManager")
	totalValues := 500
	numValuesPerBatch := 50
	valueSizeBytes := 100
	totalIters := totalValues / numValuesPerBatch
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  testDir,
		ExpiredSegmentPollIntervalSecs: 1,
		LiveSegmentPollIntervalSecs:    1,
		NumRecordsPerSegmentThreshold:  100,
		MaxScanSizeBytes:               1500,
		TTLSeconds:                     86400 * 7,
	}
	defCtx := context.Background()
	sleepTime := 500*time.Millisecond + time.Duration(opts.ExpiredSegmentPollIntervalSecs)*time.Millisecond*1000
	opts.TTLSeconds = int((time.Duration(totalIters) * sleepTime) / time.Second)
	p := NewPartition(opts)
	defer p.Close()
	// Test live segment scans.
	for iter := 0; iter < totalIters; iter++ {
		glog.Infof("Sleeping for %v seconds to allow manager to scan live segment", sleepTime)
		time.Sleep(sleepTime)
		var values [][]byte
		for ii := 0; ii < numValuesPerBatch; ii++ {
			token := make([]byte, valueSizeBytes)
			rand.Read(token)
			values = append(values, token)
		}
		aarg := sbase.AppendEntriesArg{
			Entries:   values,
			Timestamp: time.Now().UnixNano(),
			RLogIdx:   int64(iter + 1),
		}
		aret := p.Append(defCtx, &aarg)
		if aret.Error != nil {
			glog.Fatalf("Append failed due to err: %s", aret.Error.Error())
		}
	}
	time.Sleep(sleepTime)
	if len(p.segments) != totalValues/opts.NumRecordsPerSegmentThreshold+1 {
		glog.Fatalf("Expected %d segments, got: %d", totalValues/opts.NumRecordsPerSegmentThreshold+1,
			len(p.segments))
	}

	// Test scans with max scan size bytes.
	var sarg sbase.ScanEntriesArg
	sarg.StartOffset = 0
	sarg.NumMessages = 20
	sarg.StartTimestamp = -1
	sarg.EndTimestamp = -1
	sret := p.Scan(defCtx, &sarg)
	expectedNumValues := opts.MaxScanSizeBytes / valueSizeBytes
	if sret.Error != nil {
		glog.Fatalf("Unexpected error while scanning: %s", sret.Error.Error())
	}
	if len(sret.Values) > expectedNumValues {
		glog.Fatalf("Expected <= %d messages but we got: %d", expectedNumValues, len(sret.Values))
	}

	// Test segment expiry.
	currNumSegs := len(p.segments)
	time.Sleep(sleepTime * 5)
	for iter := 0; iter < totalValues/opts.NumRecordsPerSegmentThreshold; iter++ {
		p.Close()
		p = NewPartition(opts)
		if len(p.segments) == 1 {
			break
		}
		if len(p.segments) >= currNumSegs {
			glog.Fatalf("Found %d segments. Expected < %d segments", len(p.segments), currNumSegs)
		}
		currNumSegs = len(p.segments)
		time.Sleep(sleepTime * 2)
	}
	var values [][]byte
	for ii := 0; ii < numValuesPerBatch; ii++ {
		token := make([]byte, valueSizeBytes)
		rand.Read(token)
		values = append(values, token)
	}

	// Append more values to the partition. The offset should start from totalValues.
	glog.Infof("Appending messages after all segments have expired!")
	aarg := sbase.AppendEntriesArg{
		Entries:   values,
		Timestamp: time.Now().UnixNano(),
		RLogIdx:   int64(101),
	}
	aret := p.Append(defCtx, &aarg)
	if aret.Error != nil {
		glog.Fatalf("Append failed due to err: %s", aret.Error.Error())
	}

	glog.Infof("Scanning messages after all segments have expired and new messages were appended")
	sarg.StartOffset = base.Offset(totalValues)
	sarg.NumMessages = 20
	sret = p.Scan(defCtx, &sarg)
	expectedNumValues = opts.MaxScanSizeBytes / valueSizeBytes
	expectedNextOffset := sarg.StartOffset + base.Offset(expectedNumValues)
	if sret.Error != nil {
		glog.Fatalf("Unexpected error while scanning: %s", sret.Error.Error())
	}
	if len(sret.Values) != expectedNumValues && len(sret.Values) != expectedNumValues-1 {
		glog.Fatalf("Expected only (%d or %d) messages but we got: %d", expectedNumValues-1,
			expectedNumValues, len(sret.Values))
	}
	if sret.NextOffset != expectedNextOffset {
		glog.Fatalf("Expected next offset: %d, got: %d", expectedNextOffset, sret.NextOffset)
	}
	p.Close()
}

func TestPartitionScanTimestamp(t *testing.T) {
	testutil.LogTestMarker("TestPartitionScanTimestamp")
	testDir := testutil.CreateTestDir(t, "TestPartitionScanTimestamp")
	runtime.GOMAXPROCS(16)
	ttlSeconds := 30
	opts := PartitionOpts{
		TopicName:                      "topic1",
		PartitionID:                    1,
		RootDirectory:                  testDir,
		ExpiredSegmentPollIntervalSecs: 1,
		LiveSegmentPollIntervalSecs:    1,
		NumRecordsPerSegmentThreshold:  1000,
		MaxScanSizeBytes:               15 * 1024 * 1024,
		TTLSeconds:                     ttlSeconds,
	}
	p := NewPartition(opts)
	numIters := 5000
	batchSize := 10
	var timestamps []int64
	token := make([]byte, 1024)
	rand.Read(token)
	producer := func() {
		timestamps = nil
		for ii := 0; ii < numIters; ii++ {
			now := time.Now().UnixNano()
			var arg sbase.AppendEntriesArg
			for jj := 0; jj < batchSize; jj++ {
				val := fmt.Sprintf("value-%07d", (ii*batchSize)+jj)
				arg.Entries = append(arg.Entries, append([]byte(val), token...))
			}
			arg.RLogIdx = now
			arg.Timestamp = now
			timestamps = append(timestamps, now)
			ret := p.Append(context.Background(), &arg)
			if ret.Error != nil {
				glog.Fatalf("Append error: %s", ret.Error.Error())
			}
		}
	}
	start := time.Now()
	producer()
	glog.Infof("Producer done. Total time: %v", time.Since(start))
	glog.Infof("Starting consumer!")
	var sarg sbase.ScanEntriesArg
	sarg.NumMessages = uint64(batchSize)
	sarg.StartOffset = -1
	defCtx := context.Background()
	now := time.Now()
	for ii := 1; ii < len(timestamps); ii++ {
		sarg.StartTimestamp = timestamps[ii-1]
		sarg.EndTimestamp = timestamps[ii]
		sret := p.Scan(defCtx, &sarg)
		if sret.Error != nil {
			glog.Fatalf("Unable to scan first offset due to err: %s", sret.Error.Error())
		}
		if len(sret.Values) != batchSize {
			glog.Fatalf("Mismatch. Expected %d values, got: %d. Iteration: %d", batchSize, len(sret.Values), ii)
		}
		for jj := 0; jj < batchSize; jj++ {
			expected := base.Offset(((ii - 1) * batchSize) + jj)
			if sret.Values[jj].Offset != expected {
				glog.Fatalf("Offset mismatch. Expected: %d, got: %d", expected, sret.Values[jj].Offset)
			}
		}
	}
	glog.Infof("Consumer done. Total time: %v", time.Since(now))
	oldTimestamps := append([]int64{}, timestamps...)
	glog.Infof("Waiting for some segments to expire but not all")
	time.Sleep(time.Duration(ttlSeconds-8) * time.Second)

	glog.Infof("Some or all segments must have expired by now. Starting producer again")
	producer()
	glog.Infof("Producer has finished")

	glog.Infof("Starting consumer but checking with old timestamps!")
	now = time.Now()
	sarg.NumMessages = uint64(batchSize)
	sarg.StartOffset = -1
	for ii := 1; ii < len(oldTimestamps); ii++ {
		sarg.StartTimestamp = oldTimestamps[ii-1]
		sarg.EndTimestamp = oldTimestamps[ii]
		sret := p.Scan(defCtx, &sarg)
		if sret.Error != nil {
			glog.Fatalf("Unable to scan first offset due to err: %s", sret.Error.Error())
		}
		if len(sret.Values) != 0 {
			glog.Fatalf("Mismatch. Expected 0 values, got: %d. Iteration: %d", len(sret.Values), ii)
		}
		if sret.NextOffset != -1 {
			glog.Fatalf("Next offset mismatch. Expected: %d, got: %d, iteration: %d",
				-1, sret.NextOffset, ii)
		}
	}
	glog.Infof("Consumer with old timestamps finished. Total Time: %v", time.Since(now))

	glog.Infof("Starting consumer but checking with old start timestamps but new end timestamp")
	now = time.Now()
	sarg.NumMessages = uint64(batchSize)
	sarg.StartOffset = -1
	for ii := 1; ii < len(oldTimestamps); ii++ {
		sarg.StartTimestamp = oldTimestamps[ii-1]
		sarg.EndTimestamp = timestamps[ii]
		sret := p.Scan(defCtx, &sarg)
		if sret.Error != nil {
			glog.Fatalf("Unable to scan first offset due to err: %s", sret.Error.Error())
		}
		if len(sret.Values) != batchSize {
			glog.Fatalf("Mismatch. Expected %d values, got: %d. Iteration: %d", batchSize, len(sret.Values), ii)
		}
		for jj := 0; jj < batchSize; jj++ {
			expected := base.Offset(numIters*batchSize + jj)
			got := sret.Values[jj].Offset
			if got != expected {
				glog.Fatalf("Offset mismatch. Expected: %d, got: %d, iteration: %d, Sarg: %v",
					expected, got, ii, sarg)
			}
		}
	}
	glog.Infof("Consumer with old and new timestamps done. Total Time: %v", time.Since(now))

	glog.Infof("Starting consumer but checking with new start and end timestamps")
	now = time.Now()
	for ii := 1; ii < len(timestamps); ii++ {
		sarg.StartTimestamp = timestamps[ii-1]
		sarg.EndTimestamp = timestamps[ii]
		sret := p.Scan(defCtx, &sarg)
		if sret.Error != nil {
			glog.Fatalf("Unable to scan first offset due to err: %s", sret.Error.Error())
		}
		if len(sret.Values) != batchSize {
			glog.Fatalf("Mismatch. Expected %d values, got: %d. Iteration: %d", batchSize, len(sret.Values), ii)
		}
		for jj := 0; jj < batchSize; jj++ {
			expected := base.Offset(numIters*batchSize) + base.Offset(((ii-1)*batchSize)+jj)
			if sret.Values[jj].Offset != expected {
				glog.Fatalf("Offset mismatch. Expected: %d, got: %d", expected, sret.Values[jj].Offset)
			}
		}
	}
	glog.Infof("Consumer has finished with new timestamps. Total Time: %v", time.Since(now))
	glog.Infof("Partition timestamp test finished successfully!")
}
