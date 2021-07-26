package storage

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"math/rand"
	"os"
	"testing"
	"time"
)

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
	util.LogTestMarker("TestPartitionInitialize")
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
	util.LogTestMarker("TestPartitionReInitialize")
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
	util.LogTestMarker("TestPartitionAppend")
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
	util.LogTestMarker("TestPartitionNewSegmentCreation")
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
	segs := p.getSegments(10, 50)
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
	segs = p.getSegments(121, 221)
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
	segs = p.getSegments(121, 800)
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
	segs = p.getSegments(121, 1200)
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
	segs = p.getSegments(1100, 1200)
	if !(segs == nil || len(segs) == 0) {
		glog.Fatalf("Got %d segment(s) even though it does not contain our record", len(segs))
	}
	glog.Infof("TestPartitionNewSegmentCreation finished successfully")
}

func TestPartitionScan(t *testing.T) {
	util.LogTestMarker("TestPartitionScan")
	testDir := util.CreateTestDir(t, "TestPartitionScan")
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
	util.LogTestMarker("TestPartitionManager")
	testDir := util.CreateTestDir(t, "TestPartitionManager")
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

func TestPartitionStress(t *testing.T) {
	util.LogTestMarker("TestPartitionStress")
	testDir := util.CreateTestDir(t, "TestPartitionStress")
	pMap := make(map[int]*Partition)
	for ii := 0; ii < 5; ii++ {
		opts := PartitionOpts{
			TopicName:                      "topic1",
			PartitionID:                    ii + 1,
			RootDirectory:                  testDir,
			ExpiredSegmentPollIntervalSecs: 1,
			LiveSegmentPollIntervalSecs:    1,
			NumRecordsPerSegmentThreshold:  10000,
			MaxScanSizeBytes:               15 * 1024 * 1024,
			TTLSeconds:                     86400 * 7,
		}
		p := NewPartition(opts)
		pMap[ii] = p
	}
	psw := newPartitionStressWorkload(pMap, 1, 4096, 10, 2000)
	psw.start()
	time.Sleep(time.Second * 20)
	psw.stop()
}

type partitionStressWorkload struct {
	partitionMap             map[int]*Partition
	topicName                string
	numConsumersPerPartition int
	doneChan                 chan struct{}
	dataSizeBytes            int
	batchSize                int
	consumerDelayMs          time.Duration // Consumer delay in ms.
}

func newPartitionStressWorkload(partitionMap map[int]*Partition, numConsumerPerPartition int, payloadSizeBytes int, batchSize int, consumerDelayMs int) *partitionStressWorkload {
	psw := new(partitionStressWorkload)
	psw.numConsumersPerPartition = 2
	psw.doneChan = make(chan struct{})
	psw.dataSizeBytes = payloadSizeBytes
	psw.partitionMap = partitionMap
	psw.numConsumersPerPartition = numConsumerPerPartition
	psw.batchSize = batchSize
	psw.consumerDelayMs = time.Duration(consumerDelayMs) * time.Millisecond
	return psw
}

func (psw *partitionStressWorkload) start() {
	glog.Infof("Starting workloads")
	for key, _ := range psw.partitionMap {
		go psw.producer(key)
		for ii := 0; ii < psw.numConsumersPerPartition; ii++ {
			go psw.consumer(key, ii)
		}
	}
}

func (psw *partitionStressWorkload) stop() {
	glog.Infof("Stopping workloads")
	close(psw.doneChan)
}

func (psw *partitionStressWorkload) producer(partitionID int) {
	token := make([]byte, psw.dataSizeBytes)
	rand.Read(token)
	var entries [][]byte
	for ii := 0; ii < psw.batchSize; ii++ {
		entries = append(entries, token)
	}
	ticker := time.NewTicker(time.Millisecond * 5)
	for {
		select {
		case <-psw.doneChan:
			glog.Infof("Producer for partition: %d exiting", partitionID)
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			arg := sbase.AppendEntriesArg{
				Entries:   entries,
				Timestamp: now,
				RLogIdx:   now,
			}
			p := psw.partitionMap[partitionID]
			ret := p.Append(context.Background(), &arg)
			if ret.Error != nil {
				glog.Fatalf("Unexpected error while appending to partition: %d", partitionID)
			}
		}
	}
}

func (psw *partitionStressWorkload) consumer(partitionID int, consumerID int) {
	for {
		time.Sleep(psw.consumerDelayMs)
		scanner := NewPartitionScanner(psw.partitionMap[partitionID], 0)
		count := 0
		for scanner.Rewind(); scanner.Valid(); scanner.Next() {
			item, err := scanner.Get()
			if err != nil {
				glog.Fatalf("Unexpected error from consumer: %d while scanning partition: %d", consumerID, partitionID)
			}
			if item.Offset != base.Offset(count) {
				glog.Fatalf("Wrong message got from partition. Expected: %d, got: %d", count, item.Offset)
			}
			count++
		}
		_, ok := <-psw.doneChan
		if !ok {
			glog.Infof("Consumer: %d, partition: %d exiting", consumerID, partitionID)
		}
	}
}
