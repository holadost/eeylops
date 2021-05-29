package storage

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createTestPartitionDir(t *testing.T, testName string) string {
	return ""
}

func checkPartitionMetadata(t *testing.T) {

}

func testMarker(testName string) {
	glog.Infof("\n\n============================= %s =============================\n\n", testName)
}

func singleProduce(startIdx int, p *Partition, numValues int) {
	var values [][]byte
	for ii := 0; ii < numValues; ii++ {
		values = append(values, []byte(fmt.Sprintf("value-%d", startIdx)))
		startIdx += 1
	}
	err := p.Append(values)
	if err != nil {
		glog.Fatalf("Append failed due to err: %s", err.Error())
	}
}

func loadDataBasic(p *Partition, numSegments int, numValuesPerSegment int) {
	for jj := 0; jj < numSegments; jj++ {
		// Produce values.
		startIdx := jj * numValuesPerSegment
		singleProduce(startIdx, p, numValuesPerSegment)

		if numSegments != 1 {
			// Create new segment.
			p.createNewSegment()
		}
	}
}

func TestPartitionInitialize(t *testing.T) {
	testMarker("TestPartitionInitialize")
	p := NewPartition(0, "/tmp/eeylops/TestPartitionInitialize", 86400*7)
	glog.Infof("Partition ID: %d", p.partitionID)
	p.Close()
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionInitialize")
	glog.Infof("TestPartitionInitialize finished successfully")
}

func TestPartitionReInitialize(t *testing.T) {
	testMarker("TestPartitionReInitialize")
	for ii := 0; ii < 5; ii++ {
		glog.Infof("\n\n\nIteration: %d", ii)
		p := NewPartition(0, "/tmp/eeylops/TestPartitionReInitialize", 86400*7)
		glog.Infof("Partition ID: %d", p.partitionID)
		p.Close()
	}
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionReInitialize")
	glog.Infof("TestPartitionReInitialize finished successfully")
}

func TestPartitionAppend(t *testing.T) {
	testMarker("TestPartitionAppend")
	testDir := "/tmp/eeylops/TestPartitionAppend"
	defer func() { _ = os.RemoveAll(testDir) }()
	p := NewPartition(0, testDir, 86400*7)
	loadDataBasic(p, 1, 100)
	glog.Infof("TestPartitionAppend finished successfully")
}

func TestPartitionNewSegmentCreation(t *testing.T) {
	testMarker("TestPartitionNewSegmentCreation")
	testDir := "/tmp/eeylops/TestPartitionNewSegmentCreation"
	_ = os.RemoveAll(testDir)
	p := NewPartition(0, testDir, 86400*7)
	loadDataBasic(p, 10, 100)
	segs := p.getSegments(10, 50)
	if segs == nil || len(segs) != 1 {
		glog.Fatalf("Expected 1 segment, Got: %d", len(segs))
		return
	}

	// Check if we get a single segment correctly.
	glog.Infof("Testing single getSegment")
	id := segs[0].GetMetadata().ID
	if id != 1 {
		glog.Fatalf("Expected segment ID: 1, got: %d", id)
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
	testMarker("TestPartitionScan")
	testDir := "/tmp/eeylops/TestPartitionScan"
	_ = os.RemoveAll(testDir)
	p := NewPartition(0, testDir, 86400*7)
	numSegs := 10
	numValsPerSeg := 100
	loadDataBasic(p, numSegs, numValsPerSeg)
	values, errs := p.Scan(0, 1)
	if errs == nil || len(errs) == 0 {
		glog.Fatalf("Did not receive any error codes for scan")
		return
	}
	if len(errs) != 1 {
		glog.Fatalf("Expected 1 value, got: %d", len(errs))
		return
	}
	if errs[0] != nil {
		glog.Fatalf("Unable to scan first offset due to err: %s", errs[0].Error())
		return
	}
	if string(values[0]) != "value-0" {
		glog.Fatalf("Value mismatch. Expected: value-0-0, got: %s", string(values[0]))
	}

	startOffset := 121
	numMsgs := 100
	values, errs = p.Scan(uint64(startOffset), uint64(numMsgs))
	if errs == nil || len(errs) != numMsgs {
		if errs != nil {
			glog.Fatalf("Got %d errs. Expected: %d", len(errs), numMsgs)
		}
		glog.Fatalf("Got nil errs")
		return
	}
	for ii, err := range errs {
		if err != nil {
			glog.Fatalf("Unexpected error while scanning offset: %d. Error: %s", ii+startOffset, err.Error())
			return
		}

		expectedVal := fmt.Sprintf("value-%d", ii+startOffset)
		gotVal := string(values[ii])

		if expectedVal != gotVal {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, gotVal)
			return
		}
	}

	startOffset = 221
	numMsgs = (numSegs * numValsPerSeg) + startOffset
	expectedNumMsgs := (numSegs * numValsPerSeg) - startOffset
	values, errs = p.Scan(uint64(startOffset), uint64(numMsgs))
	if errs == nil || len(errs) != expectedNumMsgs {
		if errs != nil {
			glog.Fatalf("Got %d errs. Expected: %d", len(errs), numMsgs)
		}
		glog.Fatalf("Got nil errs")
		return
	}
	for ii, err := range errs {
		if err != nil {
			glog.Fatalf("Unexpected error while scanning offset: %d. Error: %s", ii+startOffset, err.Error())
			return
		}

		expectedVal := fmt.Sprintf("value-%d", ii+startOffset)
		gotVal := string(values[ii])

		if expectedVal != gotVal {
			glog.Fatalf("Value mismatch. Expected: %s, Got: %s", expectedVal, gotVal)
			return
		}
	}
	glog.Infof("TestPartitionScan finished successfully")
}

func TestStress(t *testing.T) {

}
