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

func loadDataBasic(p *Partition, numSegments int, numValuesPerSegment int) {
	for jj := 0; jj < numSegments; jj++ {
		// Generate values.
		var values [][]byte
		for ii := 0; ii < numValuesPerSegment; ii++ {
			values = append(values, []byte(fmt.Sprintf("value-%d-%d", jj, ii)))
		}

		// Append values.
		err := p.Append(values)
		if err != nil {
			glog.Fatalf("Append failed due to err: %s", err.Error())
		}

		// Create new segment.
		p.createNewSegment()
		if len(p.segments) != jj+2 {
			glog.Fatalf("Expected %d segments. Got: %d", jj+2, len(p.segments))
			return
		}
	}
}

func TestPartitionInitialize(t *testing.T) {
	p := NewPartition(0, "/tmp/eeylops/TestPartitionInitialize", 86400*7)
	glog.Infof("Partition ID: %d", p.partitionID)
	p.Close()
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionInitialize")
}

func TestPartitionReInitialize(t *testing.T) {
	for ii := 0; ii < 5; ii++ {
		glog.Infof("\n\n\nIteration: %d", ii)
		p := NewPartition(0, "/tmp/eeylops/TestPartitionReInitialize", 86400*7)
		glog.Infof("Partition ID: %d", p.partitionID)
		p.Close()
	}
	_ = os.RemoveAll("/tmp/eeylops/TestPartitionReInitialize")
}

func TestPartitionAppend(t *testing.T) {
	testDir := "/tmp/eeylops/TestPartitionAppend"
	defer func() { _ = os.RemoveAll(testDir) }()
	p := NewPartition(0, testDir, 86400*7)
	var values [][]byte
	for ii := 0; ii < 20; ii++ {
		values = append(values, []byte(fmt.Sprintf("value-%d", ii)))
	}
	err := p.Append(values)
	if err != nil {
		glog.Fatalf("Append failed due to err: %s", err.Error())
	}
}

func TestPartitionScan(t *testing.T) {

}

func TestPartitionNewSegmentCreation(t *testing.T) {
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
}

func TestStress(t *testing.T) {

}
