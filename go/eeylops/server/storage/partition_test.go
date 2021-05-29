package storage

import (
	"github.com/golang/glog"
	"os"
	"testing"
)

func createTestPartitionDir(t *testing.T, testName string) string {
	return ""
}

func checkPartitionMetadata(t *testing.T) {

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

}

func TestPartitionScan(t *testing.T) {

}

func TestPartitionNewSegmentCreation(t *testing.T) {

}

func TestStress(t *testing.T) {

}
