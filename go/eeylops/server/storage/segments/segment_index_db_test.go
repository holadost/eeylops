package segments

import (
	"eeylops/server/base"
	"eeylops/util"
	"github.com/golang/glog"
	"testing"
	"time"
)

func TestNewSegmentIndexDB(t *testing.T) {
	testName := "TestNewSegmentIndexDB"
	util.LogTestMarker(testName)
	testDir := util.CreateTestDir(t, testName)
	idb := NewSegmentIndexDB(testDir, nil)
	numEntries := 10000
	var totalTime time.Duration
	for ii := 0; ii < numEntries; ii++ {
		now := time.Now()
		err := idb.Add(now.UnixNano(), base.Offset(ii))
		if err != nil {
			glog.Fatalf("Unexpected error while adding entry: %s", err.Error())
		}
		elapsed := time.Since(now)
		totalTime += elapsed
	}
	glog.Infof("Total Add time: %v, Average add time: %v", totalTime, totalTime/time.Duration(numEntries))
}
