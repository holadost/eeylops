package segments

import (
	"eeylops/server/base"
	"eeylops/util"
	"github.com/golang/glog"
	"math/rand"
	"testing"
	"time"
)

func TestNewSegmentIndexDB(t *testing.T) {
	testName := "TestNewSegmentIndexDB"
	util.LogTestMarker(testName)
	testDir := util.CreateTestDir(t, testName)
	idb := NewSegmentIndexDB(testDir, nil)
	numEntries := 1000
	var totalAddTime time.Duration
	var totalGetTime time.Duration
	var firstAddTime time.Time
	for ii := 0; ii < numEntries; ii++ {
		// Add entries
		addnow := time.Now()
		if ii == 0 {
			firstAddTime = addnow
		}
		// We see roughly 3ms for add records on NVME drives.
		err := idb.Add(addnow.UnixNano(), base.Offset(ii))
		if err != nil {
			glog.Fatalf("Unexpected error while adding entry: %s", err.Error())
		}
		totalAddTime += time.Since(addnow)
		if (ii != 0) && (ii%5000) == 0 {
			glog.Infof("Num Iters so far: %d, Total Add time: %v, Average add time: %v",
				ii, totalAddTime, totalAddTime/time.Duration(ii+1))
		}

		// Get nearest offset.
		var ts int64
		if ii < 10 {
			ts = addnow.UnixNano()
		} else {
			ts = rand.Int63n(addnow.UnixNano()-firstAddTime.UnixNano()) + firstAddTime.UnixNano()
		}
		getnow := time.Now()
		// This takes approximately 60us on NVME drives.
		offset, err := idb.GetNearestOffsetLessThan(ts)
		if err != nil {
			glog.Fatalf("Unexpected error while getting nearest offset less than: %d. Err: %s", ts, err.Error())
		}
		totalGetTime += time.Since(getnow)

		if (ii >= 10) && (offset < 0 || offset > base.Offset(ii)) {
			glog.Fatalf("Did not get the correct offset. Expected > 0 and < %d, got: %d", ii, offset)
		}
		if (ii != 0) && (ii%5000) == 0 {
			glog.Infof("Num Iters so far: %d, Total Get time: %v, Average Get time: %v, Current Iter Offset: %d",
				ii, totalGetTime, totalGetTime/time.Duration(ii+1), offset)
		}
	}
	glog.Infof("Total Add time: %v, Average Add time: %v", totalAddTime, totalAddTime/time.Duration(numEntries))
	glog.Infof("Total Get time: %v, Average Get time: %v", totalGetTime, totalGetTime/time.Duration(numEntries))
}
