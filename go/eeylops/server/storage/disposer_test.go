package storage

import (
	"eeylops/util"
	"eeylops/util/testutil"
	"github.com/golang/glog"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
)

func TestDisposer(t *testing.T) {
	testutil.LogTestMarker("TestDisposer")
	testDir := testutil.CreateFreshTestDir("TestDisposer")
	ds := DefaultDisposer()
	wg := sync.WaitGroup{}
	var dirs []string
	numDirs := 5
	for ii := 0; ii < numDirs; ii++ {
		myDir := path.Join(testDir, strconv.Itoa(ii))
		dirs = append(dirs, myDir)
		util.CreateDir(myDir)
	}
	cb := func(err error) {
		if err != nil {
			glog.Fatalf("Unable to delete directory due to err: %s", err.Error())
		}
		wg.Done()
	}
	for ii := 0; ii < numDirs; ii++ {
		wg.Add(1)
		ds.Dispose(dirs[ii], cb)
	}
	glog.Infof("Waiting for directories to get disposed")
	wg.Wait()

	glog.Infof("Checking if directories still exist")
	for ii := 0; ii < numDirs; ii++ {
		_, err := os.Stat(dirs[ii])
		if err == nil {
			glog.Fatalf("Found dir even though it should have disposed: %s", dirs[ii])
		}
		glog.Infof("Err: %v", err)
	}
	glog.Infof("Disposer test finished successfully")
}
