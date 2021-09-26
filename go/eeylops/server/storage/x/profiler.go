package main

import (
	"eeylops/server/storage"
	"eeylops/util/testutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(4)
	testDir := testutil.CreateFreshTestDir("TestPartitionStress")
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()

	pMap := make(map[int]*storage.Partition)
	for ii := 0; ii < 5; ii++ {
		opts := storage.PartitionOpts{
			TopicName:                      "topic1",
			PartitionID:                    ii + 1,
			RootDirectory:                  testDir,
			ExpiredSegmentPollIntervalSecs: 1,
			LiveSegmentPollIntervalSecs:    1,
			NumRecordsPerSegmentThreshold:  1e6,
			MaxScanSizeBytes:               15 * 1024 * 1024,
			TTLSeconds:                     86400 * 7,
		}
		p := storage.NewPartition(opts)
		pMap[ii] = p
	}
	psw := storage.NewPartitionStressWorkload(pMap, 0, 512*1024, 10, 2000)
	psw.Start()
	time.Sleep(time.Second * 35)
	psw.Stop()
}
