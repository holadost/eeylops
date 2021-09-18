package base

import (
	"flag"
	"github.com/golang/glog"
)

var (
	dataDir = flag.String("data_directory", "", "Data directory for eeylops")
)

func GetDataDirectory() string {
	if *dataDir == "" {
		glog.Fatalf("Undefined data directory")
		return ""
	}
	return *dataDir
}

type Offset int64
type TopicIDType int
type PartitionIDType int
