package segments

import (
	"eeylops/server/base"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	"os"
	"path"
)

// SegmentIndexDB keeps the indexes for the segment. Currently, it only has a timestamp based index.
type SegmentIndexDB struct {
	Db       *gorm.DB
	Path     string
	RootPath string
	closed   bool
}

const kCreateTableQuery = "CREATE TABLE IF NOT EXISTS timestamp_index (timestamp INTEGER, offset INTEGER)"
const kAddIndexQuery = "INSERT INTO timestamp_index VALUES (%d, %d)"
const kfetchNearestOffsetQuery = "SELECT offset, LAG(offset, 1) OVER (ORDER BY timestamp DESC) WHERE timestamp < %d " +
	"LIMIT 1"
const kIndexTimestampQuery = "CREATE INDEX IF NOT EXISTS ts_idx ON timestamp_index(timestamp)"

func NewSegmentIndexDB(dbRootPath string) *SegmentIndexDB {
	mdirPath := path.Join(dbRootPath, metadataDirName)
	dbPath := path.Join(mdirPath, metadataDbName)
	err := os.MkdirAll(mdirPath, 0774)
	if err != nil {
		glog.Fatalf("Unable to create directory for segment metadata located at: %s", dbRootPath)
	}
	db, err := gorm.Open("sqlite3", dbPath)
	if err != nil {
		glog.Fatalf("Unable to open metadata ddb located at: %s", dbPath)
		return nil
	}
	idb := new(SegmentIndexDB)
	idb.Db = db
	idb.Path = dbPath
	idb.RootPath = dbRootPath
	idb.closed = false
	return idb
}

func (idb *SegmentIndexDB) Close() {
	if idb.closed {
		return
	}
	idb.closed = true
	idb.Db.Close()

}

func (idb *SegmentIndexDB) Add(timestamp int64, offset base.Offset) error {
	return nil
}

func (idb *SegmentIndexDB) GetNearestOffsetLessThan(timestamp int64) base.Offset {
	return -1
}
