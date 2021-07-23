package segments

import (
	"database/sql"
	"eeylops/server/base"
	"eeylops/util/logging"
	"github.com/golang/glog"
	"os"
	"path"
)

// SegmentIndexDB keeps the indexes for the segment. Currently, it only has a timestamp based index.
type SegmentIndexDB struct {
	Db       *sql.DB
	Path     string
	RootPath string
	closed   bool
	logger   *logging.PrefixLogger
}

const kCreateTableQuery = "CREATE TABLE IF NOT EXISTS timestamp_index (timestamp INTEGER, offset INTEGER)"
const kAddIndexQuery = "INSERT INTO timestamp_index VALUES ($1, $2)"
const kfetchNearestOffsetQuery = "SELECT offset, LAG(offset, 1) OVER (ORDER BY timestamp DESC) FROM timestamp_index " +
	"WHERE timestamp < $1 LIMIT 1"
const kIndexTimestampQuery = "CREATE INDEX IF NOT EXISTS ts_idx ON timestamp_index(timestamp)"

func NewSegmentIndexDB(dbRootPath string, logger *logging.PrefixLogger) *SegmentIndexDB {
	iDirPath := path.Join(dbRootPath, kIndexDirName)
	dbPath := path.Join(iDirPath, kIndexDBName)
	err := os.MkdirAll(iDirPath, 0774)
	if err != nil {
		glog.Fatalf("Unable to create directory for segment indexes located at: %s", dbRootPath)
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		glog.Fatalf("Unable to open index db located at: %s", dbPath)
		return nil
	}
	idb := new(SegmentIndexDB)
	idb.Db = db
	idb.Path = dbPath
	idb.RootPath = dbRootPath
	idb.closed = false
	idb.logger = logger
	if idb.logger == nil {
		idb.logger = logging.DefaultLogger()
	}
	idb.initialize()
	return idb
}

func (idb *SegmentIndexDB) initialize() {
	idb.logger.VInfof(1, "Initializing index db")
	err := idb.execQuery(kCreateTableQuery)
	if err != nil {
		idb.logger.Fatalf("Unable to create table in index db due to err: %s", err.Error())
	}
	err = idb.execQuery(kIndexTimestampQuery)
	if err != nil {
		idb.logger.Fatalf("Unable to create indexes in index db due to err: %s", err.Error())
	}
}

func (idb *SegmentIndexDB) Close() {
	if idb.closed {
		return
	}
	idb.closed = true
	idb.Db.Close()

}

func (idb *SegmentIndexDB) Add(timestamp int64, offset base.Offset) error {
	err := idb.execQuery(kAddIndexQuery, timestamp, offset)
	if err != nil {
		idb.logger.Errorf("Unable to index (ts:%d, offset:%d) due to err: %s", timestamp, offset, err.Error())
		return err
	}
	return nil
}

func (idb *SegmentIndexDB) GetNearestOffsetLessThan(timestamp int64) (base.Offset, error) {
	pq, err := idb.Db.Prepare(kfetchNearestOffsetQuery)
	if err != nil {
		idb.logger.Errorf("Unable to prepare select query due to err: %s", err.Error())
		return -1, ErrSegmentIndexDBBackend
	}
	defer pq.Close()
	rows, err := pq.Query(timestamp)
	if err != nil {
		idb.logger.Errorf("Unable to fetch last offset with ts < %d due to err: %s", timestamp, err.Error())
		return -1, ErrSegmentIndexDBBackend
	}
	defer rows.Close()
	var offset base.Offset
	var gbg interface{}
	offset = -1
	for rows.Next() {
		err = rows.Scan(&offset, &gbg)
		if err != nil {
			idb.logger.Errorf("Unable to fetch last offset with ts < %d due to row scan err: %s",
				timestamp, err.Error())
			return -1, ErrSegmentIndexDBBackend
		}
		break
	}
	return offset, nil
}

func (idb *SegmentIndexDB) execQuery(query string, args ...interface{}) error {
	tx, err := idb.Db.Begin()
	if err != nil {
		return ErrSegmentIndexDBBackend
	}
	pq, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return ErrSegmentIndexDBBackend
	}
	defer pq.Close()
	if len(args) == 0 {
		_, err = pq.Exec()
	} else {
		_, err = pq.Exec(args...)
	}
	if err != nil {
		tx.Rollback()
		return ErrSegmentIndexDBBackend
	}
	tx.Commit()
	return nil
}
