package storage

import (
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	"os"
	"path"
	"strings"
)

// segmentMetadataDB persists the segment metadata.
type segmentMetadataDB struct {
	Db       *gorm.DB
	Path     string
	RootPath string
	closed   bool
}

type metadataModel struct {
	Key   string `gorm:"type:varchar(100);PRIMARY_KEY" json:"key"`
	Value []byte `gorm:"type:BLOB;NOT NULL" json:"value"`
}

func newSegmentMetadataDB(dbRootPath string) *segmentMetadataDB {
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
	mdb := new(segmentMetadataDB)
	mdb.Db = db
	mdb.Path = dbPath
	mdb.RootPath = dbRootPath
	mdbModel := metadataModel{}
	dbc := mdb.Db.AutoMigrate(&mdbModel)
	if dbc != nil && dbc.Error != nil {
		glog.Fatalf("Unable to create and initialize metadata ddb located at: %s, due to err: %v",
			mdb.Path, dbc.Error)
	}
	mdb.closed = false
	return mdb
}

func (mdb *segmentMetadataDB) Close() {
	if mdb.closed {
		return
	}
	mdb.closed = true
	mdb.Db.Close()

}

func (mdb *segmentMetadataDB) PutMetadata(metadata *SegmentMetadata) {
	var val metadataModel
	mm := &metadataModel{
		Key:   metadataKeyName,
		Value: metadata.Serialize(),
	}
	tx := mdb.Db.Begin()
	// defer tx.Close()
	dbc := tx.Where("key = ?", metadataKeyName).First(&val)
	if dbc.Error != nil {
		if !strings.Contains(dbc.Error.Error(), "record not found") {
			tx.Rollback()
			glog.Fatalf("Unable to get metadata due to err: %s", dbc.Error.Error())
		} else {
			glog.Infof("Inserting metadata for segment located at: %s", mdb.RootPath)
			dbc = tx.Create(mm)
			if dbc.Error != nil {
				tx.Rollback()
				glog.Fatalf("Unable to insert metadata due to err: %s", dbc.Error.Error())
			}
			tx.Commit()
			return
		}
	}
	glog.Infof("Updating metadata for segment located at: %s", mdb.RootPath)
	dbc = tx.Model(mm).Where("key = ?", metadataKeyName).Updates(mm)
	if dbc.Error != nil {
		tx.Rollback()
		glog.Fatalf("Unable to update metadata due to err: %s", dbc.Error.Error())
	}
	tx.Commit()
}

func (mdb *segmentMetadataDB) GetMetadata() *SegmentMetadata {
	var mm metadataModel
	dbc := mdb.Db.Where("key = ?", metadataKeyName).First(&mm)
	if dbc.Error != nil {
		if !strings.Contains(dbc.Error.Error(), "record not found") {
			glog.Fatalf("Unable to get metadata due to err: %s", dbc.Error.Error())
		} else {
			return &SegmentMetadata{}
		}
	}
	sm := newSegmentMetadata(mm.Value)
	return sm
}
