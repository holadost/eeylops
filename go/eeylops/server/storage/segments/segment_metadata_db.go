package segments

import (
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	"os"
	"path"
	"strings"
)

// SegmentMetadataDB persists the segment metadata.
type SegmentMetadataDB struct {
	Db       *gorm.DB
	Path     string
	RootPath string
	closed   bool
}

type metadataModel struct {
	Key   string `gorm:"type:varchar(100);PRIMARY_KEY" json:"key"`
	Value []byte `gorm:"type:BLOB;NOT NULL" json:"value"`
}

func NewSegmentMetadataDB(dbRootPath string) *SegmentMetadataDB {
	mDirPath := path.Join(dbRootPath, metadataDirName)
	dbPath := path.Join(mDirPath, metadataDbName)
	err := os.MkdirAll(mDirPath, 0774)
	if err != nil {
		glog.Fatalf("Unable to create directory for segment metadata located at: %s", dbRootPath)
	}
	db, err := gorm.Open("sqlite3", dbPath)
	if err != nil {
		glog.Fatalf("Unable to open metadata ddb located at: %s", dbPath)
		return nil
	}
	mdb := new(SegmentMetadataDB)
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

func (mdb *SegmentMetadataDB) Close() {
	if mdb.closed {
		return
	}
	mdb.closed = true
	mdb.Db.Close()

}

func (mdb *SegmentMetadataDB) PutMetadata(metadata *SegmentMetadata) {
	var val metadataModel
	mm := &metadataModel{
		Key:   metadataKeyName,
		Value: metadata.Serialize(),
	}
	glog.V(1).Infof("Putting metadata in segment metadata DB: %s", metadata.ToString())
	tx := mdb.Db.Begin()
	// defer tx.Close()
	dbc := tx.Where("key = ?", metadataKeyName).First(&val)
	if dbc.Error != nil {
		if !strings.Contains(dbc.Error.Error(), "record not found") {
			tx.Rollback()
			glog.Fatalf("Unable to get metadata due to err: %s", dbc.Error.Error())
		} else {
			dbc = tx.Create(mm)
			if dbc.Error != nil {
				tx.Rollback()
				glog.Fatalf("Unable to insert metadata due to err: %s", dbc.Error.Error())
			}
			tx.Commit()
			return
		}
	}
	dbc = tx.Model(mm).Where("key = ?", metadataKeyName).Updates(mm)
	if dbc.Error != nil {
		tx.Rollback()
		glog.Fatalf("Unable to update metadata due to err: %s", dbc.Error.Error())
	}
	tx.Commit()
}

func (mdb *SegmentMetadataDB) GetMetadata() *SegmentMetadata {
	var mm metadataModel
	dbc := mdb.Db.Where("key = ?", metadataKeyName).First(&mm)
	if dbc.Error != nil {
		if !strings.Contains(dbc.Error.Error(), "record not found") {
			glog.Fatalf("Unable to get metadata due to err: %s", dbc.Error.Error())
		} else {
			return &SegmentMetadata{}
		}
	}
	sm := NewSegmentMetadata(mm.Value)
	return sm
}
