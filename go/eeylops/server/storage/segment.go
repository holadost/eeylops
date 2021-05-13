package storage

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
	"os"
	"path"
	"strings"
	"time"
)

type Segment interface {
	// Initialize the segment.
	Initialize()
	// Close the segment.
	Close()
	// Append values to the segment.
	Append([][]byte) error
	// Scan numMessages values from the segment store from the given start offset.
	Scan(startOffset uint64, numMessages uint64) ([][]byte, []error)
	// Stats fetches the stats for this instance of segment.
	Stats()
	// GetMetadata fetches the metadata of the segment.
	GetMetadata() SegmentMetadata
	// SetMetadata sets the metadata. This is updated internally and by the partition when a segment is created.
	SetMetadata(SegmentMetadata) error
	// MarkImmutable marks the segment as immutable.
	MarkImmutable()
}

// SegmentMetadata holds the metadata of a segment.
type SegmentMetadata struct {
	ID                 uint64    `json:"id"`                  // Segment ID.
	Immutable          bool      `json:"immutable"`           // Flag to indicate whether segment is immutable.
	StartOffset        uint64    `json:"start_offset"`        // StartOffset of the segment.
	EndOffset          uint64    `json:"end_offset"`          // EndOffset of the segment. Not valid if segment is live.
	CreatedTimestamp   time.Time `json:"created_timestamp"`   // Segment created time.
	ImmutableTimestamp time.Time `json:"immutable_timestamp"` // Time when segment was marked as immutable.
	ImmutableReason    int       `json:"immutable_reason"`    // The reason why the segment was marked immutable.
}

func newSegmentMetadata(data []byte) *SegmentMetadata {
	var sm SegmentMetadata
	err := json.Unmarshal(data, &sm)
	if err != nil {
		glog.Fatalf("Failed to deserialize segment metadata due to err: %s", err.Error())
	}
	return &sm
}

func (sm *SegmentMetadata) Serialize() []byte {
	data, err := json.Marshal(sm)
	if err != nil {
		glog.Fatalf("Unable to serialize metadata for segment")
	}
	return data
}

// Constants.
const dataDirName = "data"
const metadataDirName = "metadata"
const metadataDbName = "metadata.db"
const metadataKeyName = "metadata"

// segmentMetadataDB persists the segment metadata.
type segmentMetadataDB struct {
	Db       *gorm.DB
	Path     string
	RootPath string
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
		glog.Fatalf("Unable to open metadata db located at: %s", dbPath)
		return nil
	}
	mdb := new(segmentMetadataDB)
	mdb.Db = db
	mdb.Path = dbPath
	mdb.RootPath = dbRootPath
	mdbModel := metadataModel{}
	dbc := mdb.Db.AutoMigrate(&mdbModel)
	if dbc != nil && dbc.Error != nil {
		glog.Fatalf("Unable to create and initialize metadata db located at: %s, due to err: %v",
			mdb.Path, dbc.Error)
	}
	return mdb
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
			glog.Infof("Inserting segment metadata for segment located at: %s", mdb.RootPath)
			dbc = tx.Create(mm)
			if dbc.Error != nil {
				tx.Rollback()
				glog.Fatalf("Unable to insert metadata due to err: %s", dbc.Error.Error())
			}
			tx.Commit()
			return
		}
	}
	glog.Infof("Updating segment metadata located at: %s", mdb.RootPath)
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
