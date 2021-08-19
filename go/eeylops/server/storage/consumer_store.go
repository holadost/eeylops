package storage

import (
	"bytes"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"os"
	"path"
	"strconv"
)

const kConsumerStoreDirectory = "consumer_store"
const kConsumerKeyDelimiter = "::::"

var kNilOffsetBytes = []byte("nil")

type ConsumerStore struct {
	kvStore    *kv_store.BadgerKVStore
	rootDir    string
	csDir      string
	lasRlogIdx int64
}

func NewConsumerStore(rootDir string) *ConsumerStore {
	cs := new(ConsumerStore)
	cs.rootDir = rootDir
	cs.csDir = path.Join(rootDir, kConsumerStoreDirectory)
	if err := os.MkdirAll(cs.csDir, 0774); err != nil {
		glog.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	cs.initialize()
	return cs
}

func (cs *ConsumerStore) initialize() {
	glog.Infof("Initializing consumer store located at: %s", cs.csDir)
	opts := badger.DefaultOptions(cs.csDir)
	opts.SyncWrites = true
	opts.NumMemtables = 2
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2
	opts.IndexCacheSize = 0
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	cs.kvStore = kv_store.NewBadgerKVStore(cs.csDir, opts)
	val, err := cs.kvStore.Get(sbase.KLastRLogIdxKeyBytes)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			cs.lasRlogIdx = -1
		} else {
			glog.Fatalf("Unable to initialize last log index due to err: %s", err.Error())
		}
	} else {
		cs.lasRlogIdx = int64(util.BytesToUint(val))
	}
}

func (cs *ConsumerStore) Close() error {
	return cs.kvStore.Close()
}

func (cs *ConsumerStore) RegisterConsumer(consumerID string, topicID base.TopicIDType, partitionID uint,
	rLogIdx int64) error {
	glog.Infof("Registering consumer for topic: %d, partition: %d. Consumer ID: %s",
		topicID, partitionID, consumerID)
	if rLogIdx < cs.lasRlogIdx {
		glog.Warningf("Invalid replication log index: %d. Expected > %d", rLogIdx, cs.lasRlogIdx)
		return ErrInvalidRLogIdx
	}
	key := generateConsumerKey(consumerID, topicID, partitionID)
	_, err := cs.kvStore.Get(key)
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			glog.Errorf("Unable to register consumer due to err: %s", err.Error())
			return ErrConsumerStoreFetch
		}
		rkey, rval := sbase.PrepareRLogIDXKeyVal(rLogIdx)
		var keys [][]byte
		var values [][]byte
		keys = append(keys, key, rkey)
		values = append(values, kNilOffsetBytes, rval)
		err = cs.kvStore.BatchPut(keys, values)
		if err != nil {
			glog.Errorf("Unable to register consumer in KV store due to err: %s", err.Error())
			return ErrConsumerStoreCommit
		}
		cs.lasRlogIdx = rLogIdx
		return nil
	}
	// The consumer is already registered.
	glog.Infof("The consumer: %s for topic ID: %d and partition: %d was already registered "+
		"Avoiding re-registering", consumerID, topicID, partitionID)
	return nil
}

func (cs *ConsumerStore) Commit(consumerID string, topicID base.TopicIDType, partitionID uint,
	offsetNum base.Offset, rLogIdx int64) error {
	if rLogIdx < cs.lasRlogIdx {
		glog.Warningf("Invalid replication log index: %d. Expected > %d", rLogIdx, cs.lasRlogIdx)
		return ErrInvalidRLogIdx
	}
	key := generateConsumerKey(consumerID, topicID, partitionID)
	_, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Attempting to commit an offset even though consumer: %s is not registered for "+
			"topic ID: %d, partition: %d", consumerID, topicID, partitionID)
		return ErrConsumerNotRegistered
	}
	rkey, rval := sbase.PrepareRLogIDXKeyVal(rLogIdx)
	var keys [][]byte
	var values [][]byte
	val := util.UintToBytes(uint64(offsetNum))
	keys = append(keys, key, rkey)
	values = append(values, val, rval)
	err = cs.kvStore.BatchPut(keys, values)
	if err != nil {
		glog.Errorf("Unable to commit an offset due to err: %s", err.Error())
		return ErrConsumerStoreCommit
	}
	cs.lasRlogIdx = rLogIdx
	return nil
}

func (cs *ConsumerStore) GetLastCommitted(consumerID string, topicID base.TopicIDType,
	partitionID uint) (base.Offset, error) {
	key := generateConsumerKey(consumerID, topicID, partitionID)
	val, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Did not find any offset committed by consumer: %s for topic ID: %d and partition: %d",
			consumerID, topicID, partitionID)
		return -1, ErrConsumerNotRegistered
	}
	if bytes.Compare(val, kNilOffsetBytes) == 0 {
		return -1, nil
	}
	lastCommitted := base.Offset(util.BytesToUint(val))
	return lastCommitted, nil
}

func (cs *ConsumerStore) Snapshot() error {
	return nil
}

func (cs *ConsumerStore) Restore() error {
	return nil
}

func generateConsumerKey(consumerID string, topicID base.TopicIDType, partitionID uint) []byte {
	return []byte(consumerID + kConsumerKeyDelimiter + fmt.Sprintf("%d", topicID) + kConsumerKeyDelimiter +
		strconv.Itoa(int(partitionID)))
}
