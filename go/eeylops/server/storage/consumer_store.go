package storage

import (
	"eeylops/server/storage/kv_store"
	"encoding/binary"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"os"
	"path"
	"strconv"
)

const consumerStoreDirectory = "consumer_store"
const keyDelimiter = "::::"

type ConsumerStore struct {
	kvStore *kv_store.BadgerKVStore
	rootDir string
	csDir   string
}

func NewConsumerStore(rootDir string) *ConsumerStore {
	cs := new(ConsumerStore)
	cs.rootDir = rootDir
	cs.csDir = path.Join(rootDir, consumerStoreDirectory)
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
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 3  // Use 3 compactors.
	opts.IndexCacheSize = 0
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	cs.kvStore = kv_store.NewBadgerKVStore(cs.csDir, opts)
}

func (cs *ConsumerStore) Close() error {
	return cs.kvStore.Close()
}

func (cs *ConsumerStore) RegisterConsumer(consumerID string, topicName string, partitionID uint) error {
	glog.Infof("Registering new consumer for topic: %s, partition: %d. Consumer ID: %s",
		topicName, partitionID, consumerID)
	key := generateConsumerKey(consumerID, topicName, partitionID)
	_, err := cs.kvStore.Get(key)
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			glog.Errorf("Unable to register consumer due to err: %s", err.Error())
			return ErrConsumerStoreCommit
		}
		err = cs.kvStore.Put(key, []byte("nil"))
		if err != nil {
			glog.Errorf("Unable to register consumer in KV store due to err: %s", err.Error())
			return ErrConsumerStoreCommit
		}
	}
	// The consumer is already registered.
	glog.Infof("The consumer: %s for topic: %s and partition: %d was already registered "+
		"Avoiding re-registering", consumerID, topicName, partitionID)
	return nil
}

func (cs *ConsumerStore) Commit(consumerID string, topicName string, partitionID uint, offsetNum uint64) error {
	key := generateConsumerKey(consumerID, topicName, partitionID)
	_, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Attempting to commit an offset even though consumer: %s is not registered for "+
			"topic: %s, partition: %d", consumerID, topicName, partitionID)
		return ErrConsumerStoreCommit
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offsetNum)
	err = cs.kvStore.Put(key, val)
	if err != nil {
		glog.Errorf("Unable to commit an offset due to err: %s", err.Error())
		return ErrConsumerStoreCommit
	}
	return nil
}

func (cs *ConsumerStore) GetLastCommitted(consumerID string, topicName string, partitionID uint) (uint64, error) {
	key := generateConsumerKey(consumerID, topicName, partitionID)
	val, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Did not find any offset committed by consumer: %s for topic: %s and partition: %d",
			consumerID, topicName, partitionID)
		return 0, ErrConsumerStoreFetch
	}
	lastCommitted := binary.BigEndian.Uint64(val)
	return lastCommitted, nil
}

func (cs *ConsumerStore) Snapshot() error {
	return nil
}

func (cs *ConsumerStore) Restore() error {
	return nil
}

func generateConsumerKey(consumerID string, topicName string, partitionID uint) []byte {
	return []byte(consumerID + keyDelimiter + topicName + keyDelimiter + strconv.Itoa(int(partitionID)))
}
