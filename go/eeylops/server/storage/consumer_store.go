package storage

import (
	"encoding/binary"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/golang/glog"
	"os"
	"path"
	"strconv"
)

const consumerStoreDirectory = "consumer_store"
const keyDelimiter = "::::"

type ConsumerStore struct {
	kvStore *BadgerKVStore
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
	opts.NumCompactors = 2
	opts.NumMemtables = 2
	opts.BlockCacheSize = 0
	opts.Compression = options.None
	opts.MaxLevels = 2
	opts.VerifyValueChecksum = true
	cs.kvStore = NewBadgerKVStore(cs.csDir, opts)
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
		// Mostly the key does not exist and so we got an error. We should check this error. but for
		// now, just move on.
		err = cs.kvStore.Put(key, []byte("nil"))
		if err != nil {
			return fmt.Errorf("unable to register consumer due to err: %w", err)
		}
	}
	// The consumer is already registered.
	glog.V(0).Infof("The consumer: %s for topic: %s and partition: %d was already registered "+
		"Avoiding re-registering", consumerID, topicName, partitionID)
	return nil
}

func (cs *ConsumerStore) Commit(consumerID string, topicName string, partitionID uint, offsetNum uint64) error {
	key := generateConsumerKey(consumerID, topicName, partitionID)
	_, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Attempting to commit an offset even though consumer: %s is not registered for "+
			"topic: %s, partition: %d", consumerID, topicName, partitionID)
		return fmt.Errorf("unable to commit due to err: %w", err)
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offsetNum)
	err = cs.kvStore.Put(key, val)
	if err != nil {
		glog.Errorf("Unable to commit an offset due to err: %s", err.Error())
		return fmt.Errorf("unable to commit due to err: %w", err)
	}
	return nil
}

func (cs *ConsumerStore) GetLastCommitted(consumerID string, topicName string, partitionID uint) (uint64, error) {
	key := generateConsumerKey(consumerID, topicName, partitionID)
	val, err := cs.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Did not find any offset committed by consumer: %s for topic: %s and partition: %d",
			consumerID, topicName, partitionID)
		return 0, fmt.Errorf("unable to get last committed offset due to err: %w", err)
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
