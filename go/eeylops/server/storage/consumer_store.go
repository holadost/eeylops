package storage

import (
	"encoding/binary"
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
	db      *badger.DB
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
	var err error
	cs.db, err = badger.Open(opts)
	if err != nil {
		glog.Fatalf("Unable to open consumer store due to err: %s", err.Error())
	}
}

func (cs *ConsumerStore) RegisterConsumer(consumerID string, topicName string, partitionID uint) error {
	err := cs.db.Update(func(txn *badger.Txn) error {
		key := generateKey(consumerID, topicName, partitionID)
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			glog.Infof("Registering new consumer: %s for topic: %s, partition: %d", consumerID, topicName,
				partitionID)
			err = txn.Set(key, []byte(""))
			return err
		}
		if err != nil {
			return err
		}
		// The consumer is already registered.
		glog.V(0).Infof("The consumer: %s for topic: %s and partition: %d was already registered. "+
			"Avoiding re-registering", consumerID, topicName, partitionID)
		return nil
	})
	if err != nil {
		glog.Errorf("Unable to register consumer due to error: %s", err.Error())
	}
	return err
}

func (cs *ConsumerStore) Commit(consumerID string, topicName string, partitionID uint, offsetNum uint64) error {
	err := cs.db.Update(func(txn *badger.Txn) error {
		key := generateKey(consumerID, topicName, partitionID)
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return err
		}
		if err != nil {
			return err
		}
		// Commit the given offset
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, offsetNum)
		err = txn.Set(key, val)
		return err
	})
	if err != nil {
		glog.Errorf("Unable to commit for consumer: %s, topic: %s and partition: %d due to err: %s",
			consumerID, topicName, partitionID, err.Error())
	}
	return err
}

func (cs *ConsumerStore) GetLastCommitted(consumerID string, topicName string, partitionID uint) (uint64, error) {
	var lastCommitted uint64
	err := cs.db.View(func(txn *badger.Txn) error {
		key := generateKey(consumerID, topicName, partitionID)
		item, err := txn.Get(key)
		if err != nil {
			glog.Errorf("Unable to get last committed offset due to err: %s", err.Error())
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			glog.Errorf("Unable to get last committed offset due to err: %s", err.Error())
			return err
		}
		lastCommitted = binary.BigEndian.Uint64(val)
		return nil
	})
	return lastCommitted, err
}

func (cs *ConsumerStore) Snapshot() error {
	return nil
}

func (cs *ConsumerStore) Restore() error {
	return nil
}

func generateKey(consumerID string, topicName string, partitionID uint) []byte {
	return []byte(consumerID + keyDelimiter + topicName + keyDelimiter + strconv.Itoa(int(partitionID)))
}
