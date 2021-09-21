package storage

import (
	"bytes"
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	bkv "eeylops/server/storage/kv_store/badger_kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path"
	"strconv"
	"strings"
)

const kConsumerStoreDirectory = "consumer_store"
const kConsumerKeyDelimiter = "::"
const kConsumerStoreMainColumnFamily = "main_cf"
const kConsumerStoreMiscColumnFamily = "misc_cf"

var kNilOffsetBytes = []byte("nil")

// ConsumerStore is used by the broker to manage the last committed offsets by every registered consumer.
type ConsumerStore struct {
	kvStore     kv_store.KVStore
	storeID     string
	rootDir     string
	csDir       string
	lastRLogIdx int64
	logger      *logging.PrefixLogger
}

// NewConsumerStore instantiates a new instance of ConsumerStore. A consumer store instance is tied to a single broker
// instance generally.
func NewConsumerStore(rootDir string, id string) *ConsumerStore {
	cs := new(ConsumerStore)
	cs.storeID = id
	cs.logger = logging.NewPrefixLogger(fmt.Sprintf("ConsumerStore: %s", cs.storeID))
	cs.rootDir = rootDir
	cs.csDir = path.Join(rootDir, kConsumerStoreDirectory)
	if err := os.MkdirAll(cs.csDir, 0774); err != nil {
		cs.logger.Fatalf("Unable to create directory for consumer store due to err: %v", err)
		return nil
	}
	cs.initialize()
	return cs
}

// initialize the consumer store.
func (cs *ConsumerStore) initialize() {
	cs.logger.Infof("Initializing consumer store located at: %s", cs.csDir)
	opts := badger.DefaultOptions(cs.csDir)
	opts.SyncWrites = true
	opts.NumMemtables = 2
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2
	opts.IndexCacheSize = 16 * 1024 * 1024
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	opts.LoadBloomsOnOpen = false
	cs.kvStore = bkv.NewBadgerKVStore(cs.csDir, opts, cs.logger)
	cs.addCfsIfNotExists()

	// Find the last replicated log index.
	val, err := cs.kvStore.Get(&kv_store.KVStoreKey{
		Key:          storagebase.GetLastRLogKeyBytes(),
		ColumnFamily: kConsumerStoreMiscColumnFamily,
	})
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			cs.lastRLogIdx = -1
		} else {
			cs.logger.Fatalf("Unable to initialize last log index due to err: %s", err.Error())
		}
	} else {
		cs.lastRLogIdx = storagebase.GetRLogValFromBytes(val.Value)
	}
}

// Close the consumer store.
func (cs *ConsumerStore) Close() error {
	return cs.kvStore.Close()
}

// RegisterConsumer registers a consumer with the broker.
func (cs *ConsumerStore) RegisterConsumer(consumerID string, topicID base.TopicIDType, partitionID uint,
	rLogIdx int64) error {
	cs.logger.Infof("Registering consumer for topic: %d, partition: %d. Consumer ID: %s",
		topicID, partitionID, consumerID)
	if rLogIdx < cs.lastRLogIdx {
		cs.logger.Warningf("Invalid replication log index: %d. Expected > %d", rLogIdx, cs.lastRLogIdx)
		return ErrInvalidRLogIdx
	}
	if strings.Contains(consumerID, kConsumerKeyDelimiter) {
		cs.logger.Fatalf("Consumer ID cannot contain %s. Consumer ID: %s", kConsumerKeyDelimiter, consumerID)
	}
	key := cs.generateConsumerKey(consumerID, topicID, partitionID)
	_, err := cs.kvStore.Get(&kv_store.KVStoreKey{
		Key:          key,
		ColumnFamily: kConsumerStoreMainColumnFamily,
	})
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			cs.logger.Errorf("Unable to register consumer due to err: %s", err.Error())
			return ErrConsumerStoreFetch
		}
		rKey, rVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
		var entries []*kv_store.KVStoreEntry
		entries = append(entries, &kv_store.KVStoreEntry{
			Key:          key,
			Value:        kNilOffsetBytes,
			ColumnFamily: kConsumerStoreMainColumnFamily,
		}, &kv_store.KVStoreEntry{
			Key:          rKey,
			Value:        rVal,
			ColumnFamily: kConsumerStoreMiscColumnFamily,
		})
		err = cs.kvStore.BatchPut(entries)
		if err != nil {
			cs.logger.Errorf("Unable to register consumer in KV store due to err: %s", err.Error())
			return ErrConsumerStoreCommit
		}
		cs.lastRLogIdx = rLogIdx
		return nil
	}
	// The consumer is already registered.
	cs.logger.VInfof(1, "The consumer: %s for topic ID: %d and partition: %d was already registered "+
		"Avoiding re-registering", consumerID, topicID, partitionID)
	return nil
}

// Commit commits the given offsetNum as the last consumed offset by that consumer.
func (cs *ConsumerStore) Commit(consumerID string, topicID base.TopicIDType, partitionID uint,
	offsetNum base.Offset, rLogIdx int64) error {
	if rLogIdx < cs.lastRLogIdx {
		cs.logger.Warningf("Invalid replication log index: %d. Expected > %d", rLogIdx, cs.lastRLogIdx)
		return ErrInvalidRLogIdx
	}
	if strings.Contains(consumerID, kConsumerKeyDelimiter) {
		cs.logger.Fatalf("Consumer ID cannot contain %s. Consumer ID: %s", kConsumerKeyDelimiter, consumerID)
	}
	key := cs.generateConsumerKey(consumerID, topicID, partitionID)
	_, err := cs.kvStore.Get(&kv_store.KVStoreKey{
		Key:          key,
		ColumnFamily: kConsumerStoreMainColumnFamily,
	})
	if err != nil {
		cs.logger.Errorf("Attempting to commit an offset even though consumer: %s is not registered for "+
			"topic ID: %d, partition: %d", consumerID, topicID, partitionID)
		return ErrConsumerNotRegistered
	}
	rKey, rVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
	var entries []*kv_store.KVStoreEntry
	entries = append(entries, &kv_store.KVStoreEntry{
		Key:          key,
		Value:        cs.offsetToBytes(offsetNum),
		ColumnFamily: kConsumerStoreMainColumnFamily,
	}, &kv_store.KVStoreEntry{
		Key:          rKey,
		Value:        rVal,
		ColumnFamily: kConsumerStoreMiscColumnFamily,
	})
	err = cs.kvStore.BatchPut(entries)
	if err != nil {
		cs.logger.Errorf("Unable to commit an offset due to err: %s", err.Error())
		return ErrConsumerStoreCommit
	}
	cs.lastRLogIdx = rLogIdx
	return nil
}

// GetLastCommitted returns the last consumed offset by a consumer.
func (cs *ConsumerStore) GetLastCommitted(consumerID string, topicID base.TopicIDType,
	partitionID uint) (base.Offset, error) {
	key := cs.generateConsumerKey(consumerID, topicID, partitionID)
	val, err := cs.kvStore.Get(&kv_store.KVStoreKey{
		Key:          key,
		ColumnFamily: kConsumerStoreMainColumnFamily,
	})
	if err != nil {
		cs.logger.Errorf("Did not find any offset committed by consumer: %s for topic ID: %d and partition: %d",
			consumerID, topicID, partitionID)
		return -1, ErrConsumerNotRegistered
	}
	if bytes.Compare(val.Value, kNilOffsetBytes) == 0 {
		return -1, nil
	}
	lastCommitted := cs.bytesToOffset(val.Value)
	return lastCommitted, nil
}

func (cs *ConsumerStore) RemoveNonExistentTopicConsumers(existentTopicIds []base.TopicIDType) {
	cs.logger.Infof("Removing all non existent topic consumers from store")
	deleteKeys := func(keys []*kv_store.KVStoreKey) {
		if err := cs.kvStore.BatchDelete(keys); err != nil {
			cs.logger.Fatalf("Unable to delete keys due to err: %v", err)
		}
	}
	scanner, err := cs.kvStore.NewScanner(kConsumerStoreMainColumnFamily, nil, false)
	if err != nil {
		cs.logger.Fatalf("Unable to initialize scanner due to err: %v", err)
	}
	var keysToRemove []*kv_store.KVStoreKey
	totalKeysRemoved := 0
	for ; scanner.Valid(); scanner.Next() {
		key, _, err := scanner.GetItem()
		if err != nil {
			cs.logger.Fatalf("Unable to scan from consumer store due to err: %v", err)
		}
		for _, tid := range existentTopicIds {
			if cs.doesKeyContainTopicID(key, tid) {
				keysToRemove = append(keysToRemove, &kv_store.KVStoreKey{
					Key:          key,
					ColumnFamily: kConsumerStoreMainColumnFamily,
				})
				totalKeysRemoved++
				if len(keysToRemove) == 64 {
					deleteKeys(keysToRemove)
					keysToRemove = nil
				}
			}
		}
	}
	if len(keysToRemove) > 0 {
		deleteKeys(keysToRemove)
	}
	if totalKeysRemoved > 0 {
		cs.logger.Infof("Removed %d keys from the consumer store", totalKeysRemoved)
	}
}

// Snapshot the consumer store.
func (cs *ConsumerStore) Snapshot() error {
	return nil
}

// Restore consumer store from an existing snapshot.
func (cs *ConsumerStore) Restore() error {
	return nil
}

// offsetToBytes converts the given offset to bytes.
func (cs *ConsumerStore) offsetToBytes(offset base.Offset) []byte {
	return util.UintToBytes(uint64(offset))
}

// bytesToOffset converts the given byte slice to an offset number.
func (cs *ConsumerStore) bytesToOffset(data []byte) base.Offset {
	return base.Offset(util.BytesToUint(data))
}

// generateConsumerKey is a helper method that generates a unique key based on the consumer id, topic id and partition
// for the backing KV store of the consumer store.
func (cs *ConsumerStore) generateConsumerKey(consumerID string, topicID base.TopicIDType, partitionID uint) []byte {
	return []byte(consumerID + kConsumerKeyDelimiter + fmt.Sprintf("%d", topicID) + kConsumerKeyDelimiter +
		strconv.Itoa(int(partitionID)))
}

func (cs *ConsumerStore) doesKeyContainTopicID(key []byte, topicID base.TopicIDType) bool {
	fields := strings.Split(string(key), kConsumerKeyDelimiter)
	if fields[1] == fmt.Sprintf("%d", topicID) {
		return true
	}
	return false
}

// addCfsIfNotExists adds the consumer store column families to the KV store.
func (cs *ConsumerStore) addCfsIfNotExists() {
	for _, cf := range []string{kConsumerStoreMainColumnFamily, kConsumerStoreMiscColumnFamily} {
		if err := cs.kvStore.AddColumnFamily(cf); err != nil {
			if err != kv_store.ErrKVStoreColumnFamilyExists {
				cs.logger.Fatalf("Unable to add column family: %s due to err: %v", cf, err)
			}
		}
	}
}
