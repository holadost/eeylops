package storage

import (
	"bytes"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"encoding/json"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
)

const kTopicsConfigStoreDirectory = "topics_config_store"
const kLastTopicIDKey = "last::topic::id"

var kLastTopicIDBytes = []byte(kLastTopicIDKey)

// TopicsConfigStore holds all the topics for eeylops.
type TopicsConfigStore struct {
	kvStore                  *kv_store.BadgerKVStore
	logger                   *logging.PrefixLogger
	rootDir                  string
	tsDir                    string
	topicNameMap             map[string]*base.TopicConfig
	topicIDMap               map[base.TopicIDType]*base.TopicConfig
	topicMapLock             sync.RWMutex
	topicIDGenerationEnabled bool
	nextTopicID              base.TopicIDType
	lastRLogIdx              int64
}

// NewTopicsConfigStore builds a new TopicsConfigStore instance. ID generation is disabled.
func NewTopicsConfigStore(rootDir string) *TopicsConfigStore {
	ts := new(TopicsConfigStore)
	ts.rootDir = rootDir
	ts.tsDir = path.Join(rootDir, kTopicsConfigStoreDirectory)
	ts.logger = logging.NewPrefixLogger("topics_config_store")
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		ts.logger.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.topicIDGenerationEnabled = false
	ts.initialize()
	return ts
}

// NewTopicsConfigStoreWithTopicIDGenerationEnabled builds a new TopicsConfigStore instance with ID generation
// enabled. When ID generation is enabled, the topics config store will automatically generate IDs for the topic.
// It panics if new topics with IDs are added.
func NewTopicsConfigStoreWithTopicIDGenerationEnabled(rootDir string) *TopicsConfigStore {
	ts := new(TopicsConfigStore)
	ts.rootDir = rootDir
	ts.tsDir = path.Join(rootDir, kTopicsConfigStoreDirectory)
	ts.logger = logging.NewPrefixLogger("topics_config_store")
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		ts.logger.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.topicIDGenerationEnabled = true
	ts.initialize()
	return ts
}

// initialize the topics config store.
func (tcs *TopicsConfigStore) initialize() {
	tcs.logger.Infof("Initializing topic store located at: %s", tcs.tsDir)
	// Initialize KV store.
	opts := badger.DefaultOptions(tcs.tsDir)
	opts.SyncWrites = true
	opts.NumMemtables = 2
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2  // Use 3 compactors.
	opts.IndexCacheSize = 0
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	tcs.kvStore = kv_store.NewBadgerKVStore(tcs.tsDir, opts)

	// Initialize internal maps.
	tcs.topicNameMap = make(map[string]*base.TopicConfig)
	tcs.topicIDMap = make(map[base.TopicIDType]*base.TopicConfig)
	tcs.nextTopicID = tcs.getNextTopicIDFromKVStore()
	allTopics := tcs.getTopicsFromKVStore()
	for _, topic := range allTopics {
		tcs.addTopicToMaps(topic)
	}

	// Initialize last replicated log index key.
	rLogKey, _ := sbase.PrepareRLogIDXKeyVal(0)
	lastVal, err := tcs.kvStore.Get(rLogKey)
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			tcs.logger.Fatalf("Unable to initialize topics config store due to err: %s", err.Error())
		}
		tcs.lastRLogIdx = -1
	} else {
		tcs.lastRLogIdx = int64(util.BytesToUint(lastVal))
	}
	tcs.logger.Infof("Last replicated log index in topics config store: %d", tcs.lastRLogIdx)
}

// Close the topics store.
func (tcs *TopicsConfigStore) Close() error {
	return tcs.kvStore.Close()
}

// AddTopic adds the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (tcs *TopicsConfigStore) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	if rLogIdx <= tcs.lastRLogIdx {
		tcs.logger.Warningf("A higher replicated log index: %d exists in the topics store. "+
			"Ignoring this request with log index: %d to add topic: %s", tcs.lastRLogIdx, rLogIdx, topic.Name)
		return ErrInvalidRLogIdx
	}
	_, exists := tcs.topicNameMap[topic.Name]
	if exists {
		tcs.logger.Warningf("Topic named: %s already exists. Not adding topic again", topic.Name)
		return ErrTopicExists
	}
	if tcs.topicIDGenerationEnabled {
		if topic.ID > 0 {
			tcs.logger.Fatalf("Topic ID generation enabled but received topic with an ID. Topic: %s",
				topic.ToString())
		}
		topic.ID = tcs.nextTopicID
	} else {
		// A topic ID must have been provided. Panic if not.
		if topic.ID <= 0 {
			tcs.logger.Fatalf("Topic ID generation is disabled an a topic id was not provided for topic: %s",
				topic.ToString())
		}
	}
	tcs.logger.Infof("Adding new topic: \n---------------%s\n---------------", topic.ToString())
	var keys [][]byte
	var values [][]byte
	rLogKey, rLogVal := sbase.PrepareRLogIDXKeyVal(rLogIdx)
	keys = append(keys, []byte(topic.Name), kLastTopicIDBytes, rLogKey)
	values = append(values, tcs.marshalTopic(&topic), util.UintToBytes(uint64(topic.ID)), rLogVal)
	err := tcs.kvStore.BatchPut(keys, values)
	if err != nil {
		tcs.logger.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return ErrTopicStore
	}
	tcs.addTopicToMaps(&topic)
	if tcs.topicIDGenerationEnabled {
		tcs.nextTopicID += 1
	}
	tcs.lastRLogIdx = rLogIdx
	return nil
}

// RemoveTopic removes the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (tcs *TopicsConfigStore) RemoveTopic(topicId base.TopicIDType, rLogIdx int64) error {
	tcs.logger.Infof("Removing topic: %d from topic store", topicId)
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	tpc, exists := tcs.topicIDMap[topicId]
	if !exists {
		return ErrTopicNotFound
	}
	topicConfig, exists := tcs.topicIDMap[tpc.ID]
	if !exists {
		tcs.logger.Fatalf("Found topic named: %s in name map but not in ID map. Topic ID: %d", tpc.Name, tpc.ID)
	}
	if rLogIdx <= tcs.lastRLogIdx {
		tcs.logger.Warningf("A higher log index already exists in the topics config store. " +
			"Ignoring this request")
		return ErrInvalidRLogIdx
	}
	key := []byte(topicConfig.Name)
	txn := tcs.kvStore.NewTransaction()
	defer txn.Discard()
	if err := txn.Delete(key); err != nil {
		tcs.logger.Errorf("Unable to delete topic: %s due to err: %s", topicConfig.Name, err.Error())
		return ErrTopicStore
	}
	if err := txn.Put(sbase.PrepareRLogIDXKeyVal(rLogIdx)); err != nil {
		tcs.logger.Errorf("Unable to add replicated log index: %d due to err: %s", rLogIdx, err.Error())
		return ErrTopicStore
	}
	if err := txn.Commit(); err != nil {
		tcs.logger.Errorf("Unable to commit transaction to remove topic: %s due to err: %s", topicConfig.Name,
			err.Error())
		return ErrTopicStore
	}
	tcs.removeTopicFromMaps(tpc)
	tcs.lastRLogIdx = rLogIdx
	return nil
}

// GetTopicByName fetches the topic by its name.
func (tcs *TopicsConfigStore) GetTopicByName(topicName string) (base.TopicConfig, error) {
	tcs.topicMapLock.RLock()
	defer tcs.topicMapLock.RUnlock()
	tpc, exists := tcs.topicNameMap[topicName]
	if !exists {
		return base.TopicConfig{}, ErrTopicNotFound
	}
	return *tpc, nil
}

// GetTopicByID fetches the topic by its ID.
func (tcs *TopicsConfigStore) GetTopicByID(topicID base.TopicIDType) (base.TopicConfig, error) {
	tcs.topicMapLock.RLock()
	defer tcs.topicMapLock.RUnlock()
	tpc, exists := tcs.topicIDMap[topicID]
	if !exists {
		return base.TopicConfig{}, ErrTopicNotFound
	}
	return *tpc, nil
}

// GetAllTopics fetches all topics in the store.
func (tcs *TopicsConfigStore) GetAllTopics() []base.TopicConfig {
	tcs.topicMapLock.RLock()
	defer tcs.topicMapLock.RUnlock()
	var topics []base.TopicConfig
	tcs.logger.Infof("Total number of topics in maps: %d", len(tcs.topicIDMap))
	for _, topic := range tcs.topicIDMap {
		topics = append(topics, *topic)
	}
	return topics
}

// GetLastTopicIDCreated returns the last topic ID that was created.
func (tcs *TopicsConfigStore) GetLastTopicIDCreated() base.TopicIDType {
	return tcs.nextTopicID - 1
}

// Snapshot creates a snapshot of the store.
func (tcs *TopicsConfigStore) Snapshot() error {
	return nil
}

// Restore restores the store from a given snapshot.
func (tcs *TopicsConfigStore) Restore() error {
	return nil
}

// getTopicsFromKVStore fetches all the topics from the underlying KV store.
func (tcs *TopicsConfigStore) getTopicsFromKVStore() []*base.TopicConfig {
	keys, values, _, err := tcs.kvStore.Scan(nil, -1, -1, false)
	if err != nil {
		tcs.logger.Fatalf("Unable to get all topics in topic store due to err: %s", err.Error())
	}
	var topics []*base.TopicConfig
	if len(values) == 0 {
		return topics
	}
	rLogKey, _ := sbase.PrepareRLogIDXKeyVal(0)
	for ii := 0; ii < len(values); ii++ {
		if bytes.Compare(keys[ii], kLastTopicIDBytes) == 0 || bytes.Compare(keys[ii], rLogKey) == 0 {
			continue
		}
		topics = append(topics, tcs.unmarshalTopic(values[ii]))
	}
	tcs.logger.Infof("Total number of topics from KV store: %d", len(topics))
	return topics
}

// marshalTopic serializes the given topic config.
func (tcs *TopicsConfigStore) marshalTopic(topic *base.TopicConfig) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		tcs.logger.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

// unmarshalTopic deserializes the given data to TopicConfig.
func (tcs *TopicsConfigStore) unmarshalTopic(data []byte) *base.TopicConfig {
	var topic base.TopicConfig
	err := json.Unmarshal(data, &topic)
	if err != nil {
		tcs.logger.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return &topic
}

// addTopicToMaps adds the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (tcs *TopicsConfigStore) addTopicToMaps(topic *base.TopicConfig) {
	if len(tcs.topicIDMap) != len(tcs.topicNameMap) {
		tcs.logger.Fatalf("Topics mismatch in topicNameMap and topicIDMap. Got: %d, %d", len(tcs.topicNameMap),
			len(tcs.topicIDMap))
	}
	tcs.topicNameMap[topic.Name] = topic
	tcs.topicIDMap[topic.ID] = topic
}

// removeTopicFromMaps removes the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (tcs *TopicsConfigStore) removeTopicFromMaps(topic *base.TopicConfig) {
	if len(tcs.topicIDMap) != len(tcs.topicNameMap) {
		tcs.logger.Fatalf("Topics mismatch in topicNameMap and topicIDMap. Got: %d, %d", len(tcs.topicNameMap),
			len(tcs.topicIDMap))
	}
	_, exists := tcs.topicNameMap[topic.Name]
	if !exists {
		tcs.logger.Fatalf("Did not find topic: %s in name map", topic.Name)
	}
	_, exists = tcs.topicIDMap[topic.ID]
	if !exists {
		tcs.logger.Fatalf("Did not find topic: %s[%d] in name map", topic.Name, topic.ID)
	}
	delete(tcs.topicNameMap, topic.Name)
	delete(tcs.topicIDMap, topic.ID)
}

// getNextTopicIDFromKVStore fetches the next topic ID from the underlying KV store that will be used for new topics.
func (tcs *TopicsConfigStore) getNextTopicIDFromKVStore() base.TopicIDType {
	if !tcs.topicIDGenerationEnabled {
		return 0
	}
	val, err := tcs.kvStore.Get(kLastTopicIDBytes)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// No topics have been created yet.
			glog.Infof("DID NOT find any topics in the topics store. Starting from topic ID: 1")
			return 1
		} else {
			tcs.logger.Fatalf("Unable to initialize next topic ID due to err: %s", err.Error())
		}
	}
	return base.TopicIDType(util.BytesToUint(val)) + 1
}
