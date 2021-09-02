package storage

import (
	"bytes"
	"eeylops/server/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"encoding/json"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path"
	"sync"
)

const kTopicsConfigStoreDirectory = "topics_config_store"
const kLastTopicIDKey = "last::::topic::::id"
const kLastRLogIdx = "last::::rlog::::idx"

var kLastTopicIDBytes = []byte(kLastTopicIDKey)
var kLastRLogIdxBytes = []byte(kLastRLogIdx)

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
}

func NewTopicsConfigStore(rootDir string) *TopicsConfigStore {
	ts := new(TopicsConfigStore)
	ts.rootDir = rootDir
	ts.tsDir = path.Join(rootDir, kTopicsConfigStoreDirectory)
	ts.logger = logging.NewPrefixLogger("topics_config_store")
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		ts.logger.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.initialize()
	return ts
}

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
}

func (tcs *TopicsConfigStore) Close() error {
	return tcs.kvStore.Close()
}

func (tcs *TopicsConfigStore) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	_, exists := tcs.topicNameMap[topic.Name]
	if exists {
		tcs.logger.Warningf("Topic named: %s already exists. Not adding topic again", topic.Name)
		return ErrTopicExists
	}
	if tcs.topicIDGenerationEnabled {
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
	keys = append(keys, []byte(topic.Name), kLastTopicIDBytes, kLastRLogIdxBytes)
	values = append(values, tcs.marshalTopic(&topic), util.UintToBytes(uint64(topic.ID)),
		util.UintToBytes(uint64(rLogIdx)))
	err := tcs.kvStore.BatchPut(keys, values)
	if err != nil {
		tcs.logger.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return ErrTopicStore
	}
	tcs.addTopicToMaps(&topic)
	if tcs.topicIDGenerationEnabled {
		tcs.nextTopicID += 1
	}
	return nil
}

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
	key := []byte(topicConfig.Name)
	err := tcs.kvStore.Delete(key)
	if err != nil {
		tcs.logger.Errorf("Unable to delete topic: %s due to err: %s", topicConfig.Name, err.Error())
		return ErrTopicStore
	}
	tcs.removeTopicFromMaps(tpc)
	return nil
}

func (tcs *TopicsConfigStore) GetTopicByName(topicName string) (base.TopicConfig, error) {
	tcs.topicMapLock.RLock()
	defer tcs.topicMapLock.RUnlock()
	tpc, exists := tcs.topicNameMap[topicName]
	if !exists {
		return base.TopicConfig{}, ErrTopicNotFound
	}
	return *tpc, nil
}

func (tcs *TopicsConfigStore) GetTopicByID(topicID base.TopicIDType) (base.TopicConfig, error) {
	tcs.topicMapLock.RLock()
	defer tcs.topicMapLock.RUnlock()
	tpc, exists := tcs.topicIDMap[topicID]
	if !exists {
		return base.TopicConfig{}, ErrTopicNotFound
	}
	return *tpc, nil
}

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

func (tcs *TopicsConfigStore) GetLastTopicIDCreated() base.TopicIDType {
	return tcs.nextTopicID - 1
}

func (tcs *TopicsConfigStore) Snapshot() error {
	return nil
}

func (tcs *TopicsConfigStore) Restore() error {
	return nil
}

func (tcs *TopicsConfigStore) getTopicsFromKVStore() []*base.TopicConfig {
	keys, values, _, err := tcs.kvStore.Scan(nil, -1, -1, false)
	if err != nil {
		tcs.logger.Fatalf("Unable to get all topics in topic store due to err: %s", err.Error())
	}
	var topics []*base.TopicConfig
	if len(values) == 0 {
		return topics
	}
	for ii := 0; ii < len(values); ii++ {
		if bytes.Compare(keys[ii], kLastTopicIDBytes) == 0 || bytes.Compare(keys[ii], kLastRLogIdxBytes) == 0 {
			continue
		}
		topics = append(topics, tcs.unmarshalTopic(values[ii]))
	}
	tcs.logger.Infof("Total number of topics from KV store: %d", len(topics))
	return topics
}

func (tcs *TopicsConfigStore) marshalTopic(topic *base.TopicConfig) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		tcs.logger.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

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

func (tcs *TopicsConfigStore) getNextTopicIDFromKVStore() base.TopicIDType {
	if !tcs.topicIDGenerationEnabled {
		return 0
	}
	val, err := tcs.kvStore.Get(kLastTopicIDBytes)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// No topics have been created yet.
			return 1
		} else {
			tcs.logger.Fatalf("Unable to initialize next topic ID due to err: %s", err.Error())
		}
	}
	return base.TopicIDType(util.BytesToUint(val)) + 1
}
