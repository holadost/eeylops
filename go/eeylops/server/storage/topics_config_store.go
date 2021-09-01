package storage

import (
	"eeylops/server/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"encoding/json"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
)

const kTopicsConfigStoreDirectory = "topics_config_store"
const kLastTopicIDKey = "last::::topic::::id"

var kLastTopicIDBytes = []byte(kLastTopicIDKey)

// TopicsConfigStore holds all the topics for eeylops.
type TopicsConfigStore struct {
	kvStore      *kv_store.BadgerKVStore
	rootDir      string
	tsDir        string
	topicNameMap map[string]*base.TopicConfig
	topicIDMap   map[base.TopicIDType]*base.TopicConfig
	topicMapLock sync.RWMutex
	nextTopicID  base.TopicIDType
}

func NewTopicsConfigStore(rootDir string) *TopicsConfigStore {
	ts := new(TopicsConfigStore)
	ts.rootDir = rootDir
	ts.tsDir = path.Join(rootDir, kTopicsConfigStoreDirectory)
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		glog.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.initialize()
	return ts
}

func (tcs *TopicsConfigStore) initialize() {
	glog.Infof("Initializing topic store located at: %s", tcs.tsDir)
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
	allTopics := tcs.getTopicsFromKVStore()
	for _, topic := range allTopics {
		tcs.addTopicToMaps(topic)
	}
}

func (tcs *TopicsConfigStore) Close() error {
	return tcs.kvStore.Close()
}

func (tcs *TopicsConfigStore) AddTopic(topic base.TopicConfig) error {
	glog.Infof("Adding new topic: \n---------------%s\n---------------", topic.ToString())
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	_, exists := tcs.topicNameMap[topic.Name]
	if exists {
		glog.Warningf("Topic named: %s already exists. Not adding topic again", topic.Name)
		return ErrTopicExists
	}
	topic.ID = tcs.nextTopicID
	var keys [][]byte
	var values [][]byte
	keys = append(keys, []byte(topic.Name), kLastTopicIDBytes)
	values = append(values, tcs.marshalTopic(&topic), util.UintToBytes(uint64(topic.ID)))
	err := tcs.kvStore.BatchPut(keys, values)
	if err != nil {
		glog.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return ErrTopicStore
	}
	tcs.addTopicToMaps(&topic)
	tcs.nextTopicID += 1
	return nil
}

func (tcs *TopicsConfigStore) RemoveTopic(topicName string) error {
	glog.Infof("Removing topic: %s from topic store", topicName)
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	tpc, exists := tcs.topicNameMap[topicName]
	if !exists {
		return ErrTopicNotFound
	}
	_, exists = tcs.topicIDMap[tpc.ID]
	if !exists {
		glog.Fatalf("Found topic named: %s in name map but not in ID map. Topic ID: %d", tpc.Name, tpc.ID)
	}
	key := []byte(topicName)
	err := tcs.kvStore.Delete(key)
	if err != nil {
		glog.Errorf("Unable to delete topic: %s due to err: %s", topicName, err.Error())
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
	_, values, _, err := tcs.kvStore.Scan(nil, -1, -1, false)
	if err != nil {
		glog.Fatalf("Unable to get all topics in topic store due to err: %s", err.Error())
	}
	var topics []*base.TopicConfig
	for ii := 0; ii < len(values); ii++ {
		topics = append(topics, tcs.unmarshalTopic(values[ii]))
	}
	return topics
}

func (tcs *TopicsConfigStore) marshalTopic(topic *base.TopicConfig) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		glog.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

func (tcs *TopicsConfigStore) unmarshalTopic(data []byte) *base.TopicConfig {
	var topic base.TopicConfig
	err := json.Unmarshal(data, &topic)
	if err != nil {
		glog.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return &topic
}

// addTopicToMaps adds the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (tcs *TopicsConfigStore) addTopicToMaps(topic *base.TopicConfig) {
	tcs.topicNameMap[topic.Name] = topic
	tcs.topicIDMap[topic.ID] = topic
}

// removeTopicFromMaps removes the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (tcs *TopicsConfigStore) removeTopicFromMaps(topic *base.TopicConfig) {
	_, exists := tcs.topicNameMap[topic.Name]
	if !exists {
		glog.Fatalf("Did not find topic: %s in name map", topic.Name)
	}
	_, exists = tcs.topicIDMap[topic.ID]
	if !exists {
		glog.Fatalf("Did not find topic: %s[%d] in name map", topic.Name, topic.ID)
	}
	delete(tcs.topicNameMap, topic.Name)
	delete(tcs.topicIDMap, topic.ID)
}
