package storage

import (
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	bkv "eeylops/server/storage/kv_store/badger_kv_store"
	"eeylops/util/logging"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path"
	"sync"
)

const kTopicsConfigStoreDirectory = "topics_config_store"
const kTopicsConfigStoreMainColumnFamily = "main_cf"
const kTopicsConfigStoreMiscColumnFamily = "misc_cf"

// TopicsConfigStore holds all the topics for eeylops.
type TopicsConfigStore struct {
	kvStore      kv_store.KVStore
	logger       *logging.PrefixLogger
	storeID      string
	rootDir      string
	tsDir        string
	topicNameMap map[string]*base.TopicConfig
	topicIDMap   map[base.TopicIDType]*base.TopicConfig
	topicMapLock sync.RWMutex
	lastRLogIdx  int64
}

// NewTopicsConfigStore builds a new TopicsConfigStore instance. ID generation is disabled.
func NewTopicsConfigStore(rootDir string, id string) *TopicsConfigStore {
	ts := new(TopicsConfigStore)
	ts.rootDir = rootDir
	ts.storeID = id
	ts.tsDir = path.Join(rootDir, kTopicsConfigStoreDirectory)
	ts.logger = logging.NewPrefixLogger(fmt.Sprintf("TopicsConfigStore: %s", ts.storeID))
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		ts.logger.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.initialize()
	return ts
}

// initialize the topics config store.
func (tcs *TopicsConfigStore) initialize() {
	tcs.logger.Infof("Initializing topics config store located at: %s", tcs.tsDir)
	// Initialize KV store.
	opts := badger.DefaultOptions(tcs.tsDir)
	opts.SyncWrites = true
	opts.NumMemtables = 2
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2  // Use 3 compactors.
	opts.IndexCacheSize = 16 * 1024 * 1024
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	opts.LoadBloomsOnOpen = false
	tcs.kvStore = bkv.NewBadgerKVStore(tcs.tsDir, opts, tcs.logger)
	tcs.addCfsIfNotExists()

	// Initialize internal maps.
	tcs.topicNameMap = make(map[string]*base.TopicConfig)
	tcs.topicIDMap = make(map[base.TopicIDType]*base.TopicConfig)
	allTopics := tcs.getTopicsFromKVStore()
	for _, topic := range allTopics {
		tcs.addTopicToMaps(topic)
	}

	// Initialize last replicated log index key.
	lastVal, err := tcs.kvStore.Get(&kv_store.KVStoreKey{
		Key:          storagebase.GetLastRLogKeyBytes(),
		ColumnFamily: kConsumerStoreMiscColumnFamily,
	})
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			tcs.logger.Fatalf("Unable to initialize topics config store due to err: %s", err.Error())
		}
		tcs.lastRLogIdx = -1
	} else {
		tcs.lastRLogIdx = storagebase.GetRLogValFromBytes(lastVal.Value)
	}
	tcs.logger.VInfof(1, "Last replicated log index in topics config store: %d", tcs.lastRLogIdx)
}

// Close the topics store.
func (tcs *TopicsConfigStore) Close() error {
	return tcs.kvStore.Close()
}

// AddTopic adds the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (tcs *TopicsConfigStore) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	sanitize := func() error {
		tcs.topicMapLock.RLock()
		defer tcs.topicMapLock.RUnlock()
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
		if topic.ID <= 0 {
			tcs.logger.Fatalf("Topic ID generation is disabled and a topic id was not provided for topic: %s",
				topic.ToString())
		}
		return nil
	}
	if err := sanitize(); err != nil {
		return err
	}
	var entries []*kv_store.KVStoreEntry
	rLogKey, rLogVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
	entries = append(entries, &kv_store.KVStoreEntry{
		Key:          rLogKey,
		Value:        rLogVal,
		ColumnFamily: kTopicsConfigStoreMiscColumnFamily,
	}, &kv_store.KVStoreEntry{
		Key:          []byte(topic.Name),
		Value:        tcs.marshalTopic(&topic),
		ColumnFamily: kTopicsConfigStoreMainColumnFamily,
	})
	tcs.logger.Infof("Adding new topic: \n---------------%s\n---------------", topic.ToString())
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	err := tcs.kvStore.BatchPut(entries)
	if err != nil {
		tcs.logger.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return ErrTopicStore
	}
	tcs.addTopicToMaps(&topic)
	tcs.lastRLogIdx = rLogIdx
	return nil
}

// RemoveTopic removes the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (tcs *TopicsConfigStore) RemoveTopic(topicId base.TopicIDType, rLogIdx int64) error {
	var tpc *base.TopicConfig
	var exists bool
	sanitize := func() error {
		tcs.topicMapLock.RLock()
		defer tcs.topicMapLock.RUnlock()
		tpc, exists = tcs.topicIDMap[topicId]
		if !exists {
			return ErrTopicNotFound
		}
		_, exists = tcs.topicNameMap[tpc.Name]
		if !exists {
			tcs.logger.Fatalf("Found topic named: %s in name map but not in ID map. Topic ID: %d",
				tpc.Name, tpc.ID)
		}
		if rLogIdx <= tcs.lastRLogIdx {
			tcs.logger.Warningf("A higher log index already exists in the topics config store. " +
				"Ignoring this request")
			return ErrInvalidRLogIdx
		}
		return nil
	}
	if err := sanitize(); err != nil {
		return err
	}
	key := tcs.topicNameToKeyBytes(tpc.Name)
	var entries []*kv_store.KVStoreEntry
	rKey, rVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
	entries = append(entries, &kv_store.KVStoreEntry{
		Key:          rKey,
		Value:        rVal,
		ColumnFamily: kTopicsConfigStoreMiscColumnFamily,
	})

	tcs.logger.Infof("Removing topic: %d from topic store", topicId)
	tcs.topicMapLock.Lock()
	defer tcs.topicMapLock.Unlock()
	txn := tcs.kvStore.NewTransaction()
	defer txn.Discard()
	if err := txn.Delete(&kv_store.KVStoreKey{Key: key, ColumnFamily: kTopicsConfigStoreMainColumnFamily}); err != nil {
		tcs.logger.Errorf("Unable to delete topic: %s due to err: %s", tpc.Name, err.Error())
		return ErrTopicStore
	}
	if err := txn.BatchPut(entries); err != nil {
		tcs.logger.Errorf("Unable to add replicated log index: %d due to err: %s", rLogIdx, err.Error())
		return ErrTopicStore
	}
	if err := txn.Commit(); err != nil {
		tcs.logger.Errorf("Unable to commit transaction to remove topic: %s due to err: %s", tpc.Name,
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
	for _, topic := range tcs.topicIDMap {
		topics = append(topics, *topic)
	}
	return topics
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
	scanner, err := tcs.kvStore.NewScanner(kTopicsConfigStoreMainColumnFamily, nil, false)
	if err != nil {
		tcs.logger.Fatalf("Unable to gather topics from store due to err: %v", err)
	}
	var topics []*base.TopicConfig
	var topicNames []string
	for ; scanner.Valid(); scanner.Next() {
		key, value, err := scanner.GetItem()
		if err != nil {
			tcs.logger.Fatalf("Unable to fetch item due to err: %v", err)
		}
		topics = append(topics, tcs.unmarshalTopic(value))
		topicNames = append(topicNames, tcs.keyBytesToTopicName(key))
	}
	tcs.logger.Infof("Total number of topics from KV store: %d. Topics: %v", len(topics), topicNames)
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

// addCfsIfNotExists adds the topics config store column families to the KV store.
func (tcs *TopicsConfigStore) addCfsIfNotExists() {
	for _, cf := range []string{kTopicsConfigStoreMainColumnFamily, kTopicsConfigStoreMiscColumnFamily} {
		if err := tcs.kvStore.AddColumnFamily(cf); err != nil {
			if err != kv_store.ErrKVStoreColumnFamilyExists {
				tcs.logger.Fatalf("Unable to add column family: %s due to err: %v", cf, err)
			}
		}
	}
}

// topicNameToKeyBytes is a helper method that converts the given topic name to bytes.
func (tcs *TopicsConfigStore) topicNameToKeyBytes(name string) []byte {
	if len(name) == 0 {
		tcs.logger.Fatalf("Invalid topic name: %s", name)
	}
	return []byte(name)
}

// keyBytesToTopicName is a helper method that converts the given byte slice to a topic name.
func (tcs *TopicsConfigStore) keyBytesToTopicName(data []byte) string {
	if len(data) == 0 {
		tcs.logger.Fatalf("Invalid topic name: %v", data)
	}
	return string(data)
}
