package mothership

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	storagebase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/server/storage/kv_store/badger_kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"encoding/json"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path"
	"sync"
)

const kMothershipTopicsConfigStoreDirectory = "topics_config_store"
const kMothershipKVStoreTopicsColumnFamily = "topics_cf"
const kMothershipKVStoreMiscColumnFamily = "misc_cf"
const kMothershipKVStoreLastTopicIDKey = "last_topic_id"
const kMothershipKVStoreLogicalTimestampKey = "last_logical_ts"

var kLastTopicIDBytes = []byte(kMothershipKVStoreLastTopicIDKey)
var kLastLogicalTimestampBytes = []byte(kMothershipKVStoreLogicalTimestampKey)

// MothershipStore holds all the topics for eeylops.
type MothershipStore struct {
	kvStore                      kv_store.KVStore
	logger                       *logging.PrefixLogger
	rootDir                      string
	tsDir                        string
	topicNameMap                 map[string]*base.TopicConfig
	topicIDMap                   map[base.TopicIDType]*base.TopicConfig
	topicMapLock                 sync.RWMutex
	nextTopicID                  base.TopicIDType
	lastRLogIdx                  int64
	topicsConfigLogicalTimestamp int64
}

// NewMothershipStore builds a new MothershipStore instance. ID generation is disabled.
func NewMothershipStore(rootDir string) *MothershipStore {
	msc := new(MothershipStore)
	msc.rootDir = rootDir
	msc.tsDir = path.Join(rootDir, kMothershipTopicsConfigStoreDirectory)
	msc.logger = logging.NewPrefixLogger("mothership_store")
	if err := os.MkdirAll(msc.tsDir, 0774); err != nil {
		msc.logger.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	msc.initialize()
	return msc
}

// initialize the topics config store.
func (mss *MothershipStore) initialize() {
	mss.logger.Infof("Initializing topic store located at: %s", mss.tsDir)
	// Initialize KV store.
	opts := badger.DefaultOptions(mss.tsDir)
	opts.SyncWrites = true
	opts.NumMemtables = 2
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2  // Use 3 compactors.
	opts.IndexCacheSize = 16 * (1024 * 1024)
	opts.Compression = 0
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	mss.kvStore = badger_kv_store.NewBadgerKVStore(mss.tsDir, opts)
	mss.addCfsIfNotExists()

	// Initialize replicated log index, next topic ID and last logical timestamp.
	txn := mss.kvStore.NewTransaction()
	mss.initializeLastRLogIdxFromStore(txn)
	mss.initializeNextTopicIDFromKVStore(txn)
	mss.initializeLastLogicalTs(txn)
	txn.Discard()

	// Initialize internal topic maps.
	mss.topicNameMap = make(map[string]*base.TopicConfig)
	mss.topicIDMap = make(map[base.TopicIDType]*base.TopicConfig)
	allTopics := mss.getTopicsFromKVStore()
	for _, topic := range allTopics {
		mss.addTopicToMaps(topic)
	}
}

// Close the topics store.
func (mss *MothershipStore) Close() error {
	return mss.kvStore.Close()
}

// AddTopic adds the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (mss *MothershipStore) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	sanitize := func() error {
		mss.topicMapLock.RLock()
		defer mss.topicMapLock.RUnlock()
		if topic.ID > 0 {
			mss.logger.Fatalf("Topic ID generation enabled but received topic with an ID. Topic: %s",
				topic.ToString())
		}
		topic.ID = mss.nextTopicID
		if rLogIdx <= mss.lastRLogIdx {
			mss.logger.Warningf("A higher replicated log index: %d exists in the topics store. "+
				"Ignoring this request with log index: %d to add topic: %s", mss.lastRLogIdx, rLogIdx, topic.Name)
			return storage.ErrInvalidRLogIdx
		}
		_, exists := mss.topicNameMap[topic.Name]
		if exists {
			mss.logger.Warningf("Topic named: %s already exists. Not adding topic again", topic.Name)
			return storage.ErrTopicExists
		}
		return nil
	}
	if err := sanitize(); err != nil {
		return err
	}

	mss.logger.Infof("Adding new topic: \n---------------%s\n---------------", topic.ToString())
	var entries []*kv_store.KVStoreEntry
	// Add replicated log index.
	rLogKey, rLogVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
	var rlogEntry kv_store.KVStoreEntry
	rlogEntry.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	rlogEntry.Key = rLogKey
	rlogEntry.Value = rLogVal

	// Add next topic id entry.
	var ntEntry kv_store.KVStoreEntry
	ntEntry.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	ntEntry.Key = kLastTopicIDBytes
	ntEntry.Value = mss.topicIdToBytes(topic.ID)

	// Add topic entry.
	var topicEntry kv_store.KVStoreEntry
	topicEntry.ColumnFamily = kMothershipKVStoreTopicsColumnFamily
	topicEntry.Key = mss.topicNameToKeyBytes(topic.Name)
	topicEntry.Value = mss.marshalTopic(&topic)

	// Update logical timestamp as well.
	var ltsEntry kv_store.KVStoreEntry
	ltsEntry.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	ltsEntry.Key = kLastLogicalTimestampBytes
	ltsEntry.Value = mss.logicalTimestampToBytes(mss.topicsConfigLogicalTimestamp + 1)

	entries = append(entries, &rlogEntry, &ntEntry, &topicEntry, &ltsEntry)

	mss.topicMapLock.Lock()
	defer mss.topicMapLock.Unlock()
	err := mss.kvStore.BatchPut(entries)
	if err != nil {
		mss.logger.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return storage.ErrTopicStore
	}
	mss.addTopicToMaps(&topic)

	// Update internal state
	mss.topicsConfigLogicalTimestamp++
	mss.nextTopicID++
	mss.lastRLogIdx = rLogIdx
	return nil
}

// RemoveTopic removes the given topic to the store. This method is not concurrent-safe.
// rLogIdx specifies the replicated log index for this request.
func (mss *MothershipStore) RemoveTopic(topicId base.TopicIDType, rLogIdx int64) error {
	var tpc *base.TopicConfig
	var exists bool
	sanitize := func() error {
		mss.topicMapLock.RLock()
		defer mss.topicMapLock.RUnlock()
		tpc, exists = mss.topicIDMap[topicId]
		if !exists {
			return storage.ErrTopicNotFound
		}
		_, exists = mss.topicNameMap[tpc.Name]
		if !exists {
			mss.logger.Fatalf("Found topic named: %s in name map but not in ID map. Topic ID: %d",
				tpc.Name, tpc.ID)
		}
		if rLogIdx <= mss.lastRLogIdx {
			mss.logger.Warningf("A higher log index already exists in the topics config store. " +
				"Ignoring this request")
			return storage.ErrInvalidRLogIdx
		}
		return nil
	}
	if err := sanitize(); err != nil {
		return err
	}

	mss.logger.Infof("Removing topic: %d from topic store", topicId)
	key := kv_store.KVStoreKey{
		ColumnFamily: kMothershipKVStoreTopicsColumnFamily,
		Key:          mss.topicNameToKeyBytes(tpc.Name),
	}
	var entries []*kv_store.KVStoreEntry
	var rlogEntry kv_store.KVStoreEntry
	var ltsEntry kv_store.KVStoreEntry
	rLogKey, rLogVal := storagebase.PrepareRLogIDXKeyVal(rLogIdx)
	rlogEntry.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	rlogEntry.Key = rLogKey
	rlogEntry.Value = rLogVal
	ltsEntry.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	ltsEntry.Key = kLastLogicalTimestampBytes
	ltsEntry.Value = mss.logicalTimestampToBytes(mss.topicsConfigLogicalTimestamp + 1)
	entries = append(entries, &rlogEntry, &ltsEntry)

	// Acquire write lock on the store.
	mss.topicMapLock.Lock()
	defer mss.topicMapLock.Unlock()
	txn := mss.kvStore.NewTransaction()
	defer txn.Discard()
	// Delete the topic.
	if err := txn.Delete(&key); err != nil {
		mss.logger.Errorf("Unable to delete topic: %s due to err: %s", tpc.Name, err.Error())
		return storage.ErrTopicStore
	}
	// Update logical timestamp and replicated log index.
	if err := txn.BatchPut(entries); err != nil {
		mss.logger.Errorf("Unable to add replicated log index: %d due to err: %s", rLogIdx, err.Error())
		return storage.ErrTopicStore
	}
	if err := txn.Commit(); err != nil {
		mss.logger.Errorf("Unable to commit transaction to remove topic: %s due to err: %s", tpc.Name,
			err.Error())
		return storage.ErrTopicStore
	}
	mss.removeTopicFromMaps(tpc)
	mss.lastRLogIdx = rLogIdx
	mss.topicsConfigLogicalTimestamp++
	return nil
}

// GetTopicByName fetches the topic by its name.
func (mss *MothershipStore) GetTopicByName(topicName string) (base.TopicConfig, error) {
	mss.topicMapLock.RLock()
	defer mss.topicMapLock.RUnlock()
	tpc, exists := mss.topicNameMap[topicName]
	if !exists {
		return base.TopicConfig{}, storage.ErrTopicNotFound
	}
	return *tpc, nil
}

// GetTopicByID fetches the topic by its ID.
func (mss *MothershipStore) GetTopicByID(topicID base.TopicIDType) (base.TopicConfig, error) {
	mss.topicMapLock.RLock()
	defer mss.topicMapLock.RUnlock()
	tpc, exists := mss.topicIDMap[topicID]
	if !exists {
		return base.TopicConfig{}, storage.ErrTopicNotFound
	}
	return *tpc, nil
}

// GetAllTopics fetches all topics in the store.
func (mss *MothershipStore) GetAllTopics() []base.TopicConfig {
	mss.topicMapLock.RLock()
	defer mss.topicMapLock.RUnlock()
	var topics []base.TopicConfig
	mss.logger.Infof("Total number of topics in maps: %d", len(mss.topicIDMap))
	for _, topic := range mss.topicIDMap {
		topics = append(topics, *topic)
	}
	return topics
}

// GetLastTopicIDCreated returns the last topic ID that was created.
func (mss *MothershipStore) GetLastTopicIDCreated() base.TopicIDType {
	return mss.nextTopicID - 1
}

// Snapshot creates a snapshot of the store.
func (mss *MothershipStore) Snapshot() error {
	return nil
}

// Restore restores the store from a given snapshot.
func (mss *MothershipStore) Restore() error {
	return nil
}

// getTopicsFromKVStore fetches all the topics from the underlying KV store.
func (mss *MothershipStore) getTopicsFromKVStore() []*base.TopicConfig {
	var topics []*base.TopicConfig
	scanner, err := mss.kvStore.NewScanner(kMothershipKVStoreTopicsColumnFamily, nil, false)
	if err != nil {
		mss.logger.Fatalf("Unable to get all topics in topic store due to err: %s", err.Error())
	}
	for ; scanner.Valid(); scanner.Next() {
		_, val, err := scanner.GetItem()
		if err != nil {
			mss.logger.Fatalf("Unable to scan item due to err: %v", err)
		}
		topics = append(topics, mss.unmarshalTopic(val))
	}
	mss.logger.Infof("Total number of topics from KV store: %d", len(topics))
	return topics
}

// marshalTopic serializes the given topic config.
func (mss *MothershipStore) marshalTopic(topic *base.TopicConfig) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		mss.logger.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

// unmarshalTopic deserializes the given data to TopicConfig.
func (mss *MothershipStore) unmarshalTopic(data []byte) *base.TopicConfig {
	var topic base.TopicConfig
	err := json.Unmarshal(data, &topic)
	if err != nil {
		mss.logger.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return &topic
}

// addTopicToMaps adds the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (mss *MothershipStore) addTopicToMaps(topic *base.TopicConfig) {
	if len(mss.topicIDMap) != len(mss.topicNameMap) {
		mss.logger.Fatalf("Topics mismatch in topicNameMap and topicIDMap. Got: %d, %d", len(mss.topicNameMap),
			len(mss.topicIDMap))
	}
	mss.topicNameMap[topic.Name] = topic
	mss.topicIDMap[topic.ID] = topic
}

// removeTopicFromMaps removes the given topic to the maps. This method assumes the topicMapLock has been acquired.
func (mss *MothershipStore) removeTopicFromMaps(topic *base.TopicConfig) {
	if len(mss.topicIDMap) != len(mss.topicNameMap) {
		mss.logger.Fatalf("Topics mismatch in topicNameMap and topicIDMap. Got: %d, %d", len(mss.topicNameMap),
			len(mss.topicIDMap))
	}
	_, exists := mss.topicNameMap[topic.Name]
	if !exists {
		mss.logger.Fatalf("Did not find topic: %s in name map", topic.Name)
	}
	_, exists = mss.topicIDMap[topic.ID]
	if !exists {
		mss.logger.Fatalf("Did not find topic: %s[%d] in name map", topic.Name, topic.ID)
	}
	delete(mss.topicNameMap, topic.Name)
	delete(mss.topicIDMap, topic.ID)
}

// initializeNextTopicIDFromKVStore fetches the next topic ID from the underlying KV store that will be used for new topics.
func (mss *MothershipStore) initializeNextTopicIDFromKVStore(txn kv_store.Transaction) {
	key := kv_store.KVStoreKey{
		Key:          kLastTopicIDBytes,
		ColumnFamily: kMothershipKVStoreMiscColumnFamily,
	}
	val, err := txn.Get(&key)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// No topics have been created yet.
			mss.logger.Infof("DID NOT find any topics in the topics store. Starting from topic ID: 1")
			mss.nextTopicID = 1
			return
		} else {
			mss.logger.Fatalf("Unable to initialize next topic ID due to err: %s", err.Error())
		}
	}
	mss.nextTopicID = base.TopicIDType(util.BytesToUint(val.Value)) + 1
}

// initializeNextTopicIDFromKVStore fetches the next topic ID from the underlying KV store that will be used for new topics.
func (mss *MothershipStore) initializeLastLogicalTs(txn kv_store.Transaction) {
	key := kv_store.KVStoreKey{
		Key:          kLastLogicalTimestampBytes,
		ColumnFamily: kMothershipKVStoreMiscColumnFamily,
	}
	val, err := txn.Get(&key)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			mss.topicsConfigLogicalTimestamp = 0
			return
		} else {
			mss.logger.Fatalf("Unable to initialize next topic ID due to err: %s", err.Error())
		}
	}
	mss.topicsConfigLogicalTimestamp = int64(util.BytesToUint(val.Value))
}

// initializeNextTopicIDFromKVStore fetches the next topic ID from the underlying KV store that will be used for new topics.
func (mss *MothershipStore) initializeLastRLogIdxFromStore(txn kv_store.Transaction) {
	rLogKey, _ := storagebase.PrepareRLogIDXKeyVal(0)
	var key kv_store.KVStoreKey
	key.Key = rLogKey
	key.ColumnFamily = kMothershipKVStoreMiscColumnFamily
	lastVal, err := txn.Get(&key)
	if err != nil {
		if err != kv_store.ErrKVStoreKeyNotFound {
			mss.logger.Fatalf("Unable to initialize topics config store due to err: %s", err.Error())
		}
		mss.lastRLogIdx = -1
	} else {
		mss.lastRLogIdx = int64(util.BytesToUint(lastVal.Value))
		mss.logger.Infof("Last replicated log index in topics config store: %d", mss.lastRLogIdx)
	}
}

func (mss *MothershipStore) addCfsIfNotExists() {
	// Add column families if they don't exist.
	cfs := []string{kMothershipKVStoreTopicsColumnFamily, kMothershipKVStoreMiscColumnFamily}
	for _, cf := range cfs {
		if err := mss.kvStore.AddColumnFamily(cf); err != nil {
			if err != kv_store.ErrKVStoreColumnFamilyExists {
				mss.logger.Fatalf("Unable to add column family: %s due to err: %v", cf, err)
			}
		}
	}
}

func (mss *MothershipStore) topicIdToBytes(topicID base.TopicIDType) []byte {
	if topicID <= 0 {
		mss.logger.Fatalf("Invalid topic ID: %d", topicID)
	}
	return util.UintToBytes(uint64(topicID))
}

func (mss *MothershipStore) bytesToTopicID(data []byte) base.TopicIDType {
	if len(data) != 8 {
		mss.logger.Fatalf("Invalid topic ID data as it %d bytes when we are expecting only 8", len(data))
	}
	return base.TopicIDType(util.BytesToUint(data))
}

func (mss *MothershipStore) logicalTimestampToBytes(logicalTs int64) []byte {
	if logicalTs <= 0 {
		mss.logger.Fatalf("Invalid logical timestamp: %d", logicalTs)
	}
	return util.UintToBytes(uint64(logicalTs))
}

func (mss *MothershipStore) bytesToLogicalTimestamp(data []byte) int64 {
	if len(data) != 8 {
		mss.logger.Fatalf("Invalid logical timestamp data as it %d bytes when we are expecting only 8",
			len(data))
	}
	return int64(util.BytesToUint(data))
}

func (mss *MothershipStore) topicNameToKeyBytes(name string) []byte {
	if len(name) == 0 {
		mss.logger.Fatalf("Invalid topic name: %s", name)
	}
	return []byte(name)
}

func (mss *MothershipStore) keyBytesToTopicName(data []byte) string {
	if len(data) == 0 {
		mss.logger.Fatalf("Invalid topic: %v", data)
	}
	return string(data)
}
