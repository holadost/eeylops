package storage

import (
	"eeylops/server/base"
	"eeylops/server/storage/kv_store"
	"encoding/json"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/golang/glog"
	"os"
	"path"
)

const kTopicsConfigStoreDirectory = "topics_config_store"

// TopicsConfigStore holds all the topics for eeylops.
type TopicsConfigStore struct {
	kvStore *kv_store.BadgerKVStore
	rootDir string
	tsDir   string
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
}

func (tcs *TopicsConfigStore) Close() error {
	return tcs.kvStore.Close()
}

func (tcs *TopicsConfigStore) AddTopic(topic base.TopicConfig) error {
	glog.Infof("Adding new topic: \n---------------%s\n---------------", topic.ToString())
	key := []byte(topic.Name)
	val := tcs.marshalTopic(&topic)
	err := tcs.kvStore.Put(key, val)
	if err != nil {
		glog.Errorf("Unable to add topic: %s due to err: %s", topic.Name, err.Error())
		return ErrTopicStore
	}
	return nil
}

func (tcs *TopicsConfigStore) RemoveTopic(topicName string) error {
	glog.Infof("Removing topic: %s from topic store", topicName)
	key := []byte(topicName)
	_, err := tcs.GetTopic(topicName)
	if err != nil {
		glog.Errorf("Unable to fetch topic info to mark it for removal due to err: %s", err.Error())
		if err == kv_store.ErrKVStoreKeyNotFound {
			return ErrTopicNotFound
		}
		return ErrTopicStore
	}
	err = tcs.kvStore.Delete(key)
	if err != nil {
		glog.Errorf("Unable to delete topic: %s due to err: %s", topicName, err.Error())
		return ErrTopicStore
	}
	return nil
}

func (tcs *TopicsConfigStore) GetTopic(topicName string) (base.TopicConfig, error) {
	key := []byte(topicName)
	var topic base.TopicConfig
	topicVal, err := tcs.kvStore.Get(key)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			return topic, ErrTopicNotFound
		}
		glog.Errorf("Unable to get topic: %s due to err: %s", topicName, err.Error())
		return topic, ErrTopicStore
	}
	topic = tcs.unmarshalTopic(topicVal)
	return topic, nil
}

func (tcs *TopicsConfigStore) GetAllTopics() ([]base.TopicConfig, error) {
	_, values, _, err := tcs.kvStore.Scan(nil, -1, -1, false)
	glog.Infof("Total number of topics in the store: %d", len(values))
	if err != nil {
		glog.Errorf("Unable to get all topics in topic store due to err: %s", err.Error())
		return nil, ErrTopicStore
	}
	var topics []base.TopicConfig
	for ii := 0; ii < len(values); ii++ {
		topics = append(topics, tcs.unmarshalTopic(values[ii]))
	}
	return topics, nil
}

func (tcs *TopicsConfigStore) Snapshot() error {
	return nil
}

func (tcs *TopicsConfigStore) Restore() error {
	return nil
}

func (tcs *TopicsConfigStore) marshalTopic(topic *base.TopicConfig) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		glog.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

func (tcs *TopicsConfigStore) unmarshalTopic(data []byte) base.TopicConfig {
	var topic base.TopicConfig
	err := json.Unmarshal(data, &topic)
	if err != nil {
		glog.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return topic
}
