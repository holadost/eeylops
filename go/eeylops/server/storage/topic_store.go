package storage

import (
	"eeylops/server/base"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/golang/glog"
	"os"
	"path"
)

const topicStoreDirectory = "topic_store"

// TopicStore holds all the topics for eeylops.
type TopicStore struct {
	kvStore *BadgerKVStore
	rootDir string
	tsDir   string
}

func NewTopicStore(rootDir string) *TopicStore {
	ts := new(TopicStore)
	ts.rootDir = rootDir
	ts.tsDir = path.Join(rootDir, topicStoreDirectory)
	if err := os.MkdirAll(ts.tsDir, 0774); err != nil {
		glog.Fatalf("Unable to create directory for topic store due to err: %v", err)
		return nil
	}
	ts.initialize()
	return ts
}

func (ts *TopicStore) initialize() {
	glog.Infof("Initializing topic store located at: %s", ts.tsDir)
	opts := badger.DefaultOptions(ts.tsDir)
	opts.SyncWrites = true
	opts.NumCompactors = 2
	opts.NumMemtables = 2
	opts.BlockCacheSize = 0
	opts.Compression = options.None
	opts.VerifyValueChecksum = true
	ts.kvStore = NewBadgerKVStore(ts.tsDir, opts)
}

func (ts *TopicStore) Close() error {
	return ts.kvStore.Close()
}

func (ts *TopicStore) AddTopic(topic base.Topic) error {
	glog.Infof("Adding new topic: %s", topic.ToString())
	key := []byte(topic.Name)
	val := ts.marshalTopic(&topic)
	err := ts.kvStore.Put(key, val)
	if err != nil {
		return NewStoreError(fmt.Sprintf("unable to add topic due to err: %s", err.Error()), TopicStoreErr)
	}
	return nil
}

func (ts *TopicStore) MarkTopicForRemoval(topicName string) error {
	glog.Infof("Marking topic: %s for removal", topicName)
	key := []byte(topicName)
	topic, err := ts.GetTopic(topicName)
	if err != nil {
		glog.Errorf("Unable to fetch topic info to mark it for removal due to err: %s", err.Error())
		return NewStoreError(fmt.Sprintf("unable to find topic: %s to mark it for removal", topicName),
			TopicStoreErr)
	}
	topic.ToRemove = true
	val := ts.marshalTopic(&topic)
	if err = ts.kvStore.Put(key, val); err != nil {
		return NewStoreError(fmt.Sprintf("unable to mark topic for removal due to err: %s", err.Error()),
			TopicStoreErr)
	}
	return nil
}

func (ts *TopicStore) RemoveTopic(topicName string) error {
	glog.Infof("Removing topic: %s from topic store", topicName)
	key := []byte(topicName)
	topic, err := ts.GetTopic(topicName)
	if err != nil {
		glog.Errorf("Unable to fetch topic info to mark it for removal due to err: %s", err.Error())
		return NewStoreError(fmt.Sprintf("unable to find topic: %s to mark it for removal", topicName),
			TopicStoreErr)
	}
	if !topic.ToRemove {
		glog.Errorf("Cannot remove topic: %s as it was not previously marked for removal. Topic: %s",
			topicName, topic.ToString())
		return NewStoreError(
			fmt.Sprintf("unable to remove topic: %s as it is not marked for removal", topicName),
			TopicStoreErr)
	}
	err = ts.kvStore.Delete(key)
	if err != nil {
		glog.Errorf("Unable to delete topic: %s due to err: %s", topicName, err.Error())
		return NewStoreError(
			fmt.Sprintf("unable to remove topic: %s due to err: %s", topicName, err.Error()),
			TopicStoreErr)
	}
	return nil
}

func (ts *TopicStore) GetTopic(topicName string) (base.Topic, error) {
	key := []byte(topicName)
	var topic base.Topic
	topicVal, err := ts.kvStore.Get(key)
	if err != nil {
		glog.Errorf("Unable to get topic: %s due to err: %s", topicName, err.Error())
		return topic, NewStoreError(
			fmt.Sprintf("unable to get topic: %s due to err: %s", topicName, err.Error()),
			TopicStoreErr)
	}
	topic = ts.unmarshalTopic(topicVal)
	return topic, nil
}

func (ts *TopicStore) GetAllTopics() ([]base.Topic, error) {
	_, values, _, err := ts.kvStore.Scan(nil, -1)
	if err != nil {
		glog.Errorf("Unable to get all topics in topic store due to err: %s", err.Error())
		return nil, err
	}
	var topics []base.Topic
	for ii := 0; ii < len(values); ii++ {
		topics = append(topics, ts.unmarshalTopic(values[ii]))
	}
	return topics, nil
}

func (ts *TopicStore) Snapshot() error {
	return nil
}

func (ts *TopicStore) Restore() error {
	return nil
}

func (ts *TopicStore) marshalTopic(topic *base.Topic) []byte {
	data, err := json.Marshal(topic)
	if err != nil {
		glog.Fatalf("Unable to marshal topic to JSON due to err: %s", err.Error())
		return []byte{}
	}
	return data
}

func (ts *TopicStore) unmarshalTopic(data []byte) base.Topic {
	var topic base.Topic
	err := json.Unmarshal(data, &topic)
	if err != nil {
		glog.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return topic
}
