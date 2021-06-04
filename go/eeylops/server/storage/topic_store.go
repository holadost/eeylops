package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
)

const topicStoreDirectory = "topic_store"

type Topic struct {
	Name         string `json:"name"`
	PartitionIDs []uint `json:"partition_ids"`
	TTLSeconds   int    `json:"ttl_seconds"`
	ToRemove     bool   `json:"to_remove"`
}

// TopicStore holds all the topics for eeylops. TopicStore is not thread-safe and need not be since
// all callers will modify topics through raft and Raft will not allow multiple commands to be
// executed in parallel.
type TopicStore struct {
	kvStore  *BadgerKVStore
	topicMap map[string]*Topic
	rootDir  string
	tsDir    string
	lock     sync.RWMutex
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
	glog.Infof("Initializing consumer store located at: %s", ts.tsDir)
	opts := badger.DefaultOptions(ts.tsDir)
	opts.SyncWrites = true
	opts.NumCompactors = 2
	opts.NumMemtables = 2
	opts.BlockCacheSize = 0
	opts.Compression = options.None
	opts.MaxLevels = 1
	opts.VerifyValueChecksum = true
	ts.kvStore = NewBadgerKVStore(ts.tsDir, opts)
}

func (ts *TopicStore) AddTopic(topic Topic) error {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	key := []byte(topic.Name)
	val := ts.marshalTopic(&topic)
	err := ts.kvStore.Put(key, val)
	if err != nil {
		return fmt.Errorf("unable to add topic due to err: %w", err)
	}
	return nil
}

func (ts *TopicStore) MarkTopicForRemoval(topicName string) error {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	key := []byte(topicName)
	topic, exists := ts.topicMap[topicName]
	if !exists {
		return fmt.Errorf("unable to find topic in the topic map")
	}
	topic.ToRemove = true
	val := ts.marshalTopic(topic)
	err := ts.kvStore.Put(key, val)
	if err != nil {
		return fmt.Errorf("unable to mark topic for removal due to err: %w", err)
	}
	topic.ToRemove = true
	return nil
}

func (ts *TopicStore) RemoveTopic(topicName string) error {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	key := []byte(topicName)
	topic, exists := ts.topicMap[topicName]
	if !exists {
		return fmt.Errorf("unable to find topic in the topic map")
	}
	if !topic.ToRemove {
		glog.Errorf("Cannot remove topic as it was not previously marked for removal.")
		return fmt.Errorf("unable to remove topic as it is not marked for removal")
	}
	err := ts.kvStore.Delete(key)
	if err != nil {
		glog.Errorf("Unable to delete topic due to err: %s", err.Error())
		return fmt.Errorf("unable to remove topic due to err: %w", err)
	}
	delete(ts.topicMap, topicName)
	return nil
}

func (ts *TopicStore) GetTopic(topicName string) (Topic, error) {
	ts.lock.RLock()
	defer ts.lock.RUnlock()
	topic, exists := ts.topicMap[topicName]
	if !exists {
		return Topic{}, fmt.Errorf("unable to find topic in the topic map")
	}
	return *topic, nil
}

func (ts *TopicStore) GetAllTopicNames() (topics []string) {
	ts.lock.RLock()
	defer ts.lock.RUnlock()
	for key, _ := range ts.topicMap {
		topics = append(topics, key)
	}
	return
}

func (ts *TopicStore) Snapshot() error {
	return nil
}

func (ts *TopicStore) Restore() error {
	return nil
}

func (ts *TopicStore) marshalTopic(topic *Topic) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	var topicBytes []byte
	if err := enc.Encode(topic); err != nil {
		glog.Fatalf("Unable to serialize topic due to err: %s", err.Error())
	}
	buf.Write(topicBytes)
	return topicBytes
}

func (ts *TopicStore) unmarshalTopic(data []byte) *Topic {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var t Topic
	if err := dec.Decode(&t); err != nil {
		glog.Fatalf("Unable to deserialize topic due to err: %s", err.Error())
	}
	return &t
}
