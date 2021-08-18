package storage

import (
	"eeylops/server/base"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

const kTopicsDirName = "topics"

type StorageController struct {
	// Backing TopicsConfigStore for topics registered with eeylops.
	topicsConfigStore *TopicsConfigStore
	// Consumer store.
	consumerStore *ConsumerStore
	// In memory map that holds the topics and partitions.
	topicMap map[base.TopicIDType]*topicEntry
	// Read-write lock to protect access to topicMap.
	topicMapLock sync.RWMutex
	// Root directory for this topic controller.
	rootDir string
	// Controller ID.
	controllerID string
	// Store scan interval in seconds.
	storeScanIntervalSecs int
	// Disposer to help remove deleted topics.
	disposer *StorageDisposer
	// Callback channel after disposer has removed topics.
	disposedChan chan string
	// Channel used by janitor and topic removal workflows.
	topicDeletionChan chan *topicEntry
	// Logger.
	logger *logging.PrefixLogger
}

type StorageControllerOpts struct {
	// The root directory for this topic controller.
	RootDirectory string // Root directory for the topic controller.

	// The controller ID. This is the same controller ID that we use for Raft as well. A topic controller is tied
	// to a single raft controller.
	ControllerID string // Controller ID.

	// The interval at which the topic store is scanned by the janitor to dispose removed topics.
	StoreGCScanIntervalSecs int
}

func NewStorageController(opts StorageControllerOpts) *StorageController {
	sc := &StorageController{}
	sc.rootDir = opts.RootDirectory
	sc.controllerID = opts.ControllerID
	sc.storeScanIntervalSecs = opts.StoreGCScanIntervalSecs
	if sc.controllerID == "" {
		glog.Fatalf("No controller ID provided")
	}
	sc.initialize()
	return sc
}

func (sc *StorageController) initialize() {
	sc.topicsConfigStore = NewTopicsConfigStore(sc.getControllerRootDirectory())
	sc.consumerStore = NewConsumerStore(sc.getControllerRootDirectory())
	sc.topicMap = make(map[base.TopicIDType]*topicEntry)
	sc.disposedChan = make(chan string, 128)
	sc.topicDeletionChan = make(chan *topicEntry, 128)

	// Read all the topics from the topic topicsConfigStore and check if the topic directories
	// exist under the given directory.
	glog.Infof("Initializing topic controller. Controller ID: %s", sc.controllerID)
	allTopics, err := sc.topicsConfigStore.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to get all topics in the topic topicsConfigStore due to err: %s", err.Error())
	}
	if len(allTopics) == 0 {
		util.CreateDir(sc.getTopicsRootDirectory())
	} else {
		fsDirs := sc.getFileSystemTopics()
		for _, topic := range allTopics {
			glog.Infof("Initializing topic: %s for controller: %s", topic.Name, sc.controllerID)
			topicDir := sc.getTopicDirectory(topic.Name, topic.ID)
			_, exists := fsDirs[topicDir]
			if !exists {
				glog.Fatalf("Did not find a directory for topic: %s(%d)", topic.Name, topic.ID)
			}
			delete(fsDirs, topicDir)
			if err := os.MkdirAll(topicDir, 0774); err != nil {
				glog.Fatalf("Unable to create topic directory for topic: %s due to err: %s",
					topic.Name, err.Error())
				return
			}
			pMap := make(map[int]*Partition)
			for _, ii := range topic.PartitionIDs {
				opts := PartitionOpts{
					TopicName:     topic.Name,
					PartitionID:   ii,
					RootDirectory: topicDir,
					TTLSeconds:    topic.TTLSeconds,
				}
				pMap[ii] = NewPartition(opts)
			}
			entry := &topicEntry{
				topic:        &topic,
				partitionMap: pMap,
			}
			sc.topicMap[topic.ID] = entry
		}
		sc.disposeZombieTopics(fsDirs)
	}
	go sc.janitor()
}

func (sc *StorageController) GetTopicByName(topicName string) (base.TopicConfig, error) {
	allTopics := sc.GetAllTopics()
	for _, topic := range allTopics {
		if topic.Name == topicName {
			return topic, nil
		}
	}
	glog.Errorf("Did not find any topic named: %s", topicName)
	return base.TopicConfig{}, ErrTopicNotFound
}

func (sc *StorageController) GetTopicByID(topicID base.TopicIDType) (base.TopicConfig, error) {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	val, exists := sc.topicMap[topicID]
	if !exists {
		glog.Errorf("Did not find any topic with ID: %d", topicID)
		return base.TopicConfig{}, ErrTopicNotFound
	}
	return *val.topic, nil
}

func (sc *StorageController) GetAllTopics() []base.TopicConfig {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	var topics []base.TopicConfig
	for _, topicEntry := range sc.topicMap {
		topics = append(topics, *topicEntry.topic)
	}
	sort.SliceStable(topics, func(i int, j int) bool {
		if strings.Compare(topics[i].Name, topics[j].Name) == -1 {
			return true
		}
		return false
	})
	return topics
}

func (sc *StorageController) AddTopic(topic base.TopicConfig) error {
	_, exists := sc.topicMap[topic.ID]
	if exists {
		glog.Errorf("Topic: %s already exists", topic.Name)
		return ErrTopicExists
	}
	tc, err := sc.GetTopicByName(topic.Name)
	if err == nil {
		glog.Errorf("Topic: %s already exists. Current topic: %s", topic.Name, tc.ToString())
		return ErrTopicExists
	}
	topicDirName := sc.getTopicDirectory(topic.Name, topic.ID)
	if err := os.MkdirAll(topicDirName, 0774); err != nil {
		glog.Errorf("Unable to create directory for topic: %s due to err: %s", topic.Name, err.Error())
		return ErrStorageController
	}
	if err := sc.topicsConfigStore.AddTopic(topic); err != nil {
		glog.Errorf("Unable to add topic: %s to topic store due to err: %s", topic.Name, err.Error())
		return ErrStorageController
	}
	partMap := make(map[int]*Partition)
	topicName := topic.Name + fmt.Sprintf("-%d", topic.ID)
	for _, elem := range topic.PartitionIDs {
		opts := PartitionOpts{
			TopicName:     topicName,
			PartitionID:   elem,
			RootDirectory: topicDirName,
			TTLSeconds:    topic.TTLSeconds,
		}
		part := NewPartition(opts)
		partMap[elem] = part
	}
	entry := &topicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	sc.topicMap[topic.ID] = entry
	return nil
}

func (sc *StorageController) RemoveTopic(topicID base.TopicIDType) error {
	te, exists := sc.topicMap[topicID]
	if !exists {
		glog.Errorf("Unable to remove topic as topic: %d was not found", topicID)
		return ErrTopicNotFound
	}
	// Remove topic from the store and map.
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	delete(sc.topicMap, topicID)
	err := sc.topicsConfigStore.RemoveTopic(te.topic.Name)
	if err != nil {
		glog.Errorf("Unable to remove topic due to err: %s", err.Error())
		return err
	}
	return nil
}

func (sc *StorageController) GetPartition(topicID base.TopicIDType, partitionID int) (*Partition, error) {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	entry, exists := sc.topicMap[topicID]
	if !exists {
		glog.Errorf("Unable to find topic: %d", topicID)
		return nil, ErrTopicNotFound
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		glog.Errorf("Unable to find partition: %d for topic: %d", partitionID, topicID)
		return nil, ErrPartitionNotFound
	}
	return partition, nil
}

func (sc *StorageController) GetConsumerStore() *ConsumerStore {
	return sc.consumerStore
}

func (sc *StorageController) getControllerRootDirectory() string {
	return sc.rootDir
}

func (sc *StorageController) getTopicsRootDirectory() string {
	return path.Join(sc.getControllerRootDirectory(), kTopicsDirName)
}

func (sc *StorageController) getTopicDirectory(topicName string, topicID base.TopicIDType) string {
	dirName := topicName + fmt.Sprintf("-%d", topicID)
	return path.Join(sc.getTopicsRootDirectory(), dirName)
}

func (sc *StorageController) getFileSystemTopics() map[string]struct{} {
	rootDir := sc.getTopicsRootDirectory()
	fileInfo, err := ioutil.ReadDir(rootDir)
	fsDirs := make(map[string]struct{})
	if err != nil {
		sc.logger.Fatalf("Unable to read topics root directory due to err: %v", err)
		return nil
	}
	for _, file := range fileInfo {
		if file.IsDir() {
			fsDirs[path.Join(rootDir, file.Name())] = struct{}{}
		}
	}
	return fsDirs
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long-running background goroutine that periodically checks topics that have been marked for removal and
// clears those topics from the underlying storage.
func (sc *StorageController) janitor() {
	glog.Infof("Starting janitor for storage controller: %s", sc.controllerID)
	for {
		select {
		case topicInfo := <-sc.disposedChan:
			// The topic was disposed.
			glog.Infof("Successfully removed topic: %s from underlying storage", topicInfo)
			break
		case te := <-sc.topicDeletionChan:
			sc.closeTopic(te)
			sc.disposeTopic(te)
			break
		}
	}
}

func (sc *StorageController) closeTopic(te *topicEntry) {
	glog.Infof("Closing topic: %s(%d)", te.topic.Name, te.topic.ID)
	for _, prt := range te.partitionMap {
		prt.Close()
	}
}

func (sc *StorageController) createDisposeCb(topicName string, topicID base.TopicIDType) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		glog.Infof("Topic: %s has been successfully disposed", topicName)
		sc.disposedChan <- fmt.Sprintf("%s(%d)", topicName, topicID)
	}
	return cb
}

func (sc *StorageController) disposeZombieTopics(fsDirs map[string]struct{}) {
	for dirPath, _ := range fsDirs {
		cb := func(err error) {
			if err != nil {
				glog.Fatalf("Unable to delete directory: %s due to err: %s", dirPath, err.Error())
			}
			glog.Infof("Successfully deleted zombie topic directory: %s", dirPath)
		}
		sc.disposeDirectory(dirPath, cb)
	}
}

func (sc *StorageController) disposeTopic(te *topicEntry) {
	sc.disposeDirectory(sc.getTopicDirectory(te.topic.Name, te.topic.ID),
		sc.createDisposeCb(te.topic.Name, te.topic.ID))
}

func (sc *StorageController) disposeDirectory(dirPath string, cb func(error)) {
	ds := DefaultDisposer()
	ds.Dispose(dirPath, cb)
}

// topicEntry is a wrapper struct to hold the topic config and the partition(s) of this topic.
type topicEntry struct {
	topic        *base.TopicConfig
	partitionMap map[int]*Partition
}
