package broker

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"
)

const kTopicsDirName = "topics"
const kExpiredTopicsDirSuffix = "---expired"

type BrokerStore struct {
	// Consumer store.
	consumerStore *storage.ConsumerStore
	// Local topics store.
	topicsStore *storage.TopicsConfigStore
	// In memory map that holds the topics and partitions.
	topicMap map[base.TopicIDType]*topicEntry
	// Read-write lock to protect access to topicMap.
	topicMapLock sync.RWMutex
	// Root directory for this topic broker.
	rootDir string
	// Broker ID.
	brokerID string
	// Store scan interval in seconds.
	storeScanIntervalSecs int
	// Disposer to help remove deleted topics.
	disposer *storage.StorageDisposer
	// Callback channel after disposer has removed topics.
	disposedChan chan string
	// Channel used by manager and topic removal workflows.
	topicDeletionChan chan *topicEntry
	// Logger.
	logger *logging.PrefixLogger
}

type BrokerStoreOpts struct {
	// The root directory for this topic broker.
	RootDirectory string // Root directory for the topic broker.

	// The broker ID. This is the same broker ID that we use for Raft as well.
	BrokerID string // Controller ID.

	// The interval at which the topic store is scanned by the manager to dispose removed topics.
	StoreGCScanIntervalSecs int
}

func NewBrokerStore(opts BrokerStoreOpts) *BrokerStore {
	bs := &BrokerStore{}
	bs.rootDir = opts.RootDirectory
	bs.brokerID = opts.BrokerID
	bs.storeScanIntervalSecs = opts.StoreGCScanIntervalSecs
	bs.logger = logging.NewPrefixLogger(bs.brokerID)
	if bs.brokerID == "" {
		bs.logger.Fatalf("No broker ID provided")
	}
	bs.initialize()
	return bs
}

func (bs *BrokerStore) initialize() {
	bs.consumerStore = storage.NewConsumerStore(bs.getBrokerRootDirectory(), bs.brokerID)
	bs.topicsStore = storage.NewTopicsConfigStore(bs.getBrokerRootDirectory(), bs.brokerID)
	bs.topicMap = make(map[base.TopicIDType]*topicEntry)
	bs.disposedChan = make(chan string, 128)
	bs.topicDeletionChan = make(chan *topicEntry, 128)

	// Read all the topics from the topic topicsConfigStore and check if the topic directories
	// exist under the given directory.
	bs.logger.Infof("Initializing broker store. Broker ID: %s", bs.brokerID)
	util.CreateDir(bs.getTopicsRootDirectory())
	fsDirs := bs.getFileSystemTopics()
	allTopics := bs.GetAllTopics()
	for _, topic := range allTopics {
		bs.logger.Infof("Initializing topic: %s for broker: %s", topic.Name, bs.brokerID)
		topicDir := bs.getTopicDirectory(topic.Name, topic.ID)
		_, exists := fsDirs[topicDir]
		if !exists {
			bs.logger.Fatalf("Did not find a directory for topic: %s(%d)", topic.Name, topic.ID)
		}
		delete(fsDirs, topicDir)
		if err := os.MkdirAll(topicDir, 0774); err != nil {
			bs.logger.Fatalf("Unable to create topic directory for topic: %s due to err: %s",
				topic.Name, err.Error())
			return
		}
		pMap := make(map[int]*storage.Partition)
		for _, prtId := range topic.PartitionIDs {
			opts := storage.PartitionOpts{
				TopicName:     bs.generateTopicName(topic.Name, topic.ID),
				PartitionID:   prtId,
				RootDirectory: topicDir,
				TTLSeconds:    topic.TTLSeconds,
			}
			pMap[prtId] = storage.NewPartition(opts)
		}
		entry := &topicEntry{
			topic:        &topic,
			partitionMap: pMap,
		}
		bs.topicMap[topic.ID] = entry
	}
	// The topics remaining in fsDirs are no longer present in the store. Remove them.
	bs.disposeZombieTopics(fsDirs)
	go bs.manager()
}

// AddTopic adds the given topic to the store, creates backing partitions
func (bs *BrokerStore) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	_, exists := bs.topicMap[topic.ID]
	if exists {
		bs.logger.Errorf("Topic: %s already exists", topic.Name)
		return storage.ErrTopicExists
	}
	topicDirName := bs.getTopicDirectory(topic.Name, topic.ID)
	if err := os.MkdirAll(topicDirName, 0774); err != nil {
		bs.logger.Errorf("Unable to create directory for topic: %s due to err: %s", topic.Name, err.Error())
		return storage.ErrStorageController
	}
	partMap := make(map[int]*storage.Partition)
	for _, elem := range topic.PartitionIDs {
		opts := storage.PartitionOpts{
			TopicName:     bs.generateTopicName(topic.Name, topic.ID),
			PartitionID:   elem,
			RootDirectory: topicDirName,
			TTLSeconds:    topic.TTLSeconds,
		}
		part := storage.NewPartition(opts)
		partMap[elem] = part
	}
	entry := &topicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	if err := bs.topicsStore.AddTopic(topic, rLogIdx); err != nil {
		return err
	}
	bs.topicMapLock.Lock()
	defer bs.topicMapLock.Unlock()
	bs.topicMap[topic.ID] = entry
	return nil
}

func (bs *BrokerStore) RemoveTopic(topicID base.TopicIDType, rLogIdx int64) error {
	_, err := bs.GetTopicByID(topicID)
	if err != nil {
		bs.logger.Errorf("Unable to remove topic as topic: %d was not found", topicID)
		return storage.ErrTopicNotFound
	}
	te := bs.removeTopicFromMap(topicID)
	if err = bs.topicsStore.RemoveTopic(topicID, rLogIdx); err != nil {
		return err
	}
	bs.topicDeletionChan <- te
	return nil
}

func (bs *BrokerStore) GetAllTopics() []base.TopicConfig {
	return bs.topicsStore.GetAllTopics()
}

func (bs *BrokerStore) GetPartition(topicID base.TopicIDType, partitionID int) (*storage.Partition, error) {
	bs.topicMapLock.RLock()
	defer bs.topicMapLock.RUnlock()
	entry, exists := bs.topicMap[topicID]
	if !exists {
		bs.logger.Errorf("Unable to find topic: %d", topicID)
		return nil, storage.ErrTopicNotFound
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		bs.logger.Errorf("Unable to find partition: %d for topic: %d", partitionID, topicID)
		return nil, storage.ErrPartitionNotFound
	}
	return partition, nil
}

func (bs *BrokerStore) GetConsumerStore() *storage.ConsumerStore {
	return bs.consumerStore
}

// GetTopicByName returns the topic whose name matches the given topicName.
func (bs *BrokerStore) GetTopicByName(topicName string) (base.TopicConfig, error) {
	return bs.topicsStore.GetTopicByName(topicName)
}

// GetTopicByID returns the topic based on the given ID.
func (bs *BrokerStore) GetTopicByID(id base.TopicIDType) (base.TopicConfig, error) {
	return bs.topicsStore.GetTopicByID(id)
}

/*************************************************** Helper methods ***************************************************/

// getBrokerRootDirectory returns the root directory for the broker.
func (bs *BrokerStore) getBrokerRootDirectory() string {
	return bs.rootDir
}

// getTopicsRootDirectory returns the root directory of all topics.
func (bs *BrokerStore) getTopicsRootDirectory() string {
	return path.Join(bs.getBrokerRootDirectory(), kTopicsDirName)
}

// getTopicDirectory returns the directory for the given topic.
func (bs *BrokerStore) getTopicDirectory(topicName string, topicID base.TopicIDType) string {
	dirName := fmt.Sprintf("%s-%d", topicName, topicID)
	return path.Join(bs.getTopicsRootDirectory(), dirName)
}

// getFileSystemTopics returns the topics that have backing file system directories and files.
func (bs *BrokerStore) getFileSystemTopics() map[string]struct{} {
	rootDir := bs.getTopicsRootDirectory()
	fileInfo, err := ioutil.ReadDir(rootDir)
	fsDirs := make(map[string]struct{})
	if err != nil {
		bs.logger.Fatalf("Unable to read topics root directory due to err: %v", err)
		return nil
	}
	for _, file := range fileInfo {
		if file.IsDir() {
			fsDirs[path.Join(rootDir, file.Name())] = struct{}{}
		}
	}
	return fsDirs
}

// generateTopicName generates the topic name based on the given name and id.
func (bs *BrokerStore) generateTopicName(name string, id base.TopicIDType) string {
	return fmt.Sprintf("%s-%d", name, id)
}

// removeTopicFromMap removes the given topicID from the map.
func (bs *BrokerStore) removeTopicFromMap(topicID base.TopicIDType) *topicEntry {
	bs.topicMapLock.Lock()
	defer bs.topicMapLock.Unlock()
	te, exists := bs.topicMap[topicID]
	if !exists {
		bs.logger.Fatalf("Did not find topic: %d in the map", topicID)
	}
	delete(bs.topicMap, topicID)
	return te
}

/******************************************* BROKER STORE MANAGER *****************************************************/
// manager is a long-running background goroutine that periodically checks topics that have been marked for removal and
// removes those topics and its associated partitions from the underlying storage.
func (bs *BrokerStore) manager() {
	bs.logger.Infof("Starting broker store manager")
	scanInterval := time.Duration(time.Second * 3600)
	if bs.storeScanIntervalSecs > 0 {
		scanInterval = time.Second * time.Duration(bs.storeScanIntervalSecs)
	}
	ticker := time.NewTicker(scanInterval)
	for {
		select {
		case topicInfo := <-bs.disposedChan:
			// The topic was disposed.
			bs.logger.Infof("Successfully removed topic: %s from underlying storage", topicInfo)
		case te := <-bs.topicDeletionChan:
			bs.closeTopic(te)
			bs.disposeTopic(te)
		case <-ticker.C:
			bs.gcConsumerStore()
		}
	}
}

// closeTopic closes all the partitions that belong to the given topic.
func (bs *BrokerStore) closeTopic(te *topicEntry) {
	bs.logger.Infof("Closing topic: %s(%d)", te.topic.Name, te.topic.ID)
	for _, prt := range te.partitionMap {
		prt.Close()
	}
}

// createDisposeCb returns a decorated function that can be used as callback when deleting topics.
func (bs *BrokerStore) createDisposeCb(topicName string, topicID base.TopicIDType) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		bs.logger.Infof("Topic: %s has been successfully disposed", topicName)
		bs.disposedChan <- fmt.Sprintf("%s(%d)", topicName, topicID)
	}
	return cb
}

// disposeZombieTopics removes all topics from the underlying file system that no longer have any records in the
// store.
func (bs *BrokerStore) disposeZombieTopics(fsDirs map[string]struct{}) {
	for dirPath, _ := range fsDirs {
		cb := func(err error) {
			if err != nil {
				bs.logger.Fatalf("Unable to delete directory: %s due to err: %s", dirPath, err.Error())
			}
			bs.logger.Infof("Successfully deleted zombie topic directory: %s", dirPath)
		}
		bs.disposeDirectory(dirPath, cb)
	}
}

// disposeTopic marks a topic to be deleted.
func (bs *BrokerStore) disposeTopic(te *topicEntry) {
	oldPath := bs.getTopicDirectory(te.topic.Name, te.topic.ID)
	newPath := path.Join(path.Dir(oldPath), bs.getExpiredDirName(path.Base(oldPath)))
	util.RenameDir(oldPath, newPath)
	bs.disposeDirectory(newPath, bs.createDisposeCb(te.topic.Name, te.topic.ID))
}

// disposeDirectory is a helper method that asks the default disposer to remove the given directory.
func (bs *BrokerStore) disposeDirectory(dirPath string, cb func(error)) {
	ds := storage.DefaultDisposer()
	ds.Dispose(dirPath, cb)
}

func (bs *BrokerStore) getExpiredDirName(dirName string) string {
	return fmt.Sprintf("%s%s", dirName, kExpiredTopicsDirSuffix)
}

/************************************** CONSUMER STORE GC *****************************************************/
func (bs *BrokerStore) gcConsumerStore() {
	allTopics := bs.GetAllTopics()
	var allTopicIds []base.TopicIDType
	for _, topic := range allTopics {
		allTopicIds = append(allTopicIds, topic.ID)
	}
	bs.consumerStore.RemoveNonExistentTopicConsumers(allTopicIds)
}

// topicEntry is a wrapper struct to hold the topic config and the partition(s) of this topic.
type topicEntry struct {
	topic        *base.TopicConfig
	partitionMap map[int]*storage.Partition
}
