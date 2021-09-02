package storage

import (
	"eeylops/server/base"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

const kTopicsDirName = "topics"
const kExpiredTopicsDirSuffix = "---expired"

type StorageController struct {
	// Consumer store.
	consumerStore *ConsumerStore
	// Local topics store.
	topicsStore *TopicsConfigStore
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
	sc.logger = logging.NewPrefixLogger(sc.controllerID)
	if sc.controllerID == "" {
		sc.logger.Fatalf("No controller ID provided")
	}
	sc.initialize()
	return sc
}

func (sc *StorageController) initialize() {
	sc.consumerStore = NewConsumerStore(sc.getControllerRootDirectory())
	sc.topicsStore = NewTopicsConfigStore(sc.getControllerRootDirectory())
	sc.topicMap = make(map[base.TopicIDType]*topicEntry)
	sc.disposedChan = make(chan string, 128)
	sc.topicDeletionChan = make(chan *topicEntry, 128)

	// Read all the topics from the topic topicsConfigStore and check if the topic directories
	// exist under the given directory.
	sc.logger.Infof("Initializing topic controller. Controller ID: %s", sc.controllerID)
	util.CreateDir(sc.getTopicsRootDirectory())
	fsDirs := sc.getFileSystemTopics()
	allTopics := sc.GetAllTopics()
	for _, topic := range allTopics {
		sc.logger.Infof("Initializing topic: %s for controller: %s", topic.Name, sc.controllerID)
		topicDir := sc.getTopicDirectory(topic.Name, topic.ID)
		_, exists := fsDirs[topicDir]
		if !exists {
			sc.logger.Fatalf("Did not find a directory for topic: %s(%d)", topic.Name, topic.ID)
		}
		delete(fsDirs, topicDir)
		if err := os.MkdirAll(topicDir, 0774); err != nil {
			sc.logger.Fatalf("Unable to create topic directory for topic: %s due to err: %s",
				topic.Name, err.Error())
			return
		}
		pMap := make(map[int]*Partition)
		for _, prtId := range topic.PartitionIDs {
			opts := PartitionOpts{
				TopicName:     sc.generateTopicName(topic.Name, topic.ID),
				PartitionID:   prtId,
				RootDirectory: topicDir,
				TTLSeconds:    topic.TTLSeconds,
			}
			pMap[prtId] = NewPartition(opts)
		}
		entry := &topicEntry{
			topic:        &topic,
			partitionMap: pMap,
		}
		sc.topicMap[topic.ID] = entry
	}
	// The topics remaining in fsDirs are no longer present in the store. Remove them.
	sc.disposeZombieTopics(fsDirs)
	go sc.janitor()
}

// AddTopic adds the given topic to the store, creates backing partitions
func (sc *StorageController) AddTopic(topic base.TopicConfig, rLogIdx int64) error {
	_, exists := sc.topicMap[topic.ID]
	if exists {
		sc.logger.Errorf("Topic: %s already exists", topic.Name)
		return ErrTopicExists
	}
	topicDirName := sc.getTopicDirectory(topic.Name, topic.ID)
	if err := os.MkdirAll(topicDirName, 0774); err != nil {
		sc.logger.Errorf("Unable to create directory for topic: %s due to err: %s", topic.Name, err.Error())
		return ErrStorageController
	}
	partMap := make(map[int]*Partition)
	for _, elem := range topic.PartitionIDs {
		opts := PartitionOpts{
			TopicName:     sc.generateTopicName(topic.Name, topic.ID),
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
	if err := sc.topicsStore.AddTopic(topic, rLogIdx); err != nil {
		return err
	}
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	sc.topicMap[topic.ID] = entry
	return nil
}

func (sc *StorageController) RemoveTopic(topicID base.TopicIDType, rLogIdx int64) error {
	_, err := sc.GetTopicByID(topicID)
	if err != nil {
		sc.logger.Errorf("Unable to remove topic as topic: %d was not found", topicID)
		return ErrTopicNotFound
	}
	te := sc.removeTopicFromMap(topicID)
	if err = sc.topicsStore.RemoveTopic(topicID, rLogIdx); err != nil {
		return err
	}
	sc.topicDeletionChan <- te
	return nil
}

func (sc *StorageController) GetAllTopics() []base.TopicConfig {
	return sc.topicsStore.GetAllTopics()
}

func (sc *StorageController) GetPartition(topicID base.TopicIDType, partitionID int) (*Partition, error) {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	entry, exists := sc.topicMap[topicID]
	if !exists {
		sc.logger.Errorf("Unable to find topic: %d", topicID)
		return nil, ErrTopicNotFound
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		sc.logger.Errorf("Unable to find partition: %d for topic: %d", partitionID, topicID)
		return nil, ErrPartitionNotFound
	}
	return partition, nil
}

func (sc *StorageController) GetConsumerStore() *ConsumerStore {
	return sc.consumerStore
}

// GetTopicByName returns the topic whose name matches the given topicName.
func (sc *StorageController) GetTopicByName(topicName string) (base.TopicConfig, error) {
	return sc.topicsStore.GetTopicByName(topicName)
}

// GetTopicByID returns the topic based on the given ID.
func (sc *StorageController) GetTopicByID(id base.TopicIDType) (base.TopicConfig, error) {
	return sc.topicsStore.GetTopicByID(id)
}

/*************************************************** Helper methods ***************************************************/

// getControllerRootDirectory returns the root directory for the controller.
func (sc *StorageController) getControllerRootDirectory() string {
	return sc.rootDir
}

// getTopicsRootDirectory returns the root directory of all topics.
func (sc *StorageController) getTopicsRootDirectory() string {
	return path.Join(sc.getControllerRootDirectory(), kTopicsDirName)
}

// getTopicDirectory returns the directory for the given topic.
func (sc *StorageController) getTopicDirectory(topicName string, topicID base.TopicIDType) string {
	dirName := topicName + fmt.Sprintf("-%d", topicID)
	return path.Join(sc.getTopicsRootDirectory(), dirName)
}

// getFileSystemTopics returns the topics that have backing file system directories and files.
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

// generateTopicName generates the topic name based on the given name and id.
func (sc *StorageController) generateTopicName(name string, id base.TopicIDType) string {
	return fmt.Sprintf("%s-%d", name, id)
}

// removeTopicFromMap removes the given topicID from the map.
func (sc *StorageController) removeTopicFromMap(topicID base.TopicIDType) *topicEntry {
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	te, exists := sc.topicMap[topicID]
	if !exists {
		sc.logger.Fatalf("Did not find topic: %d in the map", topicID)
	}
	delete(sc.topicMap, topicID)
	return te
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long-running background goroutine that periodically checks topics that have been marked for removal and
// removes those topics and its associated partitions from the underlying storage.
func (sc *StorageController) janitor() {
	sc.logger.Infof("Starting janitor for storage controller: %s", sc.controllerID)
	for {
		select {
		case topicInfo := <-sc.disposedChan:
			// The topic was disposed.
			sc.logger.Infof("Successfully removed topic: %s from underlying storage", topicInfo)
		case te := <-sc.topicDeletionChan:
			sc.closeTopic(te)
			sc.disposeTopic(te)
		}
	}
}

// closeTopic closes all the partitions that belong to the given topic.
func (sc *StorageController) closeTopic(te *topicEntry) {
	sc.logger.Infof("Closing topic: %s(%d)", te.topic.Name, te.topic.ID)
	for _, prt := range te.partitionMap {
		prt.Close()
	}
}

// createDisposeCb returns a decorated function that can be used as callback when deleting topics.
func (sc *StorageController) createDisposeCb(topicName string, topicID base.TopicIDType) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		sc.logger.Infof("Topic: %s has been successfully disposed", topicName)
		sc.disposedChan <- fmt.Sprintf("%s(%d)", topicName, topicID)
	}
	return cb
}

// disposeZombieTopics removes all topics from the underlying file system that no longer have any records in the
// store.
func (sc *StorageController) disposeZombieTopics(fsDirs map[string]struct{}) {
	for dirPath, _ := range fsDirs {
		cb := func(err error) {
			if err != nil {
				sc.logger.Fatalf("Unable to delete directory: %s due to err: %s", dirPath, err.Error())
			}
			sc.logger.Infof("Successfully deleted zombie topic directory: %s", dirPath)
		}
		sc.disposeDirectory(dirPath, cb)
	}
}

// disposeTopic marks a topic to be deleted.
func (sc *StorageController) disposeTopic(te *topicEntry) {
	oldPath := sc.getTopicDirectory(te.topic.Name, te.topic.ID)
	newPath := path.Join(path.Dir(oldPath), sc.getExpiredDirName(path.Base(oldPath)))
	util.RenameDir(oldPath, newPath)
	sc.disposeDirectory(newPath, sc.createDisposeCb(te.topic.Name, te.topic.ID))
}

// disposeDirectory is a helper method that asks the default disposer to remove the given directory.
func (sc *StorageController) disposeDirectory(dirPath string, cb func(error)) {
	ds := DefaultDisposer()
	ds.Dispose(dirPath, cb)
}

func (sc *StorageController) getExpiredDirName(dirName string) string {
	return fmt.Sprintf("%s%s", dirName, kExpiredTopicsDirSuffix)
}

// topicEntry is a wrapper struct to hold the topic config and the partition(s) of this topic.
type topicEntry struct {
	topic        *base.TopicConfig
	partitionMap map[int]*Partition
}
