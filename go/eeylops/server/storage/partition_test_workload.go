package storage

import (
	"context"
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"github.com/golang/glog"
	"math/rand"
	"time"
)

type partitionStressWorkload struct {
	partitionMap             map[int]*Partition
	topicName                string
	numConsumersPerPartition int
	doneChan                 chan struct{}
	allJobsDoneChan          chan struct{}
	dataSizeBytes            int
	batchSize                int
	consumerDelayMs          time.Duration // Consumer delay in ms.
}

func NewPartitionStressWorkload(partitionMap map[int]*Partition, numConsumerPerPartition int, payloadSizeBytes int,
	batchSize int, consumerDelayMs int) *partitionStressWorkload {
	psw := new(partitionStressWorkload)
	psw.doneChan = make(chan struct{})
	psw.allJobsDoneChan = make(chan struct{}, 256)
	psw.dataSizeBytes = payloadSizeBytes
	psw.partitionMap = partitionMap
	psw.numConsumersPerPartition = numConsumerPerPartition
	psw.batchSize = batchSize
	psw.consumerDelayMs = time.Duration(consumerDelayMs) * time.Millisecond
	return psw
}

func (psw *partitionStressWorkload) Start() {
	glog.Infof("Starting workloads")
	for key, _ := range psw.partitionMap {
		go psw.producer(key)
		for ii := 0; ii < psw.numConsumersPerPartition; ii++ {
			go psw.consumer(key, ii)
		}
	}
}

func (psw *partitionStressWorkload) Stop() {
	glog.Infof("Stopping workloads")
	close(psw.doneChan)
	totalExpected := len(psw.partitionMap) * (psw.numConsumersPerPartition + 1)
	for ii := 0; ii < totalExpected; ii++ {
		<-psw.allJobsDoneChan
	}
	glog.Infof("All workers have finished")
}

func (psw *partitionStressWorkload) producer(partitionID int) {
	token := make([]byte, psw.dataSizeBytes)
	rand.Read(token)
	var entries [][]byte
	for ii := 0; ii < psw.batchSize; ii++ {
		entries = append(entries, token)
	}
	start := time.Now()
	count := -1
	for {
		count++
		select {
		case <-psw.doneChan:
			elapsed := time.Since(start)
			avgTimePerBatch := elapsed / time.Duration(count)
			avgTimePerMsg := avgTimePerBatch / time.Duration(psw.batchSize)
			glog.Infof("Producer for partition: %d exiting. "+
				"\nTotal Time: %v"+
				"\nAverage time per batch: %v "+
				"\nAverage time per msg: %v",
				partitionID, elapsed, avgTimePerBatch, avgTimePerMsg)
			psw.allJobsDoneChan <- struct{}{}
			return
		default:
			now := time.Now().UnixNano()
			arg := storagebase.AppendEntriesArg{
				Entries:   entries,
				Timestamp: now,
				RLogIdx:   now,
			}
			p := psw.partitionMap[partitionID]
			ret := p.Append(context.Background(), &arg)
			if ret.Error != nil {
				glog.Fatalf("Unexpected error while appending to partition: %d", partitionID)
			}
		}
	}
}

func (psw *partitionStressWorkload) consumer(partitionID int, consumerID int) {
	for {
		time.Sleep(psw.consumerDelayMs)
		scanner := NewPartitionScanner(psw.partitionMap[partitionID], 0)
		count := 0
		glog.Infof("Restarting scanner. Consumer ID: %d, Partition ID: %d", consumerID, partitionID)
		for scanner.Rewind(); scanner.Valid(); scanner.Next() {
			item, err := scanner.Get()
			if err != nil {
				glog.Fatalf("Unexpected error from consumer: %d while scanning partition: %d", consumerID,
					partitionID)
			}
			if item.Offset != base.Offset(count) {
				glog.Fatalf("Wrong message got from partition. Expected: %d, got: %d", count, item.Offset)
			}
			if count%5000 == 0 {
				glog.Infof("Comsumer ID: %d, Partition ID: %d, Scanned up to offset: %d",
					consumerID, partitionID, count)
			}
			count++
		}
		select {
		case <-psw.doneChan:
			glog.Infof("Consumer: %d, partition: %d exiting", consumerID, partitionID)
			psw.allJobsDoneChan <- struct{}{}
			return
		default:
			// Do nothing
		}
	}
}
