package client

import (
	"eeylops/server/base"
	"github.com/cenkalti/backoff"
	"time"
)

// createBackoffFn is a helper function that returns a wrapped function which waits using exponential back offs.
func createBackoffFn() func(int) {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     time.Millisecond,
		RandomizationFactor: 0.2,
		Multiplier:          1.5,
		MaxInterval:         time.Second * 1,
		MaxElapsedTime:      0, // Never stop the timer.
		Clock:               backoff.SystemClock,
	}
	bfn := func(attempt int) {
		time.Sleep(b.NextBackOff())
	}
	return bfn
}

// isPartitionPresentInTopicConfig is a helper function that checks if the partitionID is present in the topic
// config.
func isPartitionPresentInTopicConfig(topicCfg base.TopicConfig, partitionID int) (found bool) {
	found = false
	for _, prtID := range topicCfg.PartitionIDs {
		if partitionID == prtID {
			found = true
			return
		}
	}
	return
}
