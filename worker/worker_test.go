package kinesis_worker

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetup(t *testing.T) {
}

func TestStreamWorkerInitialize(t *testing.T) {
	stream := StreamWorker{}
	err := stream.initialize()

	assert.Nil(t, err, "Initialization of stream should succeed")
	assert.Nil(t, stream.AwsConfig, "Default AWS config should be nil unless provided")
	assert.Equal(t, "", stream.StreamName, "StreamName should default to empty")
	assert.Equal(t, ShardIteratorTypeLatest, stream.IteratorType, "IteratorType should default to LATEST")
	assert.Nil(t, stream.StartingSequenceNumber, "StartingSequenceNumber should default to nil")
	assert.Equal(t, DefaultSleepTime, stream.SleepTime, "SleepTime should default to DefaultSleepTime")
	assert.Equal(t, int64(DefaultBatchSize), stream.BatchSize, "BatchSize should default to DefaultBatchSize")
	assert.NotNil(t, stream.Client, "Client should be initialized if not set")
	assert.NotNil(t, stream.Output, "Output should be initialized if not set")
	assert.NotNil(t, stream.State, "State should be initialized if not set")
	assert.Nil(t, stream.workers, "Slice of workers should still be nil at this stage")
}

// TODO: test initialization of ShardWorker with a mocked client

// TODO: test behaviour of ShardWorker.run with a mocked client

// TODO: test behaviour of ShardWorker.step with a mocked client
