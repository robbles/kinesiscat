package kinesis_worker

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	ShardIteratorTypeAtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	ShardIteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	ShardIteratorTypeTrimHorizon         = "TRIM_HORIZON"
	ShardIteratorTypeLatest              = "LATEST"
)

const (
	DefaultBatchSize = 10
	DefaultSleepTime = time.Second
	DefaultRegion    = "us-west-1"
)

var Logger *log.Logger = log.New()

// Use custom types to allow wrapping later if required
type Client kinesisiface.KinesisAPI
type Record *kinesis.Record

type Worker interface {
	Start() error
	Stop()
}

// Manages a single Kinesis stream and a pool of workers, one for each shard.
type StreamWorker struct {
	AwsConfig              *aws.Config
	StreamName             string
	IteratorType           string
	StartingSequenceNumber *string
	BatchSize              int64
	SleepTime              time.Duration
	Client                 Client
	workers                []Worker

	// Blocking channel of kinesis Records
	Output chan Record

	// Non-blocking channel of state updates
	State chan WorkerState
}

// Set defaults for all fields, initialize channel and client if not provided
func (stream *StreamWorker) initialize() error {
	if stream.IteratorType == "" {
		stream.IteratorType = ShardIteratorTypeLatest
	}

	if stream.BatchSize == 0 {
		stream.BatchSize = DefaultBatchSize
	}

	if stream.SleepTime == 0 {
		stream.SleepTime = DefaultSleepTime
	}

	if stream.Output == nil {
		stream.Output = make(chan Record)
	}

	if stream.State == nil {
		stream.State = make(chan WorkerState)
	}

	if stream.Client == nil {
		stream.Client = kinesis.New(session.New(), stream.AwsConfig)
	}

	Logger.WithFields(log.Fields{
		"StreamWorker": stream,
		"AWS":          stream.AwsConfig,
	}).Debug("StreamWorker initialized")

	return nil
}

func (stream *StreamWorker) Start() error {
	if err := stream.initialize(); err != nil {
		return err
	}

	// Get list of shards for the stream
	stream_res, err := stream.Client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &stream.StreamName,
	})
	if err != nil {
		return err
	}

	// Create one worker for each shard in the stream
	numWorkers := len(stream_res.StreamDescription.Shards)
	stream.workers = make([]Worker, numWorkers)

	for i, shard := range stream_res.StreamDescription.Shards {
		worker, err := NewShardWorker(stream, shard)
		if err != nil {
			return err
		}
		stream.workers[i] = worker
	}

	Logger.WithFields(log.Fields{
		"StreamName": stream.StreamName,
		"Shards":     len(stream.workers),
		"Workers":    len(stream.workers),
	}).Debug("StreamWorker starting")

	// Worker setup was successful, now start them all
	for _, worker := range stream.workers {
		worker.Start()
	}

	return nil
}

func (stream *StreamWorker) Stop() {
	for _, worker := range stream.workers {
		worker.Stop()
	}
}

// Retrieves records from a single shard and sends them on a channel
type ShardWorker struct {
	Stream        *StreamWorker
	Shard         *kinesis.Shard
	ShardId       string
	ShardIterator string
	done          chan bool
}

type WorkerState struct {
	ShardId string
	Lag     int64
}

func NewShardWorker(stream *StreamWorker, shard *kinesis.Shard) (*ShardWorker, error) {
	iter_res, err := stream.Client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             &stream.StreamName,
		ShardId:                shard.ShardId,
		ShardIteratorType:      &stream.IteratorType,
		StartingSequenceNumber: stream.StartingSequenceNumber,
	})

	if err != nil {
		return nil, err
	}

	worker := ShardWorker{
		Stream:        stream,
		Shard:         shard,
		ShardId:       *shard.ShardId,
		ShardIterator: *iter_res.ShardIterator,
		done:          make(chan bool),
	}

	return &worker, nil
}

func (w *ShardWorker) Start() error {
	go w.run()
	return nil
}

func (w *ShardWorker) Stop() {
	w.done <- true
}

func (w *ShardWorker) run() {
	delayTimer := time.NewTicker(w.Stream.SleepTime)

	Logger.WithFields(log.Fields{
		"ShardId":       w.ShardId,
		"ShardIterator": w.ShardIterator,
	}).Debug("ShardWorker starting")

	for {
		w.step()

		select {

		case <-delayTimer.C:
			// Minimum delay has elapsed, proceed with next iteration
			continue

		case <-w.done:
			Logger.WithFields(log.Fields{
				"ShardId": w.ShardId,
			}).Debug("ShardWorker finishing")

			// Received shutdown message from StreamWorker, finish
			return

		}
	}
}

// Fetch one batch of records and send them to the output channel
func (w *ShardWorker) step() {
	records_res, err := w.Stream.Client.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: &w.ShardIterator,
		Limit:         &w.Stream.BatchSize,
	})

	if err != nil {
		Logger.WithFields(log.Fields{
			"Error":         err,
			"ShardIterator": w.ShardIterator,
		}).Error("GetRecords API call failed")

		// Wait until next iteration or exit
		return
	}

	lag := *records_res.MillisBehindLatest
	w.updateState(WorkerState{ShardId: w.ShardId, Lag: lag})

	Logger.WithFields(log.Fields{
		"MillisBehindLatest": lag,
		"NumRecords":         len(records_res.Records),
		"ShardIterator":      w.ShardIterator,
		"NextShardIterator":  *records_res.NextShardIterator,
	}).Debug("Successfully fetched records")

	// Add each record in the result to the StreamWorker's output channel
	for _, record := range records_res.Records {
		w.Stream.Output <- record
	}

	// Use the new ShardIterator returned in the response for the next request
	w.ShardIterator = *records_res.NextShardIterator
}

func (w *ShardWorker) updateState(state WorkerState) {
	// Attempt to update state with a non-blocking send on State channel
	select {
	case w.Stream.State <- state:
	default:
	}
}
