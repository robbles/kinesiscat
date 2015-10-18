package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/robbles/kinesis_worker"
)

var (
	debug        bool
	streamName   string
	position     string
	outputFormat string
	batchSize    int64
	sleepTime    int64
)

func main() {

	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&streamName, "stream-name", "events", "Kinesis stream name")
	flag.StringVar(&position, "position", "LATEST", "Position in stream")
	flag.StringVar(&outputFormat, "format", "data", "What to output for each record: sequence, partition-key, or data")
	flag.Int64Var(&batchSize, "batch-size", 1, "How many records to fetch in each call")
	flag.Int64Var(&sleepTime, "sleep-time", 1000, "How long to sleep between calls (ms)")
	flag.Parse()

	if debug {
		kinesis_worker.SetLogLevel("DEBUG")
	}

	worker := kinesis_worker.StreamWorker{
		Region:       "us-west-1",
		StreamName:   streamName,
		IteratorType: position,
		BatchSize:    batchSize,
		SleepTime:    time.Duration(sleepTime) * time.Millisecond,
	}

	if err := worker.Start(); err != nil {
		log.Panicln(err)
	}

	for record := range worker.Output {
		outputRecord(record, outputFormat)
	}
}

const (
	SEQUENCE      = "sequence"
	PARTITION_KEY = "partition-key"
	DATA          = "data"
)

func outputRecord(record *kinesis.Record, format string) {
	var output *string
	switch format {
	case DATA:
		data := string(record.Data)
		output = &data
	case PARTITION_KEY:
		output = record.PartitionKey
	case SEQUENCE:
		output = record.SequenceNumber
	default:
		output = record.SequenceNumber
	}
	fmt.Println(*output)
}
