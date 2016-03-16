package main

import (
	"flag"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/robbles/kinesiscat/worker"
)

var (
	debug        bool
	region       string
	streamName   string
	position     string
	outputFormat string
	batchSize    int64
	sleepTime    int64
)

func main() {

	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&region, "region", "us-west-1", "AWS region")
	flag.StringVar(&streamName, "stream-name", "events", "Kinesis stream name")
	flag.StringVar(&position, "position", "LATEST", "Position in stream")
	flag.StringVar(&outputFormat, "format", "data", "What to output for each record: sequence, partition-key, or data")
	flag.Int64Var(&batchSize, "batch-size", 1, "How many records to fetch in each call")
	flag.Int64Var(&sleepTime, "sleep-time", 1000, "How long to sleep between calls (ms)")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
		kinesis_worker.Logger.Level = log.DebugLevel
	}

	worker := kinesis_worker.StreamWorker{
		AwsConfig:    &aws.Config{Region: aws.String(region)},
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
