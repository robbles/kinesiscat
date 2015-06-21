package main

import (
	"flag"
	"fmt"
	"time"

	logger "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	stream_name   string
	position      string
	output_format string
	batch_size    int64
	sleep_time    int64
)

func main() {

	flag.StringVar(&stream_name, "stream-name", "events", "Kinesis stream name")
	flag.StringVar(&position, "position", "LATEST", "Position in stream")
	flag.StringVar(&output_format, "format", "data", "What to output for each record: sequence, partition_key, or data")
	flag.Int64Var(&batch_size, "batch_size", 1, "How many records to fetch in each call")
	flag.Int64Var(&sleep_time, "sleep_time", 1000, "How long to sleep between calls (ms)")
	flag.Parse()

	client := kinesis.New(&aws.Config{Region: "us-west-1"})

	stream_res, err := client.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &stream_name})
	if err != nil {
		panic(err)
	}

	logger.WithFields(logger.Fields{
		"name":       *stream_res.StreamDescription.StreamName,
		"arn":        *stream_res.StreamDescription.StreamARN,
		"num_shards": len(stream_res.StreamDescription.Shards),
	}).Info("Found Stream")

	for _, shard := range stream_res.StreamDescription.Shards {
		logger.WithFields(logger.Fields{
			"id": *shard.ShardID,
		}).Info("Found Shard")
	}

	// Make queue for multiple shard workers to pass back results on
	queue := make(chan *kinesis.Record)

	// TODO: use all shards or allow specifying a shard ID
	shard := stream_res.StreamDescription.Shards[0]
	go fetchRecords(queue, client, shard)

	for record := range queue {
		outputRecord(record, output_format)
	}
}

func fetchRecords(queue chan *kinesis.Record, client *kinesis.Kinesis, shard *kinesis.Shard) {
	iter_res, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        &stream_name,
		ShardID:           shard.ShardID,
		ShardIteratorType: &position,
	})

	if err != nil {
		panic(err)
	}

	iterator := iter_res.ShardIterator

	logger.WithFields(logger.Fields{
		"iterator": *iter_res.ShardIterator,
	}).Info("Got shard iterator")

	for {
		records_res, err := client.GetRecords(&kinesis.GetRecordsInput{ShardIterator: iterator, Limit: &batch_size})

		if err != nil {
			logger.WithFields(logger.Fields{"err": err}).Warn("Error fetching records")
			time.Sleep(time.Duration(sleep_time) * time.Millisecond)
			continue
		}

		logger.WithFields(logger.Fields{
			"records": len(records_res.Records),
			"lag_ms":  *records_res.MillisBehindLatest,
		}).Info("Fetched records")

		for _, record := range records_res.Records {
			queue <- record
		}

		iterator = records_res.NextShardIterator
		time.Sleep(time.Duration(sleep_time) * time.Millisecond)
	}
}

const (
	SEQUENCE      = "sequence"
	PARTITION_KEY = "partition_key"
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
