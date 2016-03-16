## Kinesis Worker

A worker implementation for reading from Kinesis efficiently. Start a
StreamWorker with a given StreamName, and a worker goroutine will be started
automatically for each shard.

Example usage:

```go
stream := kinesis_worker.StreamWorker{
    Region:       "us-west-1",
    StreamName:   "events",
    IteratorType: "LATEST",
    BatchSize:    100,
    SleepTime:    time.Second * 1,
}

err := stream.Start()
if err {
    // one or more workers failed to start, check configuration
}

for record := range stream.Output {
    // handle kinesis_worker.Record (*kinesis.Record)
}
```
