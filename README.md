# kinesiscat
Generic command-line consumer for AWS Kinesis, inspired by edenhill/kafkacat

## Installing kinesiscat

```
# set this if you're not using Go >= 1.6
export GO15VENDOREXPERIMENT=1

go get github.com/robbles/kinesiscat
```

## Using kinesiscat

```
export AWS_ACCESS_KEY_ID=XXX
export AWS_SECRET_ACCESS_KEY=YYY

kinesiscat -debug -batch-size=100 -stream-name=my_stream
```

# Tips & Tricks

- Set the debug flag to see batch size and lag behind the current stream data on stderr
- Adjust the batch size until `MillisBehindLatest` in debug output is stable or zero (or just set it higher than your expected throughput)
- Pipe the output through https://stedolan.github.io/jq/ if you're passing JSON in messages:

```
kinesiscat -debug -batch-size=100 -stream-name=my_stream | jq -r '[.key1,.key2] | @csv'
```
