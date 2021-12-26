package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type Document struct {
	*QueryLog
	LogGroup  string `json:"log_group"`
	LogStream string `json:"log_stream"`
}

func decompressCloudwatchLogsData(data []byte) (d events.CloudwatchLogsData, err error) {
	zr, err := gzip.NewReader(bytes.NewBuffer(data))

	if err != nil {
		return
	}

	defer zr.Close()

	dec := json.NewDecoder(zr)
	err = dec.Decode(&d)

	return
}

func processRecord(record *events.KinesisFirehoseEventRecord) (rr events.KinesisFirehoseResponseRecord) {
	rr.RecordID = record.RecordID
	data, err := decompressCloudwatchLogsData(record.Data)

	if err != nil {
		log.Printf("failed to decompress CloudwatchLogsData (record_id=%s): %s", record.RecordID, err)
		rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
		return
	}

	if data.MessageType == "CONTROL_MESSAGE" {
		log.Printf("drop CONTROL_MESSAGE (record_id=%s)", record.RecordID)
		rr.Result = events.KinesisFirehoseTransformedStateDropped
		return
	} else if data.MessageType != "DATA_MESSAGE" {
		log.Printf("unknown message type (record_id=%s): %s", record.RecordID, data.MessageType)
		rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
		return
	}

	if len(data.LogEvents) == 0 {
		log.Printf("drop a record that do not contain log events (record_id=%s)", record.RecordID)
		rr.Result = events.KinesisFirehoseTransformedStateDropped
		return
	}

	// NOTE: Cannot handle multiple log events
	queryLog, err := parseQueryLog(data.LogEvents[0].Message)

	if err != nil {
		log.Printf("failed to parse query log (record_id=%s): %s", record.RecordID, err)
		rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
		return
	}

	if queryLog == nil {
		log.Printf("drop a log event that does not contain a query (record_id=%s): %s", record.RecordID, err)
		rr.Result = events.KinesisFirehoseTransformedStateDropped
		return
	}

	doc, err := json.Marshal(&Document{
		QueryLog:  queryLog,
		LogGroup:  data.LogGroup,
		LogStream: data.LogStream,
	})

	if err != nil {
		log.Printf("failed to marshal document (record_id=%s): %s", record.RecordID, err)
		rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
		return
	}

	rr.Result = events.KinesisFirehoseTransformedStateOk
	rr.Data = doc

	return
}

func countResults(records []events.KinesisFirehoseResponseRecord) (ok, dropped, failed int) {
	for _, r := range records {
		switch r.Result {
		case events.KinesisFirehoseTransformedStateOk:
			ok++
		case events.KinesisFirehoseTransformedStateDropped:
			dropped++
		case events.KinesisFirehoseTransformedStateProcessingFailed:
			failed++
		default:
			log.Fatalf("unknown result (record_id=%s): %s", r.RecordID, r.Result)
		}
	}

	return
}

func handleRequest(ctx context.Context, event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	log.Printf("start handling requests: records=%d", len(event.Records))

	records := make([]events.KinesisFirehoseResponseRecord, 0, len(event.Records))

	for _, r := range event.Records {
		rr := processRecord(&r)
		records = append(records, rr)
	}

	ok, dropped, failed := countResults(records)
	log.Printf("finish handling requests: ok=%d dropped=%d failed=%d", ok, dropped, failed)

	return events.KinesisFirehoseResponse{Records: records}, nil
}

func main() {
	lambda.Start(handleRequest)
}
