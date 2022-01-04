package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

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
	queryLog, err := parseQueryLog(data.LogEvents[0])

	if len(data.LogEvents) >= 2 {
		log.Printf("warning: the record contains multiple log events, but the second and subsequent events are ignored (record_id=%s, log_event_count=%d)", record.RecordID, len(data.LogEvents))
	}

	if err != nil {
		log.Printf("failed to parse query log (record_id=%s): %s", record.RecordID, err)
		rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
		return
	}

	if queryLog == nil {
		log.Printf("drop a log event that does not contain a query (record_id=%s)", record.RecordID)
		rr.Result = events.KinesisFirehoseTransformedStateDropped
		return
	}

	doc, err := json.Marshal(&Document{
		QueryLog:  queryLog,
		Timestamp: queryLog.LogTimestamp.Format(time.RFC3339),
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
