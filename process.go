package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"log"
	"sort"
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

	queryLogs := []*QueryLog{}

	for i, logEvent := range data.LogEvents {
		queryLog, err := parseQueryLog(logEvent)

		if err != nil {
			log.Printf("failed to parse query log (record_id=%s, index=%d): %s", record.RecordID, i, err)
		}

		if queryLog != nil {
			queryLogs = append(queryLogs, queryLog)
		}
	}

	if len(queryLogs) == 0 {
		log.Printf("drop a log event that does not contain a query (record_id=%s)", record.RecordID)
		rr.Result = events.KinesisFirehoseTransformedStateDropped
		return
	}

	// NOTE: Cannot handle multiple log events.
	if len(queryLogs) >= 2 {
		log.Printf("warning: some log events are ignored (record_id=%s, log_events_count=%d)", record.RecordID, len(queryLogs))
	}

	sort.Slice(queryLogs, func(i, j int) bool { return queryLogs[i].Duration > queryLogs[j].Duration })
	queryLog := queryLogs[0]

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
