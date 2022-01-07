package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

type Document struct {
	*QueryLog
	Timestamp  string `json:"timestamp"`
	LogGroup   string `json:"log_group"`
	LogStream  string `json:"log_stream"`
	Identifier string `json:"identifier"`
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

func processRecord(record *events.KinesisFirehoseEventRecord, esIndexPrefix string) (rr events.KinesisFirehoseResponseRecord) {
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

	var docs bytes.Buffer

	for i, queryLog := range queryLogs {
		doc, err := json.Marshal(&Document{
			QueryLog:   queryLog,
			Timestamp:  queryLog.LogTimestamp.Format(time.RFC3339),
			LogGroup:   data.LogGroup,
			LogStream:  data.LogStream,
			Identifier: strings.Split(data.LogStream, "/")[4],
		})

		if err != nil {
			log.Printf("failed to marshal document (record_id=%s): %s", record.RecordID, err)
			rr.Result = events.KinesisFirehoseTransformedStateProcessingFailed
			return
		}

		// https://stackoverflow.com/questions/69027268/aws-firehose-to-elastic-search-transforming-one-firehose-record-into-multiple
		if i > 0 {
			docs.WriteString("\n")
			index := fmt.Sprintf(`{"index":{"_index":"%s-%s"}}`, esIndexPrefix, queryLog.LogTimestamp.Format("2006-01-02"))
			docs.WriteString(index)
			docs.WriteString("\n")
		}

		docs.Write(doc)
	}

	rr.Result = events.KinesisFirehoseTransformedStateOk
	rr.Data = docs.Bytes()

	return
}
