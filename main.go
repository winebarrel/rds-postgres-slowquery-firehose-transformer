package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type Document struct {
	*QueryLog
	Timestamp string `json:"timestamp"`
	LogGroup  string `json:"log_group"`
	LogStream string `json:"log_stream"`
}

func init() {
	log.SetFlags(0)
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
