package main

import (
	"crypto/md5"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/percona/go-mysql/query"
)

type QueryLog struct {
	LogTimestamp   time.Time `json:"-"`
	RemoteHost     string    `json:"remote_host"`
	RemotePort     string    `json:"remote_port"`
	User           string    `json:"user"`
	Database       string    `json:"database"`
	ProcessId      string    `json:"process_id"`
	ErrorLevel     string    `json:"error_level"`
	Duration       float64   `json:"duration"`
	Statement      string    `json:"-"`
	StatementMD5   string    `json:"statement_md5"`
	StatementLen   int       `json:"statement_len"`
	Fingerprint    string    `json:"fingerprint"`
	FingerprintMD5 string    `json:"fingerprint_md5"`
	FingerprintLen int       `json:"fingerprint_len"`
}

var rePrefix = regexp.MustCompile(`(?s)^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+[^:]+):([^:]*):([^:]*):([^:]*):([^:]*):(.*)`)
var reLog = regexp.MustCompile(`(?s)^\s+duration:\s+(\d+\.\d+)\s+ms\s+(?:statement|execute\s+[^:]+):(.*)`)

func parseQueryLog(logEvent events.CloudwatchLogsLogEvent) (*QueryLog, error) {
	prefixMatches := rePrefix.FindStringSubmatch(logEvent.Message)

	if prefixMatches == nil {
		return nil, nil
	}

	errorLevel := string(prefixMatches[5])

	if errorLevel != "LOG" {
		return nil, nil
	}

	logMatches := reLog.FindStringSubmatch(prefixMatches[6])

	if logMatches == nil {
		return nil, nil
	}

	durationStr := string(logMatches[1])
	duration, err := strconv.ParseFloat(durationStr, 64)

	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	tsOrig := prefixMatches[1]
	timestamp, err := time.Parse("2006-01-02 15:04:05 MST", tsOrig)

	if err != nil {
		log.Printf("failed to parse timestamp (timestamp=%s): %s", tsOrig, err)
		timestamp = time.UnixMilli(logEvent.Timestamp)
	}

	stmt := string(logMatches[2])
	remoteHost := string(prefixMatches[2])
	remotePort := ""
	user := string(prefixMatches[3])
	database := ""

	if strings.Contains(remoteHost, "(") {
		hostPort := strings.SplitN(remoteHost, "(", 2)
		remoteHost = hostPort[0]
		remotePort = strings.TrimRight(hostPort[1], ")")
	}

	if strings.Contains(user, "@") {
		userDatabase := strings.SplitN(user, "@", 2)
		user = userDatabase[0]
		database = userDatabase[1]
	}

	fingerprint := query.Fingerprint(strings.ReplaceAll(stmt, `"`, ""))

	return &QueryLog{
		LogTimestamp:   timestamp,
		RemoteHost:     remoteHost,
		RemotePort:     remotePort,
		User:           user,
		Database:       database,
		ProcessId:      string(prefixMatches[4]),
		ErrorLevel:     string(prefixMatches[5]),
		Duration:       duration,
		Statement:      stmt,
		StatementMD5:   fmt.Sprintf("%x", md5.Sum([]byte(stmt))),
		StatementLen:   len(stmt),
		Fingerprint:    fingerprint,
		FingerprintMD5: fmt.Sprintf("%x", md5.Sum([]byte(fingerprint))),
		FingerprintLen: len(fingerprint),
	}, nil
}
