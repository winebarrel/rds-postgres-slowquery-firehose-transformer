package main

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/percona/go-mysql/query"
)

type QueryLog struct {
	Timestamp      string  `json:"timestamp"`
	RemoteHost     string  `json:"remote_host"`
	RemotePort     string  `json:"remote_port"`
	User           string  `json:"user"`
	Database       string  `json:"database"`
	ProcessId      string  `json:"process_id"`
	ErrorLevel     string  `json:"error_level"`
	Duration       float64 `json:"duration"`
	Statement      string  `json:"-"`
	StatementMD5   string  `json:"statement_md5"`
	Fingerprint    string  `json:"fingerprint"`
	FingerprintMD5 string  `json:"fingerprint_md5"`
}

var rePrefix = regexp.MustCompile(`(?s)^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+[^:]+):([^:]*):([^:]*):([^:]*):([^:]*):(.*)`)
var reLog = regexp.MustCompile(`(?s)^\s+duration:\s+(\d+\.\d+)\s+ms\s+(?:statement|execute\s+[^:]+):(.*)`)

func parseQueryLog(line string) (*QueryLog, error) {
	prefixMatches := rePrefix.FindStringSubmatch(line)

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

	stmt := string(logMatches[2])
	remoteHost := string(prefixMatches[2])
	remotePort := ""
	user := string(prefixMatches[3])
	database := ""

	if strings.Contains(remotePort, "(") {
		hostPort := strings.SplitN(remotePort, "(", 2)
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
		Timestamp:      prefixMatches[1],
		RemoteHost:     remoteHost,
		RemotePort:     remotePort,
		User:           user,
		Database:       database,
		ProcessId:      string(prefixMatches[4]),
		ErrorLevel:     string(prefixMatches[5]),
		Duration:       duration,
		Statement:      stmt,
		StatementMD5:   fmt.Sprintf("%x", md5.Sum([]byte(stmt))),
		Fingerprint:    fingerprint,
		FingerprintMD5: fmt.Sprintf("%x", md5.Sum([]byte(fingerprint))),
	}, nil
}
