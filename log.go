package main

import (
	"bufio"
	"encoding/json"
	"io"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Entry struct {
	Severity   string                 `json:"s"`
	System     string                 `json:"c"`
	ID         int                    `json:"id"`
	Context    string                 `json:"ctx"`
	Message    string                 `json:"msg"`
	Attributes map[string]interface{} `json:"attr"`

	T struct {
		Date time.Time `json:"$date"`
	} `json:"t"`
}

func (e *Entry) Log(log *zap.Logger) {
	var severity zapcore.Level
	switch e.Severity {
	case "W":
		severity = zapcore.WarnLevel
	case "E":
		severity = zapcore.ErrorLevel
	}
	if ce := log.Check(severity, e.Message); ce != nil {
		// Here wer ignore time.
		fields := []zapcore.Field{
			zap.String("c", e.System),
			zap.Int("id", e.ID),
			zap.String("ctx", e.Context),
		}
		if len(e.Attributes) > 0 {
			fields = append(fields, zap.Any("attr", e.Attributes))
		}
		ce.Write(fields...)
	}
}

func logProxy(log *zap.Logger) io.Writer {
	r, w := io.Pipe()

	go func() {
		// TODO: Graceful shutdown
		s := bufio.NewScanner(r)
		for s.Scan() {
			var e Entry
			if err := json.Unmarshal(s.Bytes(), &e); err != nil {
				log.Warn("Failed to unmarshal log entry", zap.Error(err))
				continue
			}
			e.Log(log)
		}
	}()

	return w
}
