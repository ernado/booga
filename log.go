package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
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
	case "E", "F":
		// We can't use Fatal level because this will call os.Exit.
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

func logProxy(log *zap.Logger, g *errgroup.Group) (io.Writer, context.CancelFunc) {
	r, w := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())

	g.Go(func() error {
		<-ctx.Done()
		return r.Close()
	})
	g.Go(func() error {
		s := bufio.NewScanner(r)
		log.Info("Log streaming started")
		defer log.Info("Log streaming ended")
		for s.Scan() {
			var e Entry
			if err := json.Unmarshal(s.Bytes(), &e); err != nil {
				log.Warn("Failed to unmarshal log entry", zap.Error(err))
				continue
			}
			e.Log(log)
		}
		return s.Err()
	})

	return w, cancel
}
