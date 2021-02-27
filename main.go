package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type templateContext struct {
	Path string
	IP   string
	Port int
}

func ensureDir(name string) error {
	if err := os.MkdirAll(name, 0700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	return nil
}

func ensureTempDir(name string) (context.CancelFunc, error) {
	if err := ensureDir(name); err != nil {
		return nil, fmt.Errorf("ensure: %w", err)
	}

	return func() {
		_ = os.RemoveAll(name)
	}, nil
}

//go:embed _templates
var templates embed.FS

func executeTo(t *template.Template, name string, data interface{}) error {
	buf := new(bytes.Buffer)
	if err := t.Execute(buf, data); err != nil {
		return fmt.Errorf("template: %w", err)
	}

	if err := os.WriteFile(name, buf.Bytes(), 0600); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

func run(ctx context.Context, log *zap.Logger) error {
	mongod := flag.String("bin", "/usr/bin/mongod", "path for mongod")
	baseDir := flag.String("dir", "", "dir to use for data (default to current dir)")
	flag.Parse()

	t, err := template.ParseFS(templates, "_templates/*.tpl")
	if err != nil {
		return fmt.Errorf("parse templates: %w", err)
	}

	instanceDir := filepath.Join(*baseDir, "db1")
	dirCleanup, err := ensureTempDir(instanceDir)
	if err != nil {
		return fmt.Errorf("ensure dir: %w", err)
	}
	defer dirCleanup()

	dataDir := filepath.Join(instanceDir, "data")
	if err := ensureDir(dataDir); err != nil {
		return fmt.Errorf("ensure data dir: %w", err)
	}

	cfgPath := filepath.Join(instanceDir, "mongod.conf")
	cfg := templateContext{
		Path: "data",
		IP:   "127.0.0.1",
		Port: 28001,
	}
	if err := executeTo(t, cfgPath, cfg); err != nil {
		return fmt.Errorf("template: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	cmd := exec.CommandContext(gCtx, *mongod, "--config", "mongod.conf")
	cmd.Stderr = os.Stderr
	logReader, logFlush := logProxy(log.Named("mongo"), g)

	cmd.Stdout = logReader
	cmd.Dir = instanceDir

	log.Info("Starting", zap.String("dir", instanceDir))
	g.Go(func() error {
		defer logFlush()
		return cmd.Run()
	})
	g.Go(func() error {
		ctx, cancel := context.WithTimeout(gCtx, time.Second*10)
		defer cancel()

		uri := fmt.Sprintf("mongodb://%s:%d", cfg.IP, cfg.Port)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}

		defer func() {
			_ = client.Disconnect(gCtx)
			log.Info("Disconnected")
		}()

		b := backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond*50), 50)

		start := time.Now()
		if err := backoff.Retry(func() error {
			if err := client.Ping(ctx, nil); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return backoff.Permanent(err)
				}
				return err
			}
			return nil
		}, b); err != nil {
			return fmt.Errorf("ping: %w", err)
		}

		log.Info("Connected", zap.Duration("duration", time.Since(start)))

		return nil
	})

	return g.Wait()
}

func main() {
	encoderCfg := zap.NewDevelopmentEncoderConfig()

	start := time.Now()
	encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderCfg.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("%07.03f", time.Since(start).Seconds()))
	}

	cfg := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:       true,
		Encoding:          "console",
		EncoderConfig:     encoderCfg,
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     true,
		DisableStacktrace: true,
	}

	log, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx, log); err != nil {
		select {
		case <-ctx.Done():
			log.Info("Done")
		default:
			log.Panic("Run failed", zap.Error(err))
		}
	}
}
