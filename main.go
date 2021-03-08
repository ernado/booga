package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func run(ctx context.Context, log *zap.Logger) error {
	var (
		mongod   = flag.String("mongod", "/usr/bin/mongod", "path for mongod")
		mongos   = flag.String("mongos", "/usr/bin/mongos", "path for mongos")
		shards   = flag.Int("shards", 2, "count of shards")
		replicas = flag.Int("replicas", 3, "count of replicas")
		baseDir  = flag.String("dir", "", "dir to use for data (default to current dir)")
		httpAddr = flag.String("http", ":8080", "http addr to listen for commands")

		users = flag.Int("users", 1000, "count of users")
		faces = flag.Int("faces", 30, "count of faces per user")
		files = flag.Int("files", 1000, "count of files")
	)
	flag.Parse()

	return NewCluster(ClusterConfig{
		Log:      log,
		Mongod:   *mongod,
		Mongos:   *mongos,
		Dir:      filepath.Join(*baseDir, "cloud_data"),
		DB:       "cloud",
		Replicas: *replicas,
		Shards:   *shards,
		Addr:     *httpAddr,

		OnSetup: fixture{
			log:   log,
			users: *users,
			faces: *faces,
			files: *files,
		}.Load,
	}).Run(ctx)
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
