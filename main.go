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
	"golang.org/x/xerrors"
)

type templateContext struct {
	Path string
	IP   string
	Port int
}

func ensureDir(name string) error {
	if err := os.MkdirAll(name, 0700); err != nil {
		return xerrors.Errorf("mkdir: %w", err)
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
		return xerrors.Errorf("template: %w", err)
	}

	if err := os.WriteFile(name, buf.Bytes(), 0600); err != nil {
		return xerrors.Errorf("write: %w", err)
	}

	return nil
}

// ensureServer ensures that mongo server is up on given uri.
func ensureServer(ctx context.Context, log *zap.Logger, uri string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return xerrors.Errorf("connect: %w", err)
	}

	defer func() {
		_ = client.Disconnect(ctx)
		log.Info("Disconnected")
	}()

	b := backoff.NewConstantBackOff(time.Millisecond * 50)

	start := time.Now()
	if err := backoff.Retry(func() error {
		if err := client.Ping(ctx, nil); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return backoff.Permanent(err)
			}
			return err
		}
		return nil
	}, backoff.WithContext(b, ctx)); err != nil {
		return xerrors.Errorf("ping: %w", err)
	}

	log.Info("Connected", zap.Duration("duration", time.Since(start)))

	return nil
}

// Options for running mongo.
type Options struct {
	BinaryPath     string
	BaseDir        string
	Name           string
	ConfigTemplate *template.Template

	IP   string
	Port int
}

// runServer runs mongo server with provided options until error or context
// cancellation.
func runServer(ctx context.Context, log *zap.Logger, opt Options) error {
	log = log.Named(opt.Name)

	// Ensuring instance directory.
	dir := filepath.Join(opt.BaseDir, opt.Name)
	dirCleanup, err := ensureTempDir(dir)
	if err != nil {
		return xerrors.Errorf("ensure dir: %w", err)
	}
	// Directory will be removed recursively on cleanup.
	defer dirCleanup()

	// Ensuring data directory.
	const dataDir = "data"
	dataPath := filepath.Join(dir, dataDir)
	if err := ensureDir(dataPath); err != nil {
		return xerrors.Errorf("ensure data dir: %w", err)
	}

	// Rendering configuration file for instance.
	const cfgName = "mongo.conf"
	cfgPath := filepath.Join(dir, cfgName)
	cfg := templateContext{
		Path: dataDir,
		IP:   opt.IP,
		Port: opt.Port,
	}
	if err := executeTo(opt.ConfigTemplate, cfgPath, cfg); err != nil {
		return xerrors.Errorf("template: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	log.Info("Starting", zap.String("dir", dir))
	g.Go(func() error {
		// Piping mongo logs to zap logger.
		logReader, logFlush := logProxy(log.Named("mongo"), g)
		defer logFlush()

		cmd := exec.CommandContext(gCtx, opt.BinaryPath, "--config", cfgName)
		cmd.Stdout = logReader
		cmd.Dir = dir

		return cmd.Run()
	})
	g.Go(func() error {
		// Waiting up to 10 seconds for mongo server to be available.
		ctx, cancel := context.WithTimeout(gCtx, time.Second*10)
		defer cancel()

		uri := fmt.Sprintf("mongodb://%s:%d", cfg.IP, cfg.Port)
		if err := ensureServer(ctx, log, uri); err != nil {
			return xerrors.Errorf("ensure server: %w", err)
		}

		return nil
	})

	return g.Wait()
}

func run(ctx context.Context, log *zap.Logger) error {
	mongod := flag.String("bin", "/usr/bin/mongod", "path for mongod")
	baseDir := flag.String("dir", "", "dir to use for data (default to current dir)")
	flag.Parse()

	t, err := template.ParseFS(templates, "_templates/*.tpl")
	if err != nil {
		return xerrors.Errorf("parse templates: %w", err)
	}

	// Running single instance until ctrl+C.
	if err := runServer(ctx, log, Options{
		Name:           "db1",
		BaseDir:        *baseDir,
		BinaryPath:     *mongod,
		ConfigTemplate: t,

		IP:   "127.0.0.1",
		Port: 28013,
	}); err != nil {
		return xerrors.Errorf("run: %w", err)
	}

	return nil
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
