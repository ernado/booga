package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

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

// ensureServer ensures that mongo server is up on given uri.
func ensureServer(ctx context.Context, log *zap.Logger, client *mongo.Client) error {
	b := backoff.NewConstantBackOff(time.Millisecond * 100)

	start := time.Now()
	if err := backoff.Retry(func() error {
		// Set separate timeout for single try.
		pingCtx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
		defer cancel()

		if err := client.Ping(pingCtx, nil); err != nil {
			select {
			case <-ctx.Done():
				return backoff.Permanent(ctx.Err())
			default:
				return err
			}
		}

		return nil
	}, backoff.WithContext(b, ctx)); err != nil {
		return xerrors.Errorf("ping: %w", err)
	}

	log.Info("Connected",
		zap.Duration("d", time.Since(start)),
	)

	return nil
}

// Options for running mongo.
type Options struct {
	BinaryPath       string
	BaseDir          string
	Name             string
	ReplicaSet       string
	ConfigServer     bool
	ShardServer      bool
	RoutingServer    bool
	ConfigServerAddr string

	OnReady func(ctx context.Context, client *mongo.Client) error

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

	g, gCtx := errgroup.WithContext(ctx)

	log.Info("Starting", zap.String("dir", dir))
	g.Go(func() error {
		// Piping mongo logs to zap logger.
		logReader, logFlush := logProxy(log.Named("mongo"), g)
		defer logFlush()

		args := []string{
			"--bind_ip", opt.IP,
			"--port", strconv.Itoa(opt.Port),
		}

		if opt.ConfigServer {
			args = append(args, "--configsvr")
		}
		if opt.ShardServer {
			args = append(args, "--shardsvr")
		}
		if opt.RoutingServer {
			args = append(args, "--configdb", opt.ConfigServerAddr)
		} else {
			args = append(args, "--replSet", opt.ReplicaSet, "--dbpath", dataDir)
		}

		cmd := exec.CommandContext(gCtx, opt.BinaryPath, args...)
		cmd.Stdout = logReader
		cmd.Stderr = logReader
		cmd.Dir = dir

		return cmd.Run()
	})
	g.Go(func() error {
		// Waiting up to 10 seconds for mongo server to be available.
		ctx, cancel := context.WithTimeout(gCtx, time.Second*5)
		defer cancel()

		uri := &url.URL{
			Scheme: "mongodb",
			Host:   net.JoinHostPort(opt.IP, strconv.Itoa(opt.Port)),
			Path:   "/",
		}

		client, err := mongo.Connect(ctx, options.Client().
			ApplyURI(uri.String()).
			// SetDirect is important, client can timeout otherwise.
			SetDirect(true),
		)
		if err != nil {
			return xerrors.Errorf("connect: %w", err)
		}

		defer func() {
			_ = client.Disconnect(ctx)
			log.Info("Disconnected")
		}()

		if err := ensureServer(ctx, log, client); err != nil {
			return xerrors.Errorf("ensure server: %w", err)
		}

		if err := opt.OnReady(ctx, client); err != nil {
			return xerrors.Errorf("onReady: %w", err)
		}

		return nil
	})

	return g.Wait()
}

type ClusterConfig struct {
	Mongod string // mongod binary path
	Mongos string // mongos binary path

	Dir string // base directory
	DB  string // database name

	// OnSetup is called
	OnSetup func(ctx context.Context, client *mongo.Client) error
}

func ensureCluster(ctx context.Context, log *zap.Logger, cfg ClusterConfig) error {
	g, gCtx := errgroup.WithContext(ctx)
	replicaSetInitialized := make(chan struct{})

	const (
		rsData   = "rsData"
		rsConfig = "rsConfig"
	)

	// Configuration servers.
	g.Go(func() error {
		return runServer(gCtx, log, Options{
			Name:         "cfg",
			BaseDir:      cfg.Dir,
			BinaryPath:   cfg.Mongod,
			ReplicaSet:   rsConfig,
			ConfigServer: true,
			OnReady: func(ctx context.Context, client *mongo.Client) error {
				// Initializing config replica set.
				rsConfig := bson.M{
					"_id": rsConfig,
					"members": []bson.M{
						{"_id": 0, "host": "127.0.0.1:28001"},
					},
				}
				if err := client.Database("admin").
					RunCommand(ctx, bson.M{"replSetInitiate": rsConfig}).
					Err(); err != nil {
					return xerrors.Errorf("replSetInitiate: %w", err)
				}

				log.Info("Config replica set initialized")
				close(replicaSetInitialized)

				return nil
			},

			IP:   "127.0.0.1",
			Port: 28001,
		})
	})

	// Data servers.
	g.Go(func() error {
		select {
		case <-replicaSetInitialized:
		case <-gCtx.Done():
			return gCtx.Err()
		}

		return runServer(gCtx, log, Options{
			Name:        "shard1",
			BaseDir:     cfg.Dir,
			BinaryPath:  cfg.Mongod,
			ReplicaSet:  rsData,
			ShardServer: true,

			OnReady: func(ctx context.Context, client *mongo.Client) error {
				// Initializing replica set.
				rsConfig := bson.M{
					"_id": rsData,
					"members": []bson.M{
						{"_id": 0, "host": "127.0.0.1:29001"},
					},
				}
				if err := client.Database("admin").
					RunCommand(ctx, bson.M{"replSetInitiate": rsConfig}).
					Err(); err != nil {
					return xerrors.Errorf("replSetInitiate: %w", err)
				}

				log.Info("Data replica set initialized")

				return nil
			},

			IP:   "127.0.0.1",
			Port: 29001,
		})
	})

	// Routing or "mongos" servers.
	g.Go(func() error {
		select {
		case <-replicaSetInitialized:
		case <-gCtx.Done():
			return gCtx.Err()
		}

		return runServer(gCtx, log, Options{
			Name:             "routing",
			BinaryPath:       cfg.Mongos,
			RoutingServer:    true,
			ConfigServerAddr: path.Join(rsConfig, "127.0.0.1:28001"),

			OnReady: func(ctx context.Context, client *mongo.Client) error {
				if err := client.Database("admin").
					RunCommand(ctx, bson.M{"addShard": path.Join(rsData, "127.0.0.1:29001")}).
					Err(); err != nil {
					return xerrors.Errorf("addShard: %w", err)
				}

				log.Info("Shard added")

				log.Info("Initializing database")
				// Mongo does not provide explicit way to create database.
				// Just creating void collection.
				if err := client.Database(cfg.DB).CreateCollection(ctx, "_init"); err != nil {
					return xerrors.Errorf("create collection: %w", err)
				}

				log.Info("Enabling sharding")
				if err := client.Database("admin").
					RunCommand(ctx, bson.M{"enableSharding": cfg.DB}).
					Err(); err != nil {
					return xerrors.Errorf("enableSharding: %w", err)
				}

				log.Info("Sharding enabled", zap.String("db", cfg.DB))

				if err := cfg.OnSetup(ctx, client); err != nil {
					return xerrors.Errorf("OnSetup: %w", err)
				}

				return nil
			},

			IP:   "127.0.0.1",
			Port: 29501,
		})
	})

	return g.Wait()
}

func run(ctx context.Context, log *zap.Logger) error {
	var (
		mongod  = flag.String("mongod", "/usr/bin/mongod", "path for mongod")
		mongos  = flag.String("mongos", "/usr/bin/mongos", "path for mongos")
		baseDir = flag.String("dir", "", "dir to use for data (default to current dir)")
	)
	flag.Parse()

	if err := ensureCluster(ctx, log, ClusterConfig{
		Mongod: *mongod,
		Mongos: *mongos,
		Dir:    *baseDir,
		DB:     "cloud",
		OnSetup: func(ctx context.Context, client *mongo.Client) error {
			log.Info("Cluster is up")
			return nil
		},
	}); err != nil {
		return xerrors.Errorf("ensure cluster: %w", err)
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
