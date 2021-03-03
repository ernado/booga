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
	"strings"
	"sync"
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

type ServerType byte

const (
	// DataServer is just regular mongo instance.
	DataServer ServerType = iota
	// ConfigServer is topology configuration mongo instance.
	ConfigServer
	// RoutingServer is router (proxy) for queries, mongos.
	RoutingServer
)

// Options for running mongo.
type Options struct {
	Type       ServerType
	BinaryPath string
	Name       string

	ReplicaSet string // only for ConfigServer or DataServer
	BaseDir    string // only for ConfigServer or DataServer

	ConfigServerAddr string // only for RoutingServer

	OnReady func(ctx context.Context, client *mongo.Client) error

	IP   string
	Port int
}

// runServer runs mongo server with provided options until error or context
// cancellation.
func runServer(ctx context.Context, log *zap.Logger, opt Options) error {
	log = log.Named(opt.Name)

	dir := filepath.Join(opt.BaseDir, opt.Name)
	switch opt.Type {
	case DataServer, ConfigServer:
		// Ensuring instance directory.
		log.Info("State will be persisted to tmp directory", zap.String("dir", dir))
		cleanup, err := ensureTempDir(dir)
		if err != nil {
			return xerrors.Errorf("ensure dir: %w", err)
		}
		// Directory will be removed recursively on cleanup.
		defer cleanup()
	}

	g, gCtx := errgroup.WithContext(ctx)

	log.Info("Starting")
	g.Go(func() error {
		// Piping mongo logs to zap logger.
		logReader, logFlush := logProxy(log, g)
		defer logFlush()

		args := []string{
			"--bind_ip", opt.IP,
			"--port", strconv.Itoa(opt.Port),
		}

		switch opt.Type {
		case ConfigServer:
			args = append(args, "--configsvr")
		case DataServer:
			args = append(args, "--shardsvr")
		}

		switch opt.Type {
		case ConfigServer, DataServer:
			args = append(args, "--replSet", opt.ReplicaSet, "--dbpath", ".")
		case RoutingServer:
			// Routing server is stateless.
			args = append(args, "--configdb", opt.ConfigServerAddr)
		}

		cmd := exec.CommandContext(gCtx, opt.BinaryPath, args...)
		cmd.Stdout = logReader
		cmd.Stderr = logReader

		switch opt.Type {
		case ConfigServer, DataServer:
			cmd.Dir = dir
		}

		return cmd.Run()
	})
	g.Go(func() error {
		// Waiting up to 30 seconds for mongo server to be available.
		ctx, cancel := context.WithTimeout(gCtx, time.Second*30)
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

	Replicas int
	Shards   int

	// OnSetup is called
	OnSetup func(ctx context.Context, client *mongo.Client) error
}

func dataPort(shardID, id int) int {
	return 29000 + shardID*100 + id
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
			Name:       "cfg",
			BaseDir:    cfg.Dir,
			BinaryPath: cfg.Mongod,
			ReplicaSet: rsConfig,
			Type:       ConfigServer,
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

		dG, dCtx := errgroup.WithContext(gCtx)

		for shardID := 0; shardID < cfg.Shards; shardID++ {
			rsName := fmt.Sprintf("%s%d", rsData, shardID)

			var members []bson.M
			for id := 0; id < cfg.Replicas; id++ {
				members = append(members,
					bson.M{
						"_id":  id,
						"host": fmt.Sprintf("127.0.0.1:%d", dataPort(shardID, id)),
					},
				)
			}
			rsConfig := bson.M{
				"_id":     rsName,
				"members": members,
			}

			var initOnce sync.Once

			for id := 0; id < cfg.Replicas; id++ {
				opt := Options{
					Name:       fmt.Sprintf("data-%d-%d", shardID, id),
					BaseDir:    cfg.Dir,
					BinaryPath: cfg.Mongod,
					ReplicaSet: rsName,
					Type:       DataServer,

					OnReady: func(ctx context.Context, client *mongo.Client) error {
						var err error
						initOnce.Do(func() {
							err = client.Database("admin").
								RunCommand(ctx, bson.M{"replSetInitiate": rsConfig}).
								Err()
						})
						return err
					},

					IP:   "127.0.0.1",
					Port: dataPort(shardID, id),
				}

				dG.Go(func() error {
					return runServer(dCtx, log, opt)
				})
			}
		}

		return dG.Wait()
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
			Type:             RoutingServer,
			ConfigServerAddr: path.Join(rsConfig, "127.0.0.1:28001"),

			OnReady: func(ctx context.Context, client *mongo.Client) error {
				// Add every shard.
				for shardID := 0; shardID < cfg.Shards; shardID++ {
					// Specify every replica set member.
					rsName := fmt.Sprintf("%s%d", rsData, shardID)
					var rsAddr []string
					for id := 0; id < cfg.Replicas; id++ {
						rsAddr = append(rsAddr, fmt.Sprintf("127.0.0.1:%d", dataPort(shardID, id)))
					}
					if err := client.Database("admin").
						RunCommand(ctx, bson.M{
							"addShard": path.Join(rsName, strings.Join(rsAddr, ",")),
						}).
						Err(); err != nil {
						return xerrors.Errorf("addShard: %w", err)
					}
				}

				log.Info("Shards added")

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
		mongod   = flag.String("mongod", "/usr/bin/mongod", "path for mongod")
		mongos   = flag.String("mongos", "/usr/bin/mongos", "path for mongos")
		shards   = flag.Int("shards", 2, "count of shards")
		replicas = flag.Int("replicas", 3, "count of replicas")
		baseDir  = flag.String("dir", "", "dir to use for data (default to current dir)")
	)
	flag.Parse()

	if err := ensureCluster(ctx, log, ClusterConfig{
		Mongod:   *mongod,
		Mongos:   *mongos,
		Shards:   *shards,
		Replicas: *replicas,
		Dir:      filepath.Join(*baseDir, "cloud_data"),
		DB:       "cloud",
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
