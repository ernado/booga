package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type Cluster struct {
	log *zap.Logger

	mongod string // mongod binary path
	mongos string // mongos binary path

	dir string // base directory
	db  string // database name

	addr string

	replicas int
	shards   int

	onSetup func(ctx context.Context, client *mongo.Client) error
}

func NewCluster(opt ClusterConfig) *Cluster {
	return &Cluster{
		log: opt.Log,

		mongod:   opt.Mongod,
		mongos:   opt.Mongos,
		dir:      opt.Dir,
		db:       "cloud",
		addr:     opt.Addr,
		replicas: opt.Replicas,
		shards:   opt.Shards,
		onSetup:  opt.OnSetup,
	}
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
func (c *Cluster) runServer(ctx context.Context, opt Options) error {
	log := c.log.Named(opt.Name)

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
			args = append(args,
				"--replSet", opt.ReplicaSet,
				"--dbpath", ".",
				"--wiredTigerCacheSizeGB", "2",
			)
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

		if err := opt.OnReady(gCtx, client); err != nil {
			return xerrors.Errorf("onReady: %w", err)
		}

		return nil
	})

	return g.Wait()
}

type ClusterConfig struct {
	Log *zap.Logger

	Mongod string // mongod binary path
	Mongos string // mongos binary path

	Dir string // base directory
	DB  string // database name

	Replicas int
	Shards   int

	// OnSetup is called
	OnSetup func(ctx context.Context, client *mongo.Client) error
	Addr    string
}

func dataPort(shardID, id int) int {
	return 29000 + shardID*100 + id
}

func (c *Cluster) ensure(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)
	replicaSetInitialized := make(chan struct{})

	const (
		rsData   = "rsData"
		rsConfig = "rsConfig"
	)

	// Configuration servers.
	g.Go(func() error {
		return c.runServer(gCtx, Options{
			Name:       "cfg",
			BaseDir:    c.dir,
			BinaryPath: c.mongod,
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

				c.log.Info("Config replica set initialized")
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

		for shardID := 0; shardID < c.shards; shardID++ {
			rsName := fmt.Sprintf("%s%d", rsData, shardID)

			var members []bson.M
			for id := 0; id < c.replicas; id++ {
				members = append(members, bson.M{
					"_id":  id,
					"host": fmt.Sprintf("127.0.0.1:%d", dataPort(shardID, id)),
				})
			}
			rsConfig := bson.M{
				"_id":     rsName,
				"members": members,
			}

			var initOnce sync.Once

			for id := 0; id < c.replicas; id++ {
				opt := Options{
					Name:       fmt.Sprintf("data-%d-%d", shardID, id),
					BaseDir:    c.dir,
					BinaryPath: c.mongod,
					ReplicaSet: rsName,
					Type:       DataServer,

					OnReady: func(ctx context.Context, client *mongo.Client) error {
						var err error
						initOnce.Do(func() {
							err = client.Database("admin").
								RunCommand(ctx, bson.M{"replSetInitiate": rsConfig}).
								Err()
						})
						if err != nil {
							return xerrors.Errorf("init: %w", err)
						}

						return nil
					},

					IP:   "127.0.0.1",
					Port: dataPort(shardID, id),
				}

				dG.Go(func() error {
					return c.runServer(dCtx, opt)
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

		return c.runServer(gCtx, Options{
			Name:             "routing",
			BinaryPath:       c.mongos,
			Type:             RoutingServer,
			ConfigServerAddr: path.Join(rsConfig, "127.0.0.1:28001"),

			OnReady: func(ctx context.Context, client *mongo.Client) error {
				// Add every shard.
				for shardID := 0; shardID < c.shards; shardID++ {
					// Specify every replica set member.
					rsName := fmt.Sprintf("%s%d", rsData, shardID)
					var rsAddr []string
					for id := 0; id < c.replicas; id++ {
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

				c.log.Info("Shards added")

				c.log.Info("Initializing database")
				// Mongo does not provide explicit way to create database.
				// Just creating void collection.
				if err := client.Database(c.db).CreateCollection(ctx, "_init"); err != nil {
					return xerrors.Errorf("create collection: %w", err)
				}

				c.log.Info("Enabling sharding")
				if err := client.Database("admin").
					RunCommand(ctx, bson.M{"enableSharding": c.db}).
					Err(); err != nil {
					return xerrors.Errorf("enableSharding: %w", err)
				}

				c.log.Info("Sharding enabled", zap.String("db", c.db))

				if err := c.setup(ctx, client); err != nil {
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

func (c *Cluster) setup(ctx context.Context, client *mongo.Client) error {
	if c.onSetup == nil {
		return nil
	}
	return c.onSetup(ctx, client)
}

func (c *Cluster) Run(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)

	server := &http.Server{
		Addr: c.addr,
	}
	g.Go(func() error {
		defer c.log.Named("http").Info("Server closed")

		// Waiting until group context is done.
		<-gCtx.Done()

		// Allowing some timeout for graceful shutdown.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()

		return server.Shutdown(closeCtx)
	})
	g.Go(func() error {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error { return c.ensure(ctx) })

	return g.Wait()
}
