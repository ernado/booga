package booga

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
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

	replicas int
	shards   int
	topology Topology

	maxCacheGB float64

	onSetup      func(ctx context.Context, client *mongo.Client) error
	setupTimeout time.Duration
	services     map[string]func()
}

func New(opt Config) *Cluster {
	return &Cluster{
		log: opt.Log,

		mongod:     opt.Mongod,
		mongos:     opt.Mongos,
		dir:        opt.Dir,
		db:         "cloud",
		replicas:   opt.Replicas,
		shards:     opt.Shards,
		maxCacheGB: opt.MaxCacheGB,

		setupTimeout: opt.SetupTimeout,
		onSetup:      opt.OnSetup,

		services: map[string]func(){},
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

type serverType byte

const (
	// dataServer is just regular mongo instance.
	dataServer serverType = iota
	// configServer is topology configuration mongo instance.
	configServer
	// routingServer is router (proxy) for queries, mongos.
	routingServer
)

// serverOptions for running mongo.
type serverOptions struct {
	Type       serverType
	BinaryPath string
	Name       string

	ReplicaSet string // only for configServer or dataServer
	BaseDir    string // only for configServer or dataServer

	ConfigServerAddr string // only for routingServer

	OnReady func(ctx context.Context, client *mongo.Client) error

	IP   string
	Port int
}

// runServer runs mongo server with provided options until error or context
// cancellation.
func (c *Cluster) runServer(ctx context.Context, opt serverOptions) error {
	log := c.log.Named(opt.Name)

	dir := filepath.Join(opt.BaseDir, opt.Name)
	switch opt.Type {
	case dataServer, configServer:
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
		case configServer:
			args = append(args, "--configsvr")
		case dataServer:
			args = append(args, "--shardsvr")
		}

		switch opt.Type {
		case configServer, dataServer:
			args = append(args,
				"--replSet", opt.ReplicaSet,
				"--dbpath", ".",
			)
			if c.maxCacheGB > 0 {
				args = append(args, "--wiredTigerCacheSizeGB", fmt.Sprintf("%f", c.maxCacheGB))
			}
		case routingServer:
			// Routing server is stateless.
			args = append(args, "--configdb", opt.ConfigServerAddr)
		}

		err := c.runRegistered(gCtx, opt.Name, func(ctx context.Context) error {
			cmd := exec.CommandContext(ctx, opt.BinaryPath, args...)
			cmd.Stdout = logReader
			cmd.Stderr = logReader

			switch opt.Type {
			case configServer, dataServer:
				cmd.Dir = dir
			}

			return cmd.Run()
		})

		return err
	})
	g.Go(func() error {
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

		ensureCtx, cancel := context.WithTimeout(gCtx, c.setupTimeout)
		defer cancel()

		if err := ensureServer(ensureCtx, log, client); err != nil {
			return xerrors.Errorf("ensure server: %w", err)
		}

		if err := opt.OnReady(gCtx, client); err != nil {
			return xerrors.Errorf("onReady: %w", err)
		}

		return nil
	})

	return g.Wait()
}

type Config struct {
	Log *zap.Logger

	Mongod string // mongod binary path
	Mongos string // mongos binary path

	Dir string // base directory
	DB  string // database name

	Replicas int
	Shards   int

	MaxCacheGB float64

	OnSetup      func(ctx context.Context, client *mongo.Client) error
	SetupTimeout time.Duration
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
		return c.runServer(gCtx, serverOptions{
			Name:       "cfg",
			BaseDir:    c.dir,
			BinaryPath: c.mongod,
			ReplicaSet: rsConfig,
			Type:       configServer,
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
				opt := serverOptions{
					Name:       fmt.Sprintf("data-%d-%d", shardID, id),
					BaseDir:    c.dir,
					BinaryPath: c.mongod,
					ReplicaSet: rsName,
					Type:       dataServer,

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

		return c.runServer(gCtx, serverOptions{
			Name:             "routing",
			BinaryPath:       c.mongos,
			Type:             routingServer,
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

				// Start monitoring cluster topology
				go c.MonitorTopology(ctx, "127.0.0.1:29501")

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
	return c.ensure(ctx)
}

func (c *Cluster) runRegistered(parentCtx context.Context, name string, f func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancel(parentCtx)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := f(gCtx); err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		return nil
	})

	c.services[name] = cancel

	return g.Wait()
}

func (c *Cluster) Services() []string {
	var services []string

	for k := range c.services {
		services = append(services, k)
	}

	sort.Strings(services)
	return services
}

func (c *Cluster) Kill(name string) error {
	f, ok := c.services[name]
	if !ok {
		return xerrors.Errorf("no service %s", name)
	}

	f()
	delete(c.services, name)

	return nil
}

type Topology struct {
	Shards []Shard
}

type Shard struct {
	ID               string                   `bson:"_id"`
	Host             string                   `bson:"host"`
	State            uint64                   `bson:"state"`
	ReplicaSetStatus map[string]ReplicaStatus `bson:"-"`
	replicaSetAddrs  []string                 `bson:"-"`
}

type ReplicaSetStatus struct {
	Members []ReplicaStatus `bson:"members"`
}

type ReplicaStatus struct {
	Addr   string `bson:"name"`
	Health uint64 `bson:"health"`
	State  string `bson:"stateStr"`
}

func (c *Cluster) Topology() Topology {
	return c.topology
}

func (c *Cluster) MonitorTopology(ctx context.Context, mongosAddr string) {
	mongos := connectInstance(ctx, c.log, mongosAddr)

	defer func() {
		_ = mongos.Disconnect(ctx)
	}()

	t := time.NewTicker(time.Second)

	for {
		select {
		case <-t.C:
			topology, err := getTopology(ctx, mongos)
			if err != nil {
				c.log.Error("failed to get cluster topology", zap.Error(err))

				continue
			}

			c.topology = *topology
		case <-ctx.Done():
			return
		}
	}
}

func connectInstance(ctx context.Context, log *zap.Logger, addr string) *mongo.Client {
	t := time.NewTicker(time.Second)

	for range t.C {
		client, err := mongo.Connect(ctx, options.Client().
			ApplyURI(fmt.Sprintf("mongodb://%s", addr)).
			// SetDirect is important, client can timeout otherwise.
			SetDirect(true),
		)
		if err != nil {
			log.Error("failed to connect to mongo instance", zap.String("addr", addr))

			continue
		}

		return client
	}

	return nil
}

func getTopology(ctx context.Context, mongos *mongo.Client) (*Topology, error) {
	cursor, err := mongos.Database("config").Collection("shards").
		Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	shards := make([]Shard, 0)

	for cursor.Next(ctx) {
		shard := Shard{
			ReplicaSetStatus: make(map[string]ReplicaStatus),
		}

		if err := cursor.Decode(&shard); err != nil {
			return nil, err
		}

		shard.replicaSetAddrs = strings.Split(strings.TrimPrefix(shard.Host, fmt.Sprintf("%s/", shard.ID)), ",")

		for _, replicaAddr := range shard.replicaSetAddrs {
			tctx, _ := context.WithTimeout(ctx, time.Millisecond*100)

			c, err := mongo.Connect(tctx, options.Client().
				ApplyURI(fmt.Sprintf("mongodb://%s", replicaAddr)).
				SetDirect(true))
			if err != nil {
				continue
			}

			defer func() {
				tctx, _ := context.WithTimeout(ctx, time.Millisecond*100)
				_ = c.Disconnect(tctx)
			}()

			var rsStatus ReplicaSetStatus
			tctx, _ = context.WithTimeout(ctx, time.Millisecond*100)

			if err := c.Database("admin").RunCommand(tctx, bson.D{{"replSetGetStatus", 1}}).
				Decode(&rsStatus); err != nil {
				continue
			}

			for _, status := range rsStatus.Members {
				shard.ReplicaSetStatus[status.Addr] = status
			}

			break
		}

		shards = append(shards, shard)
	}

	return &Topology{Shards: shards}, nil
}
