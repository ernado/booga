package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type fixture struct {
	log *zap.Logger

	users int
	faces int
	files int
}

func (f fixture) Load(ctx context.Context, client *mongo.Client) error {
	f.log.Info("Cluster is up")

	cloud := client.Database("cloud")

	for _, collection := range []string{
		"files", "users", "faces",
	} {
		if err := cloud.CreateCollection(ctx, collection); err != nil {
			return xerrors.Errorf("create collection: %w", err)
		}
		if err := client.Database("admin").
			RunCommand(ctx, bson.D{
				{"shardCollection", "cloud." + collection},
				{"key", bson.M{"user_id": 1}},
			}).Err(); err != nil {
			return xerrors.Errorf("shard collection: %w", err)
		}
	}

	// Populating.
	files := cloud.Collection("files")
	users := cloud.Collection("users")
	faces := cloud.Collection("faces")

	userIDs := make(chan int)

	uG, uCtx := errgroup.WithContext(ctx)

	processUser := func(userID int) error {
		var models []mongo.WriteModel

		if _, err := users.InsertOne(uCtx, bson.M{"user_id": userID}); err != nil {
			return xerrors.Errorf("user: %w", err)
		}

		var allFaces []uuid.UUID
		for j := 0; j < 30; j++ {
			id, _ := uuid.New()
			allFaces = append(allFaces, id)
			if _, err := faces.InsertOne(uCtx, bson.M{
				"_id":     id,
				"user_id": userID,
				"name":    fmt.Sprintf("Foo Name %d", j),
			}); err != nil {
				return xerrors.Errorf("face: %w", err)
			}
		}

		for j := 0; j < 30000; j++ {
			id, _ := uuid.New()

			var facesList []uuid.UUID

			if len(allFaces) > 0 {
				for f := 0; f < rand.Intn(10); f++ {
					facesList = append(facesList, allFaces[rand.Intn(len(allFaces))])
				}
			}

			models = append(models, &mongo.InsertOneModel{
				Document: bson.M{
					"user_id":    userID,
					"file_id":    j,
					"created_at": time.Now(),
					"p1":         rand.Int(),
					"p2":         rand.Int(),
					"faces":      facesList,

					"model_id": id,
				},
			})
		}

		if _, err := files.BulkWrite(ctx, models); err != nil {
			return xerrors.Errorf("files buil: %w", err)
		}

		f.log.Info("Processed user", zap.Int("user_id", userID))

		return nil
	}

	for j := 0; j < 10; j++ {
		uG.Go(func() error {
			for {
				select {
				case id, ok := <-userIDs:
					if !ok {
						return nil
					}
					if err := processUser(id); err != nil {
						return err
					}
				case <-uCtx.Done():
					return uCtx.Err()
				}
			}
		})
	}

	uG.Go(func() error {
		defer close(userIDs)

		for userID := 0; userID < 1000; userID++ {
			select {
			case userIDs <- userID:
				continue
			case <-uCtx.Done():
				return uCtx.Err()
			}
		}

		return nil
	})

	if err := uG.Wait(); err != nil {
		return xerrors.Errorf("insert: %w", err)
	}

	keysDoc := bson.D{
		{"user_id", int32(1)},
		{"files", int32(1)},
	}
	model := mongo.IndexModel{
		Keys: keysDoc,
	}

	if _, err := files.Indexes().CreateOne(ctx, model); err != nil {
		return xerrors.Errorf("index: %w", err)
	}

	return nil
}
