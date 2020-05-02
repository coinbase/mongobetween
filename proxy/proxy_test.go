package proxy

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"os"
	"testing"
)

type Trainer struct {
	Name string
	Age  int
	City string
}

func TestProxy(t *testing.T) {
	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	opts := options.Client().ApplyURI(uri)
	proxy, err := NewProxy(zap.L(), sd, "label", "tcp4", ":27016", false, true, opts)
	assert.Nil(t, err)

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	ctx := context.Background()
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27016/test")
	client, err := mongo.Connect(ctx, clientOptions)
	assert.Nil(t, err)

	err = client.Ping(ctx, nil)
	assert.Nil(t, err)

	collection := client.Database("test").Collection("trainers")
	_, err = collection.DeleteMany(ctx, bson.D{{}})
	assert.Nil(t, err)

	ash := Trainer{"Ash", 10, "Pallet Town"}
	misty := Trainer{"Misty", 10, "Cerulean City"}
	brock := Trainer{"Brock", 15, "Pewter City"}

	_, err = collection.InsertOne(ctx, ash)
	assert.Nil(t, err)

	_, err = collection.InsertMany(ctx, []interface{}{misty, brock})
	assert.Nil(t, err)

	filter := bson.D{{Key: "name", Value: "Ash"}}
	update := bson.D{
		{Key: "$inc", Value: bson.D{
			{Key: "age", Value: 1},
		}},
	}
	updateResult, err := collection.UpdateOne(ctx, filter, update)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), updateResult.MatchedCount)
	assert.Equal(t, int64(1), updateResult.ModifiedCount)

	var result Trainer
	err = collection.FindOne(ctx, filter).Decode(&result)
	assert.Nil(t, err)
	assert.Equal(t, "Pallet Town", result.City)

	var results []Trainer
	cur, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(2).SetBatchSize(1))
	assert.Nil(t, err)
	err = cur.All(ctx, &results)
	assert.Nil(t, err)
	assert.Equal(t, "Pallet Town", results[0].City)
	assert.Equal(t, "Cerulean City", results[1].City)

	deleteResult, err := collection.DeleteMany(ctx, bson.D{{}})
	assert.Nil(t, err)
	assert.Equal(t, int64(3), deleteResult.DeletedCount)

	err = client.Disconnect(ctx)
	assert.Nil(t, err)

	proxy.Shutdown()
}
