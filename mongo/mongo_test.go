package mongo_test

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.uber.org/zap"
	"os"
	"testing"
)

func insertOpMsg(t *testing.T) *mongo.Message {
	insert, err := bson.Marshal(bson.D{
		{Key: "insert", Value: "trainers"},
		{Key: "$db", Value: "test"},
	})
	assert.Nil(t, err)

	doc1, err := bson.Marshal(bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "name", Value: "Misty"},
		{Key: "age", Value: 10},
		{Key: "city", Value: "Cerulean City"},
	})
	assert.Nil(t, err)

	doc2, err := bson.Marshal(bson.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "name", Value: "Brock"},
		{Key: "age", Value: 15},
		{Key: "city", Value: "Pewter City"},
	})
	assert.Nil(t, err)

	return mongo.NewOpMsg(insert, []bsoncore.Document{doc1, doc2})
}

func TestRoundTrip(t *testing.T) {
	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri)
	m, err := mongo.Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	msg := insertOpMsg(t)

	res, err := m.RoundTrip(msg, []string{})
	assert.Nil(t, err)

	single := mongo.ExtractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())
}

func TestRoundTripProcessError(t *testing.T) {
	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	opts := options.Client().ApplyURI(uri)
	p, err := proxy.NewProxy(zap.L(), sd, "label", "tcp4", ":27019", false, true, opts)
	assert.Nil(t, err)

	go func() {
		err := p.Run()
		assert.Nil(t, err)
	}()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27019/test")
	m, err := mongo.Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	msg := insertOpMsg(t)

	res, err := m.RoundTrip(msg, []string{})
	assert.Nil(t, err)

	single := mongo.ExtractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())

	assert.Equal(t, description.Standalone, m.Description().Servers[0].Kind)

	// kill the proxy
	p.Kill()

	_, err = m.RoundTrip(msg, []string{})
	assert.Error(t, driver.Error{}, err)

	assert.Equal(t, description.ServerKind(description.Unknown), m.Description().Servers[0].Kind, "Failed to update the server Kind to Unknown")
}
