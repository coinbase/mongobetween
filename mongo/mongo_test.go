package mongo_test

import (
	"errors"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"
	"os"
	"sync"
	"testing"
	"time"
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

func slowQuery(t *testing.T) *mongo.Message {
	find, err := bson.Marshal(bson.D{
		{Key: "find", Value: "trainers"},
		{Key: "filter", Value: bson.D{{Key: "$where", Value: "sleep(10000) || true"}}},
		{Key: "limit", Value: 1},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "test"},
	})
	assert.Nil(t, err)

	return mongo.NewOpMsg(find, []bsoncore.Document{})
}

func serverAddress() string {
	if os.Getenv("CI") == "true" {
		return "mongodb://mongo:27017/test"
	}
	return "mongodb://localhost:27017/test"
}

func TestRoundTrip(t *testing.T) {
	uri := serverAddress()

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri)
	m, err := mongo.Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	msg := insertOpMsg(t)

	res, err := m.RoundTrip(msg)
	assert.Nil(t, err)

	single := mongo.ExtractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())
}

func TestRoundTripSharedContext(t *testing.T) {
	uri := serverAddress()

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetMaxPoolSize(10)
	m, err := mongo.Connect(zap.L(), sd, clientOptions, true)
	assert.Nil(t, err)

	msg := slowQuery(t)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			_, err := m.RoundTrip(msg)
			assert.Regexp(t, "incomplete read of message header: read tcp", err)
			assert.Regexp(t, "i/o timeout", err)

			wg.Done()
		}()
	}

	time.Sleep(200 * time.Millisecond)
	s := mongo.SelectServer(t, m)

	err = errors.New("some network error occurred")
	fakeError := topology.ConnectionError{Wrapped: err}
	s.ProcessError(fakeError)

	wg.Wait()
}

func TestRoundTripProcessError(t *testing.T) {
	uri := serverAddress()

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

	res, err := m.RoundTrip(msg)
	assert.Nil(t, err)

	single := mongo.ExtractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())

	assert.Equal(t, description.Standalone, m.Description().Servers[0].Kind)

	// kill the proxy
	p.Kill()

	_, err = m.RoundTrip(msg)
	assert.Error(t, driver.Error{}, err)

	assert.Equal(t, description.ServerKind(description.Unknown), m.Description().Servers[0].Kind, "Failed to update the server Kind to Unknown")
}
