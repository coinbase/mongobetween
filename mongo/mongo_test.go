package mongo

import (
	"context"
	"errors"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/Shopify/toxiproxy/client"
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

func newOpMsg(single bsoncore.Document, sequence []bsoncore.Document) *Message {
	sections := []opMsgSection{&opMsgSectionSingle{single}}
	if len(sequence) > 0 {
		sections = append(sections, &opMsgSectionSequence{
			identifier: "documents",
			msgs:       sequence,
		})
	}
	op := opMsg{sections: sections}
	wm := op.Encode(0)
	return &Message{wm, &op}
}

func extractSingleOpMsg(t *testing.T, msg *Message) bsoncore.Document {
	op, ok := msg.Op.(*opMsg)
	assert.True(t, ok)
	assert.Equal(t, 1, len(op.sections))
	single, ok := op.sections[0].(*opMsgSectionSingle)
	assert.True(t, ok)
	return single.msg
}

func insertOpMsg(t *testing.T) *Message {
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

	return newOpMsg(insert, []bsoncore.Document{doc1, doc2})
}

func slowQuery(t *testing.T) *Message {
	find, err := bson.Marshal(bson.D{
		{Key: "find", Value: "trainers"},
		{Key: "filter", Value: bson.D{{Key: "$where", Value: "sleep(10000) || true"}}},
		{Key: "limit", Value: 1},
		{Key: "singleBatch", Value: true},
		{Key: "$db", Value: "test"},
	})
	assert.Nil(t, err)

	return newOpMsg(find, []bsoncore.Document{})
}

func serverAddress() string {
	if os.Getenv("CI") == "true" {
		return "mongodb://mongo:27017/test"
	}
	return "mongodb://localhost:27017/test"
}

func toxiproxyAddress() string {
	if os.Getenv("CI") == "true" {
		return "mongodb://toxiproxy:27011/test"
	}
	return "mongodb://localhost:27011/test"
}

func toxiproxyClient() *toxiproxy.Client {
	if os.Getenv("CI") == "true" {
		return toxiproxy.NewClient("toxiproxy:8474")
	}
	return toxiproxy.NewClient("localhost:8474")
}

func TestRoundTrip(t *testing.T) {
	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	uri := serverAddress()
	clientOptions := options.Client().ApplyURI(uri)
	m, err := Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	msg := insertOpMsg(t)

	res, err := m.RoundTrip(msg)
	assert.Nil(t, err)

	single := extractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())
}

func TestRoundTripSharedContext(t *testing.T) {
	tp, err := toxiproxyClient().CreateProxy("mongo", "0.0.0.0:27011", "mongo:27017")
	assert.Nil(t, err)
	defer func() {
		err := tp.Delete()
		assert.Nil(t, err)
	}()

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	uri := toxiproxyAddress()
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetMaxPoolSize(10)
	m, err := Connect(zap.L(), sd, clientOptions, true)
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
	s, err := m.selectServer(context.Background(), 0)
	assert.Nil(t, err)

	_, err = tp.AddToxic("latency_down", "latency", "downstream", 1.0, toxiproxy.Attributes{"latency": 1000})
	assert.Nil(t, err)

	err = errors.New("some network error occurred")
	fakeError := topology.ConnectionError{Wrapped: err}
	s.ProcessError(fakeError)

	wg.Wait()
}

func TestRoundTripProcessError(t *testing.T) {
	tp, err := toxiproxyClient().CreateProxy("mongo", "0.0.0.0:27011", "mongo:27017")
	assert.Nil(t, err)
	defer func() {
		err := tp.Delete()
		assert.Nil(t, err)
	}()

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	uri := toxiproxyAddress()
	clientOptions := options.Client().ApplyURI(uri)
	m, err := Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	msg := insertOpMsg(t)

	res, err := m.RoundTrip(msg)
	assert.Nil(t, err)

	single := extractSingleOpMsg(t, res)

	assert.Equal(t, int32(2), single.Lookup("n").Int32())
	assert.Equal(t, 1.0, single.Lookup("ok").Double())

	assert.Equal(t, description.Standalone, m.Description().Servers[0].Kind)

	err = tp.Disable()
	assert.Nil(t, err)

	_, err = m.RoundTrip(msg)
	assert.Error(t, driver.Error{}, err)

	assert.Equal(t, description.ServerKind(description.Unknown), m.Description().Servers[0].Kind, "Failed to update the server Kind to Unknown")
}
