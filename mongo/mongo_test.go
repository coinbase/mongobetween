package mongo_test

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongod "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.uber.org/zap"
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
		uri = "mongodb://mongo1:27017/test"
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
		uri = "mongodb://mongo1:27017/test"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	upstream, err := mongo.Connect(zap.L(), sd, options.Client().ApplyURI(uri), false)
	assert.Nil(t, err)
	lookup := func(address string) *mongo.Mongo {
		return upstream
	}

	dynamic, err := proxy.NewDynamic("", zap.L())
	assert.Nil(t, err)

	p, err := proxy.NewProxy(zap.L(), sd, "label", "tcp4", ":27023", false, lookup, dynamic)
	assert.Nil(t, err)

	go func() {
		err := p.Run()
		assert.Nil(t, err)
	}()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27023/test")
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

func TestMongo_RoundTrip_NoCursorOrTxn(t *testing.T) {
	const uri = "mongodb://127.0.0.1:8001/?loadBalanced=true"

	// Create a MongoBetween client to perform mongobeteen operations in the test.
	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(10)
	clientb, err := mongo.Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	// Create an OP_MSG command that will respond with a non-exhausted cursor.
	// Then extract the cursor's id from the server response.
	cmdb, err := bson.Marshal(bson.D{
		{"insert", "coll"}, // Collection name
		{"$db", "simple"},  // database
		{"ordered", true},
	})

	assert.NoError(t, err)

	docByts, err := bson.Marshal(bson.D{{"x", 1}})
	cmd := mongo.NewOpMsg(cmdb, []bsoncore.Document{docByts})

	_, err = clientb.RoundTrip(cmd, []string{})
	assert.Nil(t, err)

	for i := 0; i < 50; i++ {
		// Put msg on the wire to get cursorID and first batch.
		_, err := clientb.RoundTrip(cmd, []string{})
		assert.Nil(t, err)
	}
}

func TestMongo_RoundTrip_Cursor(t *testing.T) {
	const uri = "mongodb://127.0.0.1:8001/?loadBalanced=true"
	const volume = 25

	// Create a driver client to load test data.
	opts := options.Client().ApplyURI(uri).SetLoadBalanced(true)

	clientd, err := mongod.Connect(context.Background(), opts)
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}

	defer func() { _ = clientd.Disconnect(context.Background()) }()

	// Insert test data into the "simple" collection on the "cursor" database.
	db := clientd.Database("cursor")

	coll := db.Collection("simple")
	defer func() { _ = coll.Drop(context.Background()) }()

	docs := make([]interface{}, volume)
	for i := 0; i < volume; i++ {
		docs[i] = bson.D{{"i64", i}}
	}

	_, err = coll.InsertMany(context.Background(), docs)
	if err != nil {
		log.Fatal(err)
	}

	// Create a MongoBetween client to perform mongobeteen operations in the test.
	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(10)
	clientb, err := mongo.Connect(zap.L(), sd, clientOptions, false)
	assert.Nil(t, err)

	// Create an OP_MSG command that will respond with a non-exhausted cursor.
	// Then extract the cursor's id from the server response.
	cmdb, err := bson.Marshal(bson.D{
		{"find", "simple"}, // Collection name
		{"$db", "cursor"},  // Database
		{"batchSize", 1},
		{"filter", bson.D{{"i64", bson.D{{"$lte", volume - 1}}}}}, // Query filter
	})

	assert.NoError(t, err)

	cmd := mongo.NewOpMsg(cmdb, nil)

	// Put msg on the wire to get cursorID and first batch.
	res, err := clientb.RoundTrip(cmd, []string{})
	assert.Nil(t, err)

	// Extract the cursorID from the response to create a "getMore" command.
	raw := bson.Raw(mongo.ExtractSingleOpMsg(t, res))

	cursor, err := raw.LookupErr("cursor")
	assert.NoError(t, err, "failed to lookup cursor")

	cursorDoc, ok := cursor.DocumentOK()
	assert.True(t, ok, "cursor ws not a document")

	cursorIDRaw, err := cursorDoc.LookupErr("id")
	assert.NoError(t, err, "failed to lookup cursorID")

	cursorID, ok := cursorIDRaw.Int64OK()
	assert.True(t, ok, "cursorID was not i64")

	// Create an OP_MSG "getMore" command.
	getMoreb, err := bson.Marshal(bson.D{
		{"getMore", cursorID},
		{"$db", "cursor"},
		{"collection", "simple"},
		{"batchSize", 1},
	})
	assert.NoError(t, err)

	getMoreCmd := mongo.NewOpMsg(getMoreb, nil)

	wg := sync.WaitGroup{}
	wg.Add(volume - 1) // Ensure every round trip completes

	for i := 0; i < volume-1; i++ {
		go func() {
			defer wg.Done()

			gmRes, err := clientb.RoundTrip(getMoreCmd, []string{})
			assert.Nil(t, err)

			// Make sure that the cursor does not have a "CursorNotFound" error code.
			doc := mongo.ExtractSingleOpMsg(t, gmRes)

			errCode, err := doc.LookupErr("code")
			if err != nil {
				return
				//continue
			}

			errCodeI32, ok := errCode.Int32OK()
			assert.False(t, ok && errCodeI32 == 43, "Cursor not found")
		}()
	}

	wg.Wait()
}
