package proxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var (
	collection = "test_proxy_with_dual_reads"
)

func setupDualReadClients(t *testing.T) (*observer.ObservedLogs, []*mongo.Client, []func()) {
	json := fmt.Sprintf(`{
	  "Clusters": {
		":%d": {
      "DualReadFrom": ":%d",
		  "DualReadSamplePercent": 100
		}
	  }
	}`, proxyPort, proxyPort+1)
	f, err := ioutil.TempFile("", "*.json")
	assert.Nil(t, err)

	var shutdownFuncs []func()
	shutdownFuncs = append(shutdownFuncs, func() {
		_ = os.Remove(f.Name())
	})

	_, err = f.Write([]byte(json))
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	logger := zap.New(observedZapCore)
	d, err := NewDynamic(f.Name(), logger)
	assert.Nil(t, err)

	proxies := setupProxies(t, d, proxyPort, 2)
	shutdownFuncs = append(shutdownFuncs, func() {
		for _, p := range proxies {
			p.Shutdown()
		}
	})
	for _, p := range proxies {
		proxy := p
		proxy.log = logger
		go func() {
			err := proxy.Run()
			assert.Nil(t, err)
		}()
	}

	clients := []*mongo.Client{setupClient(t, "localhost", proxyPort), setupClient(t, "localhost", proxyPort+1)}
	shutdownFuncs = append(shutdownFuncs, func() {
		for _, client := range clients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	})

	var upstreamClients []*mongo.Client
	if os.Getenv("CI") == "true" {
		upstreamClients = []*mongo.Client{setupClient(t, "mongo1", 27017), setupClient(t, "mongo2", 27017)}
	} else {
		upstreamClients = []*mongo.Client{setupClient(t, "localhost", 27017), setupClient(t, "localhost", 27017+1)}
	}
	shutdownFuncs = append(shutdownFuncs, func() {
		for _, client := range upstreamClients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	})

	for _, client := range upstreamClients {
		collection := client.Database("test").Collection(collection)
		_, err := collection.DeleteMany(ctx, bson.D{{}})
		assert.Nil(t, err)
	}

	return observedLogs, clients, shutdownFuncs
}

func TestProxyWithDualReads(t *testing.T) {
	observedLogs, clients, shutdownFuncs := setupDualReadClients(t)

	ash := Trainer{primitive.NewObjectID(), "Ash", 10, "Pallet Town"}
	gary := Trainer{primitive.NewObjectID(), "Gary", 10, "Pallet Town"}
	misty := Trainer{primitive.NewObjectID(), "Misty", 10, "Cerulean City"}
	brock := Trainer{primitive.NewObjectID(), "Brock", 15, "Pewter City"}

	_, err := clients[0].Database("test").Collection(collection).InsertOne(ctx, ash)
	assert.Nil(t, err)
	_, err = clients[0].Database("test").Collection(collection).InsertOne(ctx, gary)
	assert.Nil(t, err)
	_, err = clients[0].Database("test").Collection(collection).InsertOne(ctx, misty)
	assert.Nil(t, err)

	_, err = clients[1].Database("test").Collection(collection).InsertMany(ctx, []interface{}{ash, gary, misty, brock})
	assert.Nil(t, err)

	filter := bson.D{{Key: "name", Value: "Ash"}}

	var result Trainer
	err = clients[0].Database("test").Collection(collection).FindOne(ctx, filter).Decode(&result)
	assert.Nil(t, err)

	assertLogs(t, observedLogs, "Dual reads match", 1)
	assertLogs(t, observedLogs, "Dual reads mismatch", 0)

	count, err := clients[0].Database("test").Collection(collection).CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(3), count)
	assertLogs(t, observedLogs, "Dual reads match", 1)
	assertLogs(t, observedLogs, "Dual reads mismatch", 1)

	// Test with cursors
	findOptions := options.Find()
	findOptions.SetBatchSize(1)
	cursor, err := clients[0].Database("test").Collection(collection).Find(ctx, bson.D{{Key: "age", Value: 10}}, findOptions)
	assert.Nil(t, err)

	// Grab/iterate through all 3 documents in cursor
	ok := cursor.Next(ctx)
	assert.True(t, ok)
	ok = cursor.Next(ctx)
	assert.True(t, ok)
	ok = cursor.Next(ctx)
	assert.True(t, ok)
	assertLogs(t, observedLogs, "Dual reads match", 4)
	assertLogs(t, observedLogs, "Dual reads mismatch", 1)

	for _, f := range shutdownFuncs {
		f()
	}
}

func assertLogs(t *testing.T, logs *observer.ObservedLogs, message string, count int) {
	ctxTimeout, _ := context.WithTimeout(ctx, 2*time.Second)

	for {
		select {
		case <-ctxTimeout.Done():
			t.Errorf("Failed to assert log count %d for log message %s", count, message)
		default:
			matchedLogs := logs.FilterMessage(message).All()
			if count == len(matchedLogs) {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
