package proxy

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var (
	collection = "test_proxy_with_dual_reads"
)

func setupDualReadClients(t *testing.T) (*observer.ObservedLogs, []*mongo.Client) {
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
	defer func() {
		_ = os.Remove(f.Name())
	}()
	_, err = f.Write([]byte(json))
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	logger := zap.New(observedZapCore)
	d, err := NewDynamic(f.Name(), logger)
	assert.Nil(t, err)

	proxies := setupProxies(t, d, proxyPort, 2)
	defer func() {
		for _, p := range proxies {
			p.Shutdown()
		}
	}()
	for _, p := range proxies {
		proxy := p
		proxy.log = logger
		go func() {
			err := proxy.Run()
			assert.Nil(t, err)
		}()
	}

	clients := []*mongo.Client{setupClient(t, "localhost", proxyPort), setupClient(t, "localhost", proxyPort+1)}
	defer func() {
		for _, client := range clients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	}()

	var upstreamClients []*mongo.Client
	if os.Getenv("CI") == "true" {
		upstreamClients = []*mongo.Client{setupClient(t, "mongo1", 27017), setupClient(t, "mongo2", 27017)}
	} else {
		upstreamClients = []*mongo.Client{setupClient(t, "localhost", 27017), setupClient(t, "localhost", 27017+1)}
	}
	defer func() {
		for _, client := range upstreamClients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	}()

	for _, client := range upstreamClients {
		collection := client.Database("test").Collection(collection)
		_, err := collection.DeleteMany(ctx, bson.D{{}})
		assert.Nil(t, err)
	}

	return observedLogs, clients
}

func TestProxyWithDualReads(t *testing.T) {
	observedLogs, clients := setupDualReadClients(t)

	ash := Trainer{primitive.NewObjectID(), "Ash", 10, "Pallet Town"}
	misty := Trainer{primitive.NewObjectID(), "Misty", 10, "Cerulean City"}
	brock := Trainer{primitive.NewObjectID(), "Brock", 15, "Pewter City"}

	_, err := clients[0].Database("test").Collection(collection).InsertOne(ctx, ash)
	assert.Nil(t, err)

	_, err = clients[1].Database("test").Collection(collection).InsertMany(ctx, []interface{}{ash, misty, brock})
	assert.Nil(t, err)

	filter := bson.D{{Key: "name", Value: "Ash"}}

	var result Trainer
	err = clients[0].Database("test").Collection(collection).FindOne(ctx, filter).Decode(&result)
	assert.Nil(t, err)

	dualReadMatchLogs := observedLogs.FilterMessage("Dual reads match").All()
	assert.Equal(t, 1, len(dualReadMatchLogs))
	dualReadMismatchLogs := observedLogs.FilterMessage("Dual reads mismatch").All()
	assert.Equal(t, 0, len(dualReadMismatchLogs))

	count, err := clients[0].Database("test").Collection(collection).CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
	dualReadMatchLogs = observedLogs.FilterMessage("Dual reads match").All()
	assert.Equal(t, 1, len(dualReadMatchLogs))
	dualReadMismatchLogs = observedLogs.FilterMessage("Dual reads mismatch").All()
	assert.Equal(t, 1, len(dualReadMismatchLogs))

	// Test with cursors
	cursor, err := clients[0].Database("test").Collection(collection).Find(ctx, bson.M{})
	assert.Nil(t, err)
}
