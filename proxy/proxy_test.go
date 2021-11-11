package proxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"

	mongob "github.com/coinbase/mongobetween/mongo"
)

var (
	ctx       = context.Background()
	proxyPort = 27020
)

type Trainer struct {
	Name string
	Age  int
	City string
}

func TestProxy(t *testing.T) {
	proxy := setupProxy(t)

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	client := setupClient(t, "localhost", proxyPort)
	collection := client.Database("test").Collection("trainers")
	_, err := collection.DeleteMany(ctx, bson.D{{}})
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

func TestProxyUnacknowledgedWrites(t *testing.T) {
	proxy := setupProxy(t)
	defer proxy.Shutdown()

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	// Create a client with retryable writes disabled so the test will fail if the proxy crashes while processing the
	// unacknowledged write. If the proxy were to crash, it would close all connections and the next write would error
	// if retryable writes are disabled.
	clientOpts := options.Client().SetRetryWrites(false)
	client := setupClient(t, "localhost", proxyPort, clientOpts)
	defer func() {
		err := client.Disconnect(ctx)
		assert.Nil(t, err)
	}()

	// Create two *Collection instances: one for setup and basic operations and and one configured with an
	// unacknowledged write concern for testing.
	wc := writeconcern.New(writeconcern.W(0))
	setupCollection := client.Database("test").Collection("trainers")
	unackCollection, err := setupCollection.Clone(options.Collection().SetWriteConcern(wc))
	assert.Nil(t, err)

	// Setup by deleting all documents.
	_, err = setupCollection.DeleteMany(ctx, bson.D{})
	assert.Nil(t, err)

	ash := Trainer{"Ash", 10, "Pallet Town"}
	_, err = unackCollection.InsertOne(ctx, ash)
	assert.Equal(t, mongo.ErrUnacknowledgedWrite, err) // driver returns a special error value for w=0 writes

	// Insert a document using the setup collection and ensure document count is 2. Doing this ensures that the proxy
	// did not crash while processing the unacknowledged write.
	_, err = setupCollection.InsertOne(ctx, ash)
	assert.Nil(t, err)

	count, err := setupCollection.CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func TestProxyWithDynamicConfig(t *testing.T) {
	json := fmt.Sprintf(`{
	  "Clusters": {
		":%d": {
		  "DisableWrites": true,
		  "RedirectTo": ""
		},
		":%d": {
		  "DisableWrites": false,
		  "RedirectTo": ":%d"
		},
		":%d": {
		  "DisableWrites": false,
		  "RedirectTo": ""
		}
	  }
	}`, proxyPort, proxyPort+1, proxyPort+2, proxyPort+2)
	f, err := ioutil.TempFile("", "*.json")
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(f.Name())
	}()
	_, err = f.Write([]byte(json))
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	d, err := NewDynamic(f.Name(), zap.L())
	assert.Nil(t, err)

	proxies := setupProxies(t, d, proxyPort, 3)
	defer func() {
		for _, p := range proxies {
			p.Shutdown()
		}
	}()
	for _, p := range proxies {
		proxy := p
		go func() {
			err := proxy.Run()
			assert.Nil(t, err)
		}()
	}

	clients := []*mongo.Client{setupClient(t, "localhost", proxyPort), setupClient(t, "localhost", proxyPort+1), setupClient(t, "localhost", proxyPort+2)}
	defer func() {
		for _, client := range clients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	}()

	var upstreamClients []*mongo.Client
	if os.Getenv("CI") == "true" {
		upstreamClients = []*mongo.Client{setupClient(t, "mongo1", 27017), setupClient(t, "mongo2", 27017), setupClient(t, "mongo3", 27017)}
	} else {
		upstreamClients = []*mongo.Client{setupClient(t, "localhost", 27017), setupClient(t, "localhost", 27017+1), setupClient(t, "localhost", 27017+2)}
	}
	defer func() {
		for _, client := range upstreamClients {
			err := client.Disconnect(ctx)
			assert.Nil(t, err)
		}
	}()

	for _, client := range upstreamClients {
		collection := client.Database("test").Collection("trainers")
		_, err := collection.DeleteMany(ctx, bson.D{{}})
		assert.Nil(t, err)
	}

	ash := Trainer{"Ash", 10, "Pallet Town"}
	misty := Trainer{"Misty", 10, "Cerulean City"}
	brock := Trainer{"Brock", 15, "Pewter City"}

	// expect write error
	_, err = clients[0].Database("test").Collection("trainers").InsertOne(ctx, ash)
	assert.Error(t, err, "socket was unexpectedly closed")

	_, err = clients[1].Database("test").Collection("trainers").InsertMany(ctx, []interface{}{misty, brock})
	assert.Nil(t, err)

	count, err := clients[0].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	count, err = clients[1].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)

	count, err = clients[2].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)

	// check upstreams for expected counts
	count, err = upstreamClients[0].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	count, err = upstreamClients[1].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	count, err = upstreamClients[2].Database("test").Collection("trainers").CountDocuments(ctx, bson.D{})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func setupProxy(t *testing.T) *Proxy {
	t.Helper()

	dynamic, err := NewDynamic("", zap.L())
	assert.Nil(t, err)

	return setupProxies(t, dynamic, proxyPort, 1)[0]
}

func setupProxies(t *testing.T, d *Dynamic, startPort int, count int) []*Proxy {
	t.Helper()

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	upstreams := make(map[string]*mongob.Mongo)
	lookup := func(address string) *mongob.Mongo {
		return upstreams[address]
	}

	var proxies []*Proxy
	for i := 0; i < count; i++ {
		port := startPort + i
		uri := fmt.Sprintf("mongodb://localhost:%d/test", 27017+port-proxyPort)
		if os.Getenv("CI") == "true" {
			uri = fmt.Sprintf("mongodb://mongo%d:27017/test", 1+port-proxyPort)
		}

		address := fmt.Sprintf(":%d", port)

		upstream, err := mongob.Connect(zap.L(), sd, options.Client().ApplyURI(uri), false)
		assert.Nil(t, err)
		upstreams[address] = upstream

		proxy, err := NewProxy(zap.L(), sd, "label", "tcp4", address, false, lookup, d)
		assert.Nil(t, err)

		proxies = append(proxies, proxy)
	}

	return proxies
}

func setupClient(t *testing.T, host string, port int, clientOpts ...*options.ClientOptions) *mongo.Client {
	t.Helper()

	// Base options should only use ApplyURI. The full set should have the user-supplied options after uriOpts so they
	// will win out in the case of conflicts.
	proxyURI := fmt.Sprintf("mongodb://%s:%d/test", host, port)
	uriOpts := options.Client().ApplyURI(proxyURI)
	allClientOpts := append([]*options.ClientOptions{uriOpts}, clientOpts...)

	client, err := mongo.Connect(ctx, allClientOpts...)
	assert.Nil(t, err)

	// Call Ping with a low timeout to ensure the cluster is running and fail-fast if not.
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	err = client.Ping(pingCtx, nil)
	if err != nil {
		// Clean up in failure cases.
		_ = client.Disconnect(ctx)

		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging cluster: %v", err)
	}

	return client
}
