# mongobetween
`mongobetween` is a lightweight MongoDB connection pooler written in Golang. It's primary function is to handle a large number of incoming connections, and multiplex them a across a smaller connection pool to one or more MongoDB clusters.

`mongobetween` is used in production at Coinbase. It is currently deployed as a Docker sidecar alongside a Rails application using the [Ruby Mongo driver](https://github.com/mongodb/mongo-ruby-driver), connecting to a number of sharded MongoDB clusters. It was designed to connect to `mongos` routers who are responsible for server selection for read/write preferences (connecting directly to a replica set's `mongod` instances hasn't been battle tested).

### How it works
`mongobetween` listens for incoming connections from an application, and proxies any queries to the [MongoDB Go driver](https://github.com/mongodb/mongo-go-driver) which is connected to a MongoDB cluster. It also intercepts any `ismaster` commands from the application, and responds with `"I'm a shard router (mongos)"`, without proxying. This means `mongobetween` appears to the application as an always-available MongoDB shard router, and any MongoDB connection issues or failovers are handled internally by the Go driver.

### Installation
```
go install github.com/coinbase/mongobetween
```

### Usage
```
Usage: mongobetween [OPTIONS] address1=uri1 [address2=uri2] ...
  -loglevel string
    	One of: debug, info, warn, error, dpanic, panic, fatal (default "info")
  -network string
    	One of: tcp, tcp4, tcp6, unix or unixpacket (default "tcp4")
  -password string
    	MongoDB password
  -ping
    	Ping downstream MongoDB before listening
  -pretty
    	Pretty print logging
  -statsd string
    	Statsd address (default "localhost:8125")
  -unlink
    	Unlink existing unix sockets before listening
  -username string
    	MongoDB username
```

TCP socket example:
```
mongobetween ":27016=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"
```

Unix socket example:
```
mongobetween -network unix "/tmp/mongo.sock=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"
```

Proxying multiple clusters:
```
mongobetween -network unix \
  "/tmp/mongo1.sock=mongodb+srv://username:password@cluster1.mongodb.net/database?maxpoolsize=10&label=cluster1" \
  "/tmp/mongo2.sock=mongodb+srv://username:password@cluster2.mongodb.net/database?maxpoolsize=10&label=cluster2"
```

The `label` query parameter in the connection URI is used to any tag statsd metrics or logs for that connection.

### TODO

Current known missing features:
 - [ ] Transaction server pinning
 - [ ] Different cursors on separate servers with the same cursor ID value

### Statsd
`mongobetween` supports reporting health metrics to a local statsd sidecar, using the [Datadog Go library](github.com/DataDog/datadog-go). By default it reports to `localhost:8125`. The following metrics are reported:
 - `mongobetween.handle_message` (Timing) - end-to-end time handling an incoming message from the application
 - `mongobetween.round_trip` (Timing) - round trip time sending a request and receiving a response from MongoDB
 - `mongobetween.request_size` (Distribution) - request size to MongoDB
 - `mongobetween.response_size` (Distribution) - response size from MongoDB
 - `mongobetween.open_connections` (Gauge) - number of open connections between the proxy and the application
 - `mongobetween.connection_opened` (Counter) - connection opened with the application
 - `mongobetween.connection_closed` (Counter) - connection closed with the application
 - `mongobetween.cursors` (Gauge) - number of open cursors being tracked (for cursor -> server mapping)
 - `mongobetween.server_selection` (Timing) - Go driver server selection timing
 - `mongobetween.checkout_connection` (Timing) - Go driver connection checkout timing
 - `mongobetween.pool.checked_out_connections` (Gauge) - number of connections checked out from the Go driver connection pool
 - `mongobetween.pool.open_connections` (Gauge) - number of open connections from the Go driver to MongoDB
 - `mongobetween.pool_event.connection_closed` (Counter) - Go driver connection closed
 - `mongobetween.pool_event.connection_pool_created` (Counter) - Go driver connection pool created
 - `mongobetween.pool_event.connection_created` (Counter) - Go driver connection created
 - `mongobetween.pool_event.connection_check_out_failed` (Counter) - Go driver connection check out failed
 - `mongobetween.pool_event.connection_checked_out` (Counter) - Go driver connection checked out
 - `mongobetween.pool_event.connection_checked_in` (Counter) - Go driver connection checked in
 - `mongobetween.pool_event.connection_pool_cleared` (Counter) - Go driver connection pool cleared
 - `mongobetween.pool_event.connection_pool_closed` (Counter) - Go driver connection pool closed

### Background
`mongobetween` was built to address a connection storm issue between a high scale Rails app and MongoDB (see [blog post](https://blog.coinbase.com/scaling-connections-with-ruby-and-mongodb-99204dbf8857)). Due to Ruby MRI's global interpreter lock, multi-threaded web applications don't utilize multiple CPU cores. To achieve better CPU utilization, Puma is run with multiple workers (processes), each of which need a separate MongoDB connection pool. This leads to a large number of connections to MongoDB, sometimes exceeding MongoDB's upstream connection limit of 128k connections.

`mongobetween` has reduced connection counts by an order of magnitude, spikes of up to 30k connections are now reduced to around 2k. It has also significantly reduced `ismaster` commands on the cluster, as there's only a single monitor goroutine per `mongobetween` process, instead of a monitor thread for each Ruby process.
