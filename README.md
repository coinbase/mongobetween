# mongobetween

`mongobetween` is a lightweight MongoDB connection pooler written in Go. It handles a large number of incoming connections and multiplexes them across a smaller connection pool to one or more MongoDB clusters.

This is an Optioryx fork of [coinbase/mongobetween](https://github.com/coinbase/mongobetween). We forked it to maintain read preference support, Atlas TLS compatibility, and other improvements the upstream has not merged.

---

### How it works

`mongobetween` listens for incoming connections from an application and proxies queries to the [MongoDB Go driver](https://github.com/mongodb/mongo-go-driver), which holds a persistent connection pool to the upstream cluster. It intercepts `isMaster`/`hello` commands from the application and responds as a shard router (`mongos`), so the application always sees an available MongoDB endpoint and failovers are handled internally by the Go driver.

### Installation

```
go install github.com/Optioryx/mongobetween
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
  -dynamic string
        File or URL to query for dynamic configuration
  -enable-sdam-metrics
        Enable SDAM (Server Discovery And Monitoring) metrics
  -enable-sdam-logging
        Enable SDAM (Server Discovery And Monitoring) logging
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

The `label` query parameter tags statsd metrics and logs for that connection.

### Dynamic configuration

Passing a file or URL as the `-dynamic` argument enables runtime reconfiguration. Example:

```json
{
  "Clusters": {
    ":12345": {
      "DisableWrites": true,
      "RedirectTo": ""
    },
    "/var/tmp/cluster1.sock": {
      "DisableWrites": false,
      "RedirectTo": "/var/tmp/cluster2.sock"
    }
  }
}
```

This disables writes on `:12345` and redirects traffic from `cluster1.sock` to `cluster2.sock`. Useful for minimal-downtime cluster migrations.

### Statsd metrics

`mongobetween` reports health metrics to a statsd sidecar via the [Datadog Go library](https://github.com/DataDog/datadog-go). Default address: `localhost:8125`.

| Metric | Type | Description |
|---|---|---|
| `mongobetween.handle_message` | Timing | End-to-end time handling an incoming message |
| `mongobetween.round_trip` | Timing | Round trip to MongoDB |
| `mongobetween.request_size` | Distribution | Request size to MongoDB |
| `mongobetween.response_size` | Distribution | Response size from MongoDB |
| `mongobetween.open_connections` | Gauge | Open connections between proxy and application |
| `mongobetween.connection_opened` | Counter | Connection opened with application |
| `mongobetween.connection_closed` | Counter | Connection closed with application |
| `mongobetween.cursors` | Gauge | Open cursors tracked (cursor-to-server mapping) |
| `mongobetween.transactions` | Gauge | Transactions tracked (session-to-server mapping) |
| `mongobetween.server_selection` | Timing | Go driver server selection |
| `mongobetween.checkout_connection` | Timing | Go driver connection checkout |
| `mongobetween.pool.checked_out_connections` | Gauge | Connections checked out from the pool |
| `mongobetween.pool.open_connections` | Gauge | Open connections from pool to MongoDB |
| `mongobetween.pool_event.connection_closed` | Counter | Go driver connection closed |
| `mongobetween.pool_event.connection_pool_created` | Counter | Go driver pool created |
| `mongobetween.pool_event.connection_created` | Counter | Go driver connection created |
| `mongobetween.pool_event.connection_check_out_failed` | Counter | Go driver connection checkout failed |
| `mongobetween.pool_event.connection_checked_out` | Counter | Go driver connection checked out |
| `mongobetween.pool_event.connection_checked_in` | Counter | Go driver connection checked in |
| `mongobetween.pool_event.connection_pool_cleared` | Counter | Go driver pool cleared |
| `mongobetween.pool_event.connection_pool_closed` | Counter | Go driver pool closed |

### Background

`mongobetween` was originally built at Coinbase to address connection storms between a high-scale Rails app and MongoDB ([blog post](https://blog.coinbase.com/scaling-connections-with-ruby-and-mongodb-99204dbf8857)). Puma's multi-process model meant each worker needed its own MongoDB connection pool, pushing connection counts to 30k+. `mongobetween` reduced that to ~2k by multiplexing many application connections over a single pool per proxy process.

At Optioryx we use it to bound MongoDB connections from AWS Lambda workers, where each cold-started function would otherwise open its own connection pool simultaneously.
