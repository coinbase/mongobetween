# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About mongobetween

mongobetween is a lightweight MongoDB connection pooler written in Go by Coinbase. It handles a large number of incoming connections and multiplexes them across a smaller connection pool to MongoDB clusters. The proxy appears to applications as an always-available MongoDB shard router (mongos).

## Development Commands

### Build and Test
```bash
# Build the binary
make build
# or
go build -o bin/mongobetween .

# Run tests with race detection  
make test
# or
go test -count 1 -race ./...

# Run linter
make lint
# or
GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v -E golint ./...

# Run with Docker
make docker
# or 
docker-compose up
```

### Running mongobetween
```bash
# Basic TCP socket example
./bin/mongobetween ":27016=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"

# Unix socket example  
./bin/mongobetween -network unix "/tmp/mongo.sock=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"
```

## Architecture Overview

### Core Components

**Main Entry Point (`mongobetween.go`)**
- Parses configuration and creates proxy instances
- Runs each proxy in a separate goroutine
- Handles graceful shutdown via signal handling

**Configuration (`config/`)**
- `config.go`: Parses command line flags and creates client configurations
- Handles MongoDB connection URI parsing and validation
- Sets up logging (zap) and metrics (statsd) clients

**Proxy Layer (`proxy/`)**
- `proxy.go`: Main proxy server that listens on TCP/Unix sockets
- `connection.go`: Handles individual client connections 
- `dynamic.go`: Supports dynamic configuration for write disabling and traffic redirection

**MongoDB Integration (`mongo/`)**
- `mongo.go`: Wrapper around the official MongoDB Go driver
- `operations.go`: Core message handling and request/response proxying (largest file ~1000 lines)
- `ismaster.go`: Handles ismaster/hello commands by responding as a mongos router
- `cursor_cache.go`: Maps cursor IDs to specific MongoDB servers for proper routing
- `transaction_cache.go`: Tracks client sessions for transaction pinning to servers

**Utilities**
- `util/`: Statsd metrics utilities
- `lruttl/`: LRU cache with TTL functionality

### Data Flow

1. Client connects to mongobetween proxy socket
2. Proxy creates a Connection object to handle the client
3. Connection receives MongoDB wire protocol messages
4. Messages are parsed and routed through mongo.Mongo 
5. mongo.Mongo uses the official MongoDB Go driver to forward requests
6. Responses are returned back through the same path
7. Special handling for ismaster commands (responds directly without proxying)

### Key Design Patterns

- **Connection Multiplexing**: Many client connections share fewer MongoDB driver connections
- **Request Proxying**: Wire protocol messages are parsed and forwarded with minimal modification
- **Server Pinning**: Cursors and transactions are pinned to specific MongoDB servers
- **Graceful Shutdown**: Signal handling allows for clean connection closure

### Testing Setup

The CI uses Docker containers running MongoDB instances (mongo1, mongo2, mongo3) to test proxy functionality. Tests use gotestsum for JUnit report generation.