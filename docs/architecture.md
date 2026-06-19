# Architecture

## What it does

mongobetween sits between application clients (Lambda functions, ECS services) and MongoDB Atlas. It accepts many short-lived client connections and multiplexes them over a fixed-size persistent pool of upstream connections.

```
Lambda (maxPoolSize=1)  --|
Lambda (maxPoolSize=1)  --|--> mongobetween (persistent pool, N conns) --> Atlas
Lambda (maxPoolSize=1)  --|
```

Without mongobetween, each Lambda cold start opens its own TLS connection to Atlas. Under a burst of 200+ simultaneous cold starts, this causes hundreds of concurrent TLS handshakes that saturate Atlas's kernel network stack -- even without hitting the per-tier connection limit.

## Wire protocol

mongobetween speaks the MongoDB wire protocol (OP_MSG). It intercepts `isMaster`/`hello` handshakes from clients and responds as a `mongos` shard router, so clients always see a healthy, available endpoint and never attempt their own server selection or failover. The real upstream MongoDB driver (inside mongobetween) handles topology monitoring, server selection, and failover transparently.

## Connection pinning

Two types of operations require pinning to a specific upstream connection:

- **Cursors** -- a `getMore` must go to the same server that created the cursor. The cursor cache in `mongo/cursor_cache.go` maps cursor IDs to upstream connections.
- **Transactions** -- all operations in a session must go to the same server. The transaction cache in `mongo/transaction_cache.go` maps client sessions to upstream connections.

All other operations are load-balanced across the pool.

## Authentication

- **Client to mongobetween:** no authentication. mongobetween is expected to run inside a private VPC, not exposed publicly.
- **mongobetween to Atlas:** whatever mechanism is in the upstream URI. In ECS we use `authMechanism=MONGODB-AWS`, which the Go driver resolves using the ECS task IAM role credentials automatically.

## Known gaps

- Read preference routing (secondary reads) is not fully implemented. See the open PRs on GitHub. All reads currently go to the primary.
- Large cursor result sets that span multiple `getMore` calls should work, but this path has seen less production testing than primary read/write flows.
