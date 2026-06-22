# CLAUDE.md

## Project Overview

`mongobetween` is a lightweight MongoDB connection pooling proxy written in Go. It accepts many short-lived client connections and multiplexes them over a smaller, persistent pool of connections to one or more MongoDB clusters.

This is the Optioryx fork of [coinbase/mongobetween](https://github.com/coinbase/mongobetween). See [docs/architecture.md](docs/architecture.md) for how it works and [docs/deployment.md](docs/deployment.md) for how we run it.

## Repository Layout

```
config/     -- flag parsing, client options, monitoring setup
mongo/      -- upstream MongoDB connection, cursor/transaction tracking
proxy/      -- listener, connection handling, message routing
docs/       -- architecture and deployment docs
```

## Development Commands

```bash
# Build
go build ./...

# Run tests
go test ./...

# Run a local proxy against Atlas dev cluster
go run . ":27016=mongodb+srv://user:pass@cluster.mongodb.net/db?maxpoolsize=5&label=dev"

# Lint
golangci-lint run
```

## Key Conventions

- **Base branch:** `main`. All PRs target `main`. See [`.claude/rules/base-branch.md`](.claude/rules/base-branch.md).
- **Tests must pass** before merging. See [`.claude/rules/no-failing-tests.md`](.claude/rules/no-failing-tests.md).
- **Commit messages and PR copy** follow the authorship hygiene rules. See [`.claude/rules/llm-authorship-hygiene.md`](.claude/rules/llm-authorship-hygiene.md).
- **No template expressions in GitHub Actions `run:` blocks.** See [`.claude/rules/github-actions-no-template-in-run.md`](.claude/rules/github-actions-no-template-in-run.md).

## Things to Know

- mongobetween presents itself to clients as a `mongos` shard router by intercepting `isMaster`/`hello` commands. The upstream MongoDB driver handles real server selection and failover.
- Cursors and transactions are pinned to specific upstream connections. The cursor cache and transaction cache in `mongo/` track these mappings.
- Authentication to upstream Atlas uses whatever mechanism is in the connection URI (`authMechanism=MONGODB-AWS` for IAM auth in ECS).
- Client connections to mongobetween require no authentication -- it is expected to run on the same VPC as callers, not exposed publicly.
- The upstream coinbase tags are preserved as `vX.Y.Z+coinbase`. Optioryx releases start from `v1.0.0`.
