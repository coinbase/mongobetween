# Deployment

## How Optioryx runs it

mongobetween runs as an ECS Fargate service in the same private VPC as the Lambda workers. Lambda functions connect to it via its internal DNS name (Cloud Map service discovery). mongobetween connects outbound to Atlas using AWS IAM authentication via the ECS task role.

```
Lambda --> mongobetween (ECS, private VPC) --> Atlas (MONGODB-AWS auth)
```

The Lambda `MONGODB_CLUSTER` environment variable controls which endpoint it connects to:
- **Via mongobetween:** `mongodb://mongobetween.internal:27016`
- **Direct to Atlas (fallback):** `mongodb+srv://<cluster>/?authSource=%24external&authMechanism=MONGODB-AWS`

Toggling this env var is how you enable or disable mongobetween per environment without any code changes.

## ECS task configuration

The container is started with a single positional argument mapping a listen address to an upstream URI:

```
mongobetween ":27016=mongodb+srv://<cluster>/?authSource=%24external&authMechanism=MONGODB-AWS&maxpoolsize=<N>&label=pulse"
```

`maxpoolsize` in the upstream URI controls how many persistent connections mongobetween holds open to Atlas. Size this based on expected peak Lambda concurrency and Atlas tier connection limits. A value of 20-50 is a reasonable starting point for M30.

## Security groups

- mongobetween's security group: inbound TCP 27016 from the Lambda security group only.
- Lambda's security group: outbound TCP 27016 to the mongobetween security group.
- Atlas network access: add the mongobetween ECS task's NAT gateway IP (or VPC CIDR) instead of the Lambda NAT gateway IP.

## IAM

The ECS task role needs the same Atlas IAM permissions the Lambda previously held. Once Lambda traffic moves through mongobetween, the Lambda execution role no longer needs direct Atlas access.

## Observability

mongobetween reports metrics to a statsd sidecar at `localhost:8125` by default (override with `-statsd`). Key metrics to watch:

- `mongobetween.pool.open_connections` -- should stay stable; a sustained climb means connections are leaking
- `mongobetween.pool.checked_out_connections` -- spikes here under load are normal; a plateau at `maxpoolsize` means the pool is the bottleneck
- `mongobetween.pool_event.connection_check_out_failed` -- non-zero means clients are being rejected; increase `maxpoolsize` or scale the service

## Scaling

Run at least 2 Fargate tasks for HA. Each task maintains its own independent pool to Atlas. Lambda round-robins across tasks via Cloud Map DNS.

Two tasks of 256 CPU / 512 MB memory handles hundreds of concurrent Lambda connections comfortably. Scale up if `pool.checked_out_connections` is regularly saturating `maxpoolsize`.
