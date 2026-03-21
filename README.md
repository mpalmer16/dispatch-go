# dispatch-go

`dispatch-go` is the relay service between the Rust order API and the Kafka topic consumed by `dispatch-kt`.

Its job is to:

1. poll `order_outbox` for rows where `processed_at IS NULL`
2. publish the stored event payload to Kafka
3. mark the outbox row as processed
4. repeat until stopped

## Role In The System

The current event flow is:

1. `dispatch-rs` writes an order and an outbox row
2. `dispatch-go` reads the outbox row and publishes `order.created`
3. `dispatch-kt` consumes that event and writes a dispatch job

This keeps the Rust API responsible for transactional persistence while the relay handles asynchronous publication.

## Configuration

Required environment variables:

- `DATABASE_URL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`

Optional environment variables:

- `POLL_INTERVAL`
  Default: `2s`
- `BATCH_SIZE`
  Default: `10`

## Local Run

From the parent workspace, bring up shared infrastructure first:

```bash
cd ..
make local-setup
```

Then run the relay from this repo:

```bash
DATABASE_URL=postgres://dispatch:dispatch@localhost:5432/dispatch \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=orders.created \
go run ./cmd/relay
```

For faster local feedback:

```bash
DATABASE_URL=postgres://dispatch:dispatch@localhost:5432/dispatch \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=orders.created \
POLL_INTERVAL=1s \
BATCH_SIZE=5 \
go run ./cmd/relay
```

The relay logs startup, batch processing, publish failures, and successful processed rows. It shuts down cleanly on `Ctrl-C`.

## Development Layout

- `cmd/relay`
  Service entrypoint
- `internal/app`
  Polling loop and batch processing
- `internal/config`
  Environment loading and validation
- `internal/postgres`
  Outbox queries and processed-row updates
- `internal/kafka`
  Kafka producer wrapper
- `internal/outbox`
  Outbox row model

## How To See It Working

1. Start `dispatch-rs` and `dispatch-kt`.
2. Start this relay.
3. Create a new order through the Rust API with a fresh `Idempotency-Key`.
4. Watch the relay log a processed outbox row.

If you send the same request with the same idempotency key, the Rust service will reuse the existing order and no new outbox row will appear for the relay to publish.
