# dispatch-go

`dispatch-go` will be the relay service that sits between the Rust API and the Kotlin consumer.

Its job is:

1. read unprocessed rows from `order_outbox`
2. publish those events to Kafka
3. mark the outbox rows as processed

## Why Go

Go is a good fit for this service because it is simple to package, easy to run in Docker and Kubernetes, and well suited to long-running worker-style processes.

## Planned Flow

The expected loop for this service is:

1. connect to Postgres
2. connect to Kafka
3. poll `order_outbox` for rows where `processed_at IS NULL`
4. publish the stored event payload to the `orders.created` topic
5. update `processed_at`
6. repeat

## Suggested Package Layout

This project is intentionally empty for now, but the directory shape is ready for a typical Go service:

- `cmd/relay`
  The main entrypoint for the service
- `internal/app`
  App wiring and startup
- `internal/config`
  Environment/config loading
- `internal/outbox`
  Polling and row-processing logic
- `internal/postgres`
  Database access helpers
- `internal/kafka`
  Kafka producer wrapper

## Suggested First Steps

When you start implementing:

1. define config loading from env
2. add a Postgres connection
3. add an outbox row model
4. add a query for unprocessed rows
5. add a Kafka producer
6. add a loop that publishes and marks rows processed
7. add graceful shutdown and logging

## Local Project Commands

Once code exists, a typical workflow will probably be:

```bash
go test ./...
go run ./cmd/relay
```
