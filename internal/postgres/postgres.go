package postgres

import (
	"context"
	"database/sql"
	"dispatch-go/internal/kafka"
	"dispatch-go/internal/outbox"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

const pingTimeout = 5 * time.Second

func Open(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	return db, nil
}

func Ping(ctx context.Context, db *sql.DB) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	return nil
}

func CountPendingOutbox(ctx context.Context, db *sql.DB) (int, error) {
	const query = `
		SELECT COUNT(*)
		FROM order_outbox
		WHERE processed_at IS NULL
	`

	var count int

	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count pending outbox rows: %w", err)
	}

	return count, nil
}

func ListPendingOutbox(ctx context.Context, db *sql.DB, limit int) ([]outbox.Row, error) {
	const query = `
  		SELECT id, aggregate_id, event_type, payload, created_at, processed_at
  		FROM order_outbox
  		WHERE processed_at IS NULL
  		ORDER BY created_at ASC
  		LIMIT $1
  	`

	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox rows: %w", err)
	}
	defer rows.Close()

	var result []outbox.Row

	for rows.Next() {
		var row outbox.Row

		err := rows.Scan(
			&row.ID,
			&row.AggregateID,
			&row.EventType,
			&row.Payload,
			&row.CreatedAt,
			&row.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan pending outbox row: %w", err)
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox rows: %w", err)
	}

	producer, err := kafka.NewProducer(cfg.KafkaBootstrapServers)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	return result, nil

}
