package app

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"dispatch-go/internal/config"
	"dispatch-go/internal/kafka"
	"dispatch-go/internal/postgres"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Run(ctx context.Context, cfg config.Config, db *sql.DB, producer *ckafka.Producer) error {
	log.Printf("relay started: poll_interval=%s batch_size=%d topic=%s", cfg.PollInterval, cfg.BatchSize, cfg.KafkaTopic)

	if err := processBatch(ctx, cfg, db, producer); err != nil {
		log.Printf("relay batch failed: %v", err)
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := processBatch(ctx, cfg, db, producer); err != nil {
				log.Printf("relay batch failed: %v", err)
			}
		}
	}
}

func processBatch(ctx context.Context, cfg config.Config, db *sql.DB, producer *ckafka.Producer) error {
	pendingRows, err := postgres.ListPendingOutbox(ctx, db, cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("list pending outbox rows: %w", err)
	}

	if len(pendingRows) == 0 {
		return nil
	}

	log.Printf("relay batch: processing %d outbox rows", len(pendingRows))

	for _, row := range pendingRows {
		if err := kafka.Publish(producer, cfg.KafkaTopic, row.Payload); err != nil {
			return fmt.Errorf("publish outbox row %s: %w", row.ID, err)
		}

		if err := postgres.MarkOutboxProcessed(ctx, db, row.ID); err != nil {
			return fmt.Errorf("mark outbox row %s processed: %w", row.ID, err)
		}

		log.Printf("relay batch: published and marked processed outbox row=%s aggregate_id=%s", row.ID, row.AggregateID)
	}

	return nil
}
