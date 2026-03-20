package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"dispatch-go/internal/config"
	"dispatch-go/internal/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := postgres.Open(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to open postgres: %v", err)
	}
	defer db.Close()

	if err := postgres.Ping(context.Background(), db); err != nil {
		log.Fatalf("failed to reach postgres: %v", err)
	}

	pendingCount, err := postgres.CountPendingOutbox(context.Background(), db)
	if err != nil {
		log.Fatalf("failed to count pending outbox rows: %v", err)
	}

	fmt.Printf("pending outbox rows: %d\n", pendingCount)

	pendingRows, err := postgres.ListPendingOutbox(context.Background(), db, 10)
	if err != nil {
		log.Fatalf("failed to list pending outbox rows: %v", err)
	}

	fmt.Printf("loaded %d pending outbox rows\n", len(pendingRows))

	for _, row := range pendingRows {
		fmt.Printf("outbox row id=%s aggregate_id=%s event_type=%s created_at=%s\n",
			row.ID,
			row.AggregateID,
			row.EventType,
			row.CreatedAt.Format(time.RFC3339),
		)
		fmt.Printf("payload: %s\n", string(row.Payload))
	}
}
