package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"dispatch-go/internal/app"
	"dispatch-go/internal/config"
	"dispatch-go/internal/kafka"
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

	producer, err := kafka.NewProducer(cfg.KafkaBootstrapServers)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	pendingCount, err := postgres.CountPendingOutbox(context.Background(), db)
	if err != nil {
		log.Fatalf("failed to count pending outbox rows: %v", err)
	}

	fmt.Printf("dispatch-go relay starting\n")
	fmt.Printf("pending outbox rows at startup: %d\n", pendingCount)
	fmt.Printf("kafka bootstrap servers: %s\n", cfg.KafkaBootstrapServers)
	fmt.Printf("kafka topic: %s\n", cfg.KafkaTopic)
	fmt.Printf("poll interval: %s\n", cfg.PollInterval)
	fmt.Printf("batch size: %d\n", cfg.BatchSize)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, cfg, db, producer); err != nil {
		log.Fatalf("relay stopped with error: %v", err)
	}

	log.Printf("relay stopped")
}
