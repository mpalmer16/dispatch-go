package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL           string
	KafkaBootstrapServers string
	KafkaTopic            string
	PollInterval          time.Duration
	BatchSize             int
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		KafkaBootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaTopic:            os.Getenv("KAFKA_TOPIC"),
		PollInterval:          2 * time.Second,
		BatchSize:             10,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	if cfg.KafkaBootstrapServers == "" {
		return Config{}, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}

	if cfg.KafkaTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC is required")
	}

	if rawPollInterval := os.Getenv("POLL_INTERVAL"); rawPollInterval != "" {
		pollInterval, err := time.ParseDuration(rawPollInterval)
		if err != nil {
			return Config{}, fmt.Errorf("POLL_INTERVAL must be a valid duration: %w", err)
		}

		if pollInterval <= 0 {
			return Config{}, fmt.Errorf("POLL_INTERVAL must be greater than zero")
		}

		cfg.PollInterval = pollInterval
	}

	if rawBatchSize := os.Getenv("BATCH_SIZE"); rawBatchSize != "" {
		batchSize, err := strconv.Atoi(rawBatchSize)
		if err != nil {
			return Config{}, fmt.Errorf("BATCH_SIZE must be an integer: %w", err)
		}

		if batchSize <= 0 {
			return Config{}, fmt.Errorf("BATCH_SIZE must be greater than zero")
		}

		cfg.BatchSize = batchSize
	}

	return cfg, nil
}
