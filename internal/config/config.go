package config

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL           string
	KafkaBootstrapServers string
	KafkaTopic            string
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		KafkaBootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaTopic:            os.Getenv("KAFKA_TOPIC"),
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

	return cfg, nil
}
