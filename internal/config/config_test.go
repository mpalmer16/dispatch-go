package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadUsesDefaultsWhenOptionalEnvVarsAreUnset(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("BATCH_SIZE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DatabaseURL != "postgres://dispatch:dispatch@localhost:5432/dispatch" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}

	if cfg.KafkaBootstrapServers != "localhost:9092" {
		t.Fatalf("KafkaBootstrapServers = %q", cfg.KafkaBootstrapServers)
	}

	if cfg.KafkaTopic != "orders.created" {
		t.Fatalf("KafkaTopic = %q", cfg.KafkaTopic)
	}

	if cfg.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %s, want %s", cfg.PollInterval, 2*time.Second)
	}

	if cfg.BatchSize != 10 {
		t.Fatalf("BatchSize = %d, want %d", cfg.BatchSize, 10)
	}
}

func TestLoadUsesOptionalEnvVarsWhenProvided(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL", "750ms")
	t.Setenv("BATCH_SIZE", "25")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.PollInterval != 750*time.Millisecond {
		t.Fatalf("PollInterval = %s, want %s", cfg.PollInterval, 750*time.Millisecond)
	}

	if cfg.BatchSize != 25 {
		t.Fatalf("BatchSize = %d, want %d", cfg.BatchSize, 25)
	}
}

func TestLoadRejectsMissingRequiredEnvVars(t *testing.T) {
	tests := []struct {
		name    string
		envKey  string
		wantErr string
	}{
		{
			name:    "database url",
			envKey:  "DATABASE_URL",
			wantErr: "DATABASE_URL is required",
		},
		{
			name:    "kafka bootstrap servers",
			envKey:  "KAFKA_BOOTSTRAP_SERVERS",
			wantErr: "KAFKA_BOOTSTRAP_SERVERS is required",
		},
		{
			name:    "kafka topic",
			envKey:  "KAFKA_TOPIC",
			wantErr: "KAFKA_TOPIC is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.envKey, "")

			_, err := Load()
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}

			if err.Error() != tt.wantErr {
				t.Fatalf("Load() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadRejectsInvalidPollInterval(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL", "not-a-duration")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}

	if !strings.Contains(err.Error(), "POLL_INTERVAL must be a valid duration") {
		t.Fatalf("Load() error = %q", err.Error())
	}
}

func TestLoadRejectsNonPositivePollInterval(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("POLL_INTERVAL", tt.value)

			_, err := Load()
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}

			if err.Error() != "POLL_INTERVAL must be greater than zero" {
				t.Fatalf("Load() error = %q", err.Error())
			}
		})
	}
}

func TestLoadRejectsInvalidBatchSize(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BATCH_SIZE", "abc")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}

	if !strings.Contains(err.Error(), "BATCH_SIZE must be an integer") {
		t.Fatalf("Load() error = %q", err.Error())
	}
}

func TestLoadRejectsNonPositiveBatchSize(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "zero", value: "0"},
		{name: "negative", value: "-5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("BATCH_SIZE", tt.value)

			_, err := Load()
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}

			if err.Error() != "BATCH_SIZE must be greater than zero" {
				t.Fatalf("Load() error = %q", err.Error())
			}
		})
	}
}

func setRequiredEnv(t *testing.T) {
	t.Helper()

	t.Setenv("DATABASE_URL", "postgres://dispatch:dispatch@localhost:5432/dispatch")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "orders.created")
}
