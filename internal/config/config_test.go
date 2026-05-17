package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Set test environment variables
	os.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
	os.Setenv("STORAGE_ENDPOINT", "minio:9000")
	os.Setenv("BROWSER_POOL_SIZE", "5")
	os.Setenv("STORAGE_SSL", "true")

	defer func() {
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("STORAGE_ENDPOINT")
		os.Unsetenv("BROWSER_POOL_SIZE")
		os.Unsetenv("STORAGE_SSL")
	}()

	cfg := LoadConfig()

	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka1:9092" {
		t.Errorf("Expected 2 Kafka brokers, got %v", cfg.KafkaBrokers)
	}

	if cfg.S3Endpoint != "minio:9000" {
		t.Errorf("Expected S3 endpoint minio:9000, got %s", cfg.S3Endpoint)
	}

	if cfg.BrowserPoolSize != 5 {
		t.Errorf("Expected browser pool size 5, got %d", cfg.BrowserPoolSize)
	}

	if cfg.S3UseSSL != true {
		t.Errorf("Expected S3 SSL true, got %v", cfg.S3UseSSL)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	// Ensure no env vars are set
	os.Unsetenv("KAFKA_BROKERS")
	os.Unsetenv("STORAGE_ENDPOINT")
	os.Unsetenv("BROWSER_POOL_SIZE")

	cfg := LoadConfig()

	if cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("Expected default Kafka broker localhost:9092, got %s", cfg.KafkaBrokers[0])
	}

	if cfg.BrowserPoolSize != 2 {
		t.Errorf("Expected default pool size 2, got %d", cfg.BrowserPoolSize)
	}

	if cfg.S3Endpoint != "localhost:9000" {
		t.Errorf("Expected default S3 endpoint localhost:9000, got %s", cfg.S3Endpoint)
	}
}
