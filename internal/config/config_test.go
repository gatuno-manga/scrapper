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

	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log level info, got %s", cfg.LogLevel)
	}

	if cfg.LogFormat != "json" {
		t.Errorf("Expected default log format json, got %s", cfg.LogFormat)
	}
}

// TestLoadConfig_LogLevelAndFormat guards OBS-02: LOG_LEVEL and LOG_FORMAT
// must be read from the environment so operators can raise verbosity for an
// investigation, or switch to text output for local dev, without a rebuild.
func TestLoadConfig_LogLevelAndFormat(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")
	defer func() {
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("LOG_FORMAT")
	}()

	cfg := LoadConfig()

	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log level debug, got %s", cfg.LogLevel)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("Expected log format text, got %s", cfg.LogFormat)
	}
}
