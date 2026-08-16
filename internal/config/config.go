package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBrokers    []string
	KafkaGroupID    string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	S3Region        string
	S3UseSSL        bool
	BrowserURL      string
	FlareSolverrURL string
	BrowserPoolSize int
	CacheMaxSizeMB  int64
	RedisHost       string
	RedisPort       string
	RedisPassword   string

	// Kafka General
	KafkaWriteTimeout           int // seconds
	KafkaReadTimeout            int // seconds
	KafkaRequiredAcks           int
	KafkaAllowAutoTopicCreation bool

	// Logging
	LogLevel  string // debug|info|warn|error, parsed via obs.ParseLevel
	LogFormat string // "json" (production) or "text" (local dev)

	// Ops server (OBS-06): /healthz, /readyz, /metrics, env-gated pprof
	OpsAddr      string
	PprofEnabled bool

	// ShutdownGracePeriod (BUG-04) bounds how long main waits for in-flight
	// handlers to drain on SIGTERM/SIGINT before forcing exit.
	ShutdownGracePeriod time.Duration

	// Kafka Topics
	TopicChapterRequested string
	TopicUpdateBookRequested string
	TopicNewBookRequested    string
	TopicCoversRequested     string
	TopicImagesRequested     string
	TopicTestRequested       string
	TopicImageProcessing     string
	TopicChapterPagesExtracted string
	TopicChapterCompleted    string
	TopicChapterFailed    string
	TopicBookCompleted    string
	TopicUpdateBookCompleted string
	TopicCoversCompleted  string
	TopicImagesCompleted  string
	TopicDLQ             string // dead-letter queue for unprocessable messages
	TopicBookFailed      string // failure events for new-book and update-book jobs

	// Storage
	ProcessedImagesBucket string // bucket where post-processed images land
	}


func LoadConfig() (Config, error) {
	_ = godotenv.Load()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	poolSize, err := getEnvInt("BROWSER_POOL_SIZE", 2)
	if err != nil {
		return Config{}, err
	}

	cacheSize, err := getEnvInt64("CACHE_MAX_SIZE_MB", 100)
	if err != nil {
		return Config{}, err
	}

	writeTimeout, err := getEnvInt("KAFKA_WRITE_TIMEOUT", 10)
	if err != nil {
		return Config{}, err
	}

	readTimeout, err := getEnvInt("KAFKA_READ_TIMEOUT", 10)
	if err != nil {
		return Config{}, err
	}

	acks, err := getEnvInt("KAFKA_REQUIRED_ACKS", 1) // default to acks=1 (leader only)
	if err != nil {
		return Config{}, err
	}

	shutdownGrace, err := getEnvDuration("SHUTDOWN_GRACE_PERIOD", 30*time.Second)
	if err != nil {
		return Config{}, err
	}

	endpoint := getEnv("STORAGE_ENDPOINT", "localhost:9000")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	cfg := Config{
		KafkaBrokers:    strings.Split(brokers, ","),
		KafkaGroupID:    getEnv("KAFKA_GROUP_ID", "scraper-microservice"),
		S3Endpoint:      endpoint,
		S3AccessKey:     getEnv("STORAGE_ACCESS_KEY", "rustfsadmin"),
		S3SecretKey:     getEnv("STORAGE_SECRET_KEY", "rustfsadmin"),
		S3Region:        getEnv("STORAGE_REGION", "us-east-1"),
		S3UseSSL:        os.Getenv("STORAGE_SSL") == "true",
		BrowserURL:      os.Getenv("BROWSER_URL"),
		FlareSolverrURL: getEnv("FLARESOLVERR_URL", "http://localhost:8191"),
		BrowserPoolSize: poolSize,
		CacheMaxSizeMB:  cacheSize,
		RedisHost:       getEnv("REDIS_HOST", "localhost"),
		RedisPort:       getEnv("REDIS_PORT", "6379"),
		RedisPassword:   getEnv("REDIS_PASSWORD", ""),

		KafkaWriteTimeout: writeTimeout,
		KafkaReadTimeout:  readTimeout,
		KafkaRequiredAcks: acks,
		// Defaults to false: an unrecognized TOPIC_* env var should fail
		// loudly rather than silently create an unconsumed topic. Set to
		// "true" only in dev compose environments that rely on it.
		KafkaAllowAutoTopicCreation: os.Getenv("KAFKA_ALLOW_AUTO_TOPIC_CREATION") == "true",

		LogLevel:  getEnv("LOG_LEVEL", "info"),
		LogFormat: getEnv("LOG_FORMAT", "json"),

		OpsAddr: getEnv("OPS_ADDR", ":6060"),
		// Defaults to false so pprof is opt-in even in production; flip it
		// via env, no rebuild required, and never expose this port publicly.
		PprofEnabled: os.Getenv("PPROF_ENABLED") == "true",

		ShutdownGracePeriod: shutdownGrace,

		TopicChapterRequested: getEnv("TOPIC_CHAPTER_REQUESTED", "scraping.chapter.requested"),
		TopicUpdateBookRequested: getEnv("TOPIC_UPDATE_BOOK_REQUESTED", "scraping.update-book.requested"),
		TopicNewBookRequested:    getEnv("TOPIC_NEW_BOOK_REQUESTED", "scraping.new-book.requested"),
		TopicCoversRequested:     getEnv("TOPIC_COVERS_REQUESTED", "scraping.covers.requested"),
		TopicImagesRequested:     getEnv("TOPIC_IMAGES_REQUESTED", "scraping.images.requested"),
		TopicTestRequested:       getEnv("TOPIC_TEST_REQUESTED", "scraping.test"),
		TopicImageProcessing:  getEnv("TOPIC_IMAGE_PROCESSING", "image.processing.requested"),
		TopicChapterPagesExtracted: getEnv("TOPIC_CHAPTER_PAGES_EXTRACTED", "scraping.chapter.pages_extracted"),
		TopicChapterCompleted: getEnv("TOPIC_CHAPTER_COMPLETED", "scraping.chapter.completed"),
		TopicChapterFailed:    getEnv("TOPIC_CHAPTER_FAILED", "scraping.chapter.failed"),
		TopicBookCompleted:    getEnv("TOPIC_BOOK_COMPLETED", "scraping.new-book.completed"),
		TopicUpdateBookCompleted: getEnv("TOPIC_UPDATE_BOOK_COMPLETED", "scraping.update-book.completed"),
		TopicCoversCompleted:  getEnv("TOPIC_COVERS_COMPLETED", "scraping.covers.completed"),
		TopicImagesCompleted:  getEnv("TOPIC_IMAGES_COMPLETED", "scraping.images.completed"),
		TopicDLQ:             getEnv("TOPIC_DLQ", "scraping.dlq"),
		TopicBookFailed:      getEnv("TOPIC_BOOK_FAILED", "scraping.book.failed"),
		ProcessedImagesBucket: getEnv("PROCESSED_IMAGES_BUCKET", "books"),
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// validate rejects configuration values that would otherwise fail silently
// or hang the service (e.g. BROWSER_POOL_SIZE=0 makes the browser-pool
// semaphore permanently unacquirable).
func (c Config) validate() error {
	if c.BrowserPoolSize < 1 {
		return fmt.Errorf("BROWSER_POOL_SIZE must be >= 1, got %d", c.BrowserPoolSize)
	}
	if c.CacheMaxSizeMB < 1 {
		return fmt.Errorf("CACHE_MAX_SIZE_MB must be >= 1, got %d", c.CacheMaxSizeMB)
	}
	if c.KafkaWriteTimeout < 1 {
		return fmt.Errorf("KAFKA_WRITE_TIMEOUT must be >= 1, got %d", c.KafkaWriteTimeout)
	}
	if c.KafkaReadTimeout < 1 {
		return fmt.Errorf("KAFKA_READ_TIMEOUT must be >= 1, got %d", c.KafkaReadTimeout)
	}
	if c.KafkaRequiredAcks != -1 && c.KafkaRequiredAcks != 0 && c.KafkaRequiredAcks != 1 {
		return fmt.Errorf("KAFKA_REQUIRED_ACKS must be -1, 0, or 1, got %d", c.KafkaRequiredAcks)
	}
	if len(c.KafkaBrokers) == 0 || c.KafkaBrokers[0] == "" {
		return fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	if c.ShutdownGracePeriod < 1*time.Second {
		return fmt.Errorf("SHUTDOWN_GRACE_PERIOD must be >= 1s, got %s", c.ShutdownGracePeriod)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return d, nil
}
