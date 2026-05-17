package config

import (
	"fmt"
	"os"
	"strings"

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

	// Kafka General
	KafkaWriteTimeout int // seconds
	KafkaReadTimeout  int // seconds
	KafkaRequiredAcks int
	KafkaDebug        bool

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
	}


func LoadConfig() Config {
	_ = godotenv.Load()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	poolSize := 2
	if ps := os.Getenv("BROWSER_POOL_SIZE"); ps != "" {
		fmt.Sscanf(ps, "%d", &poolSize)
	}

	var cacheSize int64 = 100
	if cs := os.Getenv("CACHE_MAX_SIZE_MB"); cs != "" {
		fmt.Sscanf(cs, "%d", &cacheSize)
	}

	writeTimeout := 10
	if wt := os.Getenv("KAFKA_WRITE_TIMEOUT"); wt != "" {
		fmt.Sscanf(wt, "%d", &writeTimeout)
	}

	readTimeout := 10
	if rt := os.Getenv("KAFKA_READ_TIMEOUT"); rt != "" {
		fmt.Sscanf(rt, "%d", &readTimeout)
	}

	acks := 1 // default to acks=1 (leader only)
	if a := os.Getenv("KAFKA_REQUIRED_ACKS"); a != "" {
		fmt.Sscanf(a, "%d", &acks)
	}

	endpoint := getEnv("STORAGE_ENDPOINT", "localhost:9000")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	return Config{
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

		KafkaWriteTimeout: writeTimeout,
		KafkaReadTimeout:  readTimeout,
		KafkaRequiredAcks: acks,
		KafkaDebug:        os.Getenv("KAFKA_DEBUG") == "true",

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
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
