package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/gatuno/scraper/internal/config"
	"github.com/gatuno/scraper/internal/kafka"
	"github.com/gatuno/scraper/internal/models"
	"github.com/gatuno/scraper/internal/scraper"
	"github.com/gatuno/scraper/internal/storage"
	"github.com/gatuno/scraper/internal/ratelimit"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.LoadConfig()

	// Storage
	log.Printf("Initializing S3: Endpoint=%s, Region=%s, SSL=%v", cfg.S3Endpoint, cfg.S3Region, cfg.S3UseSSL)
	s3, err := storage.NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		log.Fatalf("Failed to init S3: %v", err)
	}

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       0,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer rdb.Close()

	// Browser Pool
	pool, err := scraper.NewBrowserPool(cfg.BrowserURL, cfg.BrowserPoolSize)
	if err != nil {
		log.Fatalf("Failed to init Browser Pool: %v", err)
	}
	defer pool.Close()

	limiter := ratelimit.NewRedisRateLimiter(rdb)
	semaphore := ratelimit.NewRedisSemaphore(rdb, 2) // Max 2 browsers per domain globally

	// Scraper
	engine := scraper.NewScraper(pool, cfg.CacheMaxSizeMB*1024*1024, limiter, semaphore)

	// Kafka
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaWriteTimeout, cfg.KafkaRequiredAcks)
	defer producer.Close()

	chapterConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-chapter", cfg.TopicChapterRequested, cfg.KafkaReadTimeout)
	defer chapterConsumer.Close()

	updateBookConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-updatebook", cfg.TopicUpdateBookRequested, cfg.KafkaReadTimeout)
	defer updateBookConsumer.Close()

	newBookConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-newbook", cfg.TopicNewBookRequested, cfg.KafkaReadTimeout)
	defer newBookConsumer.Close()

	coversConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-covers", cfg.TopicCoversRequested, cfg.KafkaReadTimeout)
	defer coversConsumer.Close()

	imagesConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-images", cfg.TopicImagesRequested, cfg.KafkaReadTimeout)
	defer imagesConsumer.Close()

	testConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-test", cfg.TopicTestRequested, cfg.KafkaReadTimeout)
	defer testConsumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Scraper Microservice started...")

	// Launch handlers
	go handleChapterRequests(ctx, chapterConsumer, producer, engine, s3, rdb, cfg)
	go handleUpdateBookRequests(ctx, updateBookConsumer, producer, engine, rdb, cfg)
	go handleNewBookRequests(ctx, newBookConsumer, producer, engine, rdb, cfg)
	go handleCoversRequests(ctx, coversConsumer, producer, engine, s3, rdb, cfg)
	go handleImagesRequests(ctx, imagesConsumer, producer, engine, s3, rdb, cfg)
	go handleTestRequests(ctx, testConsumer, engine)

	<-ctx.Done()
	log.Println("Shutting down...")
}

// publishOrLog publishes a message to a Kafka topic and logs a critical error
// if delivery fails. Call sites must NOT silently discard the error.
func publishOrLog(ctx context.Context, producer *kafka.Producer, topic string, msg interface{}) {
	if err := producer.Publish(ctx, topic, msg); err != nil {
		log.Printf("CRITICAL: failed to publish to topic %s: %v", topic, err)
	}
}

// sendToDLQ routes an unprocessable message to the dead-letter queue topic so
// it can be inspected and replayed later without blocking the main consumer.
func sendToDLQ(ctx context.Context, producer *kafka.Producer, dlqTopic, originalTopic string, payload []byte, reason error) {
	dlqMsg := models.DeadLetterMessage{
		OriginalTopic: originalTopic,
		Payload:       string(payload),
		Error:         reason.Error(),
	}
	if err := producer.Publish(ctx, dlqTopic, dlqMsg); err != nil {
		log.Printf("CRITICAL: failed to send message to DLQ %s: %v (original error: %v)", dlqTopic, err, reason)
	}
}

// commitOrLog commits a Kafka message offset and logs a critical error on failure.
func commitOrLog(ctx context.Context, consumer *kafka.Consumer, msg kafka.Message) {
	if err := consumer.Commit(ctx, msg); err != nil {
		log.Printf("CRITICAL: failed to commit Kafka offset: %v", err)
	}
}

func fetchWebsiteConfig(ctx context.Context, rdb *redis.Client, websiteID string) (models.WebsiteConfig, error) {
	var wc models.WebsiteConfig
	if websiteID == "" {
		return wc, fmt.Errorf("empty websiteId")
	}
	key := fmt.Sprintf("website:config:%s", websiteID)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return wc, fmt.Errorf("failed to get config from redis: %w", err)
	}
	if err := json.Unmarshal([]byte(val), &wc); err != nil {
		return wc, fmt.Errorf("failed to unmarshal website config: %w", err)
	}
	return wc, nil
}

func handleChapterRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, rdb *redis.Client, cfg config.Config) {
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching chapter message: %v", err)
			continue
		}

		var req models.ScrapingChapterRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("Error deserializing chapter message: %v | Raw: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicChapterRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		func(req models.ScrapingChapterRequest, msg kafka.Message) {
			// Commit the offset after the processing finishes (at-least-once).
			// By doing this synchronously, we guarantee offsets advance strictly in order.
			defer commitOrLog(ctx, consumer, msg)

			log.Printf("Processing chapter request: %s (Job: %s)", req.ChapterID, req.JobID)

			wc, err := fetchWebsiteConfig(ctx, rdb, req.WebsiteID)
			if err != nil {
				log.Printf("Failed to fetch website config: %v", err)
				sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicChapterRequested, msg.Value, err)
				return
			}

			title, imageUrls, results, cleanup, err := engine.ScrapeChapter(ctx, req, wc)
			if err != nil {
				log.Printf("Scrape failed: %v", err)
				publishOrLog(ctx, producer, cfg.TopicChapterFailed, models.ScrapingChapterFailed{
					JobID:     req.JobID,
					ChapterID: req.ChapterID,
					Error:     "SCRAPE_FAILED",
					Message:   err.Error(),
				})
				return
			}
			defer cleanup()

			// Emit intermediate event for Fast-Track reading
			intermediateImages := make([]models.ScrapedImage, 0, len(imageUrls))
			for _, url := range imageUrls {
				intermediateImages = append(intermediateImages, models.ScrapedImage{
					OriginalURL: url,
				})
			}

			publishOrLog(ctx, producer, cfg.TopicChapterPagesExtracted, models.ScrapingChapterPagesExtracted{
				JobID:        req.JobID,
				ChapterID:    req.ChapterID,
				ScrapedTitle: title,
				TotalImages:  len(imageUrls),
				Images:       intermediateImages,
			})

			type processedImage struct {
				Index int
				Image models.ScrapedImage
			}
			processedImages := make([]processedImage, 0, len(imageUrls))

			for r := range results {
				if r.Error != nil {
					log.Printf("Image download failed (index %d): %v", r.Index, r.Error)
					continue
				}

				// Generate unique ID for the image (UUIDv7 for better time-sorting/locality)
				id, _ := uuid.NewV7()
				imgID := id.String()

				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				log.Printf("Attempting upload (index %d): bucket=%s, object=%s, size=%d bytes", r.Index, req.UploadTarget.Bucket, rawName, len(r.Data))
				if _, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg"); err != nil {
					log.Printf("Upload failed (index %d): %v", r.Index, err)
					r.Data = nil
					continue
				}

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				// Publish image processing request IMMEDIATELY
				publishOrLog(ctx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawPath:      rawPathWithBucket,
					TargetBucket: cfg.ProcessedImagesBucket,
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				// Store minimal metadata for final sorting
				processedImages = append(processedImages, processedImage{
					Index: r.Index,
					Image: models.ScrapedImage{
						OriginalURL: imageUrls[r.Index],
						Path:        rawPathWithBucket,
					},
				})

				// Early memory release for GC
				r.Data = nil
			}

			// Sort only the metadata by index to ensure correct order
			sort.Slice(processedImages, func(i, j int) bool {
				return processedImages[i].Index < processedImages[j].Index
			})

			scImages := make([]models.ScrapedImage, 0, len(processedImages))
			for _, pi := range processedImages {
				scImages = append(scImages, pi.Image)
			}

			publishOrLog(ctx, producer, cfg.TopicChapterCompleted, models.ScrapingChapterCompleted{
				JobID:        req.JobID,
				ChapterID:    req.ChapterID,
				ScrapedTitle: title,
				TotalImages:  len(scImages),
				Images:       scImages,
			})

			log.Printf("Chapter completed: %s", req.ChapterID)
		}(req, msg)
	}
}

func handleUpdateBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, rdb *redis.Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicUpdateBookRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching update-book message: %v", err)
			continue
		}

		var req models.ScrapingUpdateBookRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("Error deserializing update-book message: %v | Raw: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicUpdateBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		log.Printf("Processing update-book request: %s (Job: %s)", req.BookID, req.JobID)
		
		wc, err := fetchWebsiteConfig(ctx, rdb, req.WebsiteID)
		if err != nil {
			log.Printf("Failed to fetch website config: %v | Raw Message: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicUpdateBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		result, err := engine.ScrapeUpdateBook(ctx, req, wc)
		if err != nil {
			log.Printf("Update book scrape failed: %v", err)
			publishOrLog(ctx, producer, cfg.TopicBookFailed, models.ScrapingBookFailed{
				JobID:   req.JobID,
				BookID:  req.BookID,
				Error:   "SCRAPE_FAILED",
				Message: err.Error(),
			})
			commitOrLog(ctx, consumer, msg)
			continue
		}

		publishOrLog(ctx, producer, cfg.TopicUpdateBookCompleted, result)
		log.Printf("Update book completed: %s", req.BookID)
		commitOrLog(ctx, consumer, msg)
	}
}

func handleNewBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, rdb *redis.Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicNewBookRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching new-book message: %v", err)
			continue
		}

		var req models.ScrapingNewBookRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("Error deserializing new-book message: %v | Raw: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicNewBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		log.Printf("Processing new-book request (Job: %s)", req.JobID)
		
		wc, err := fetchWebsiteConfig(ctx, rdb, req.WebsiteID)
		if err != nil {
			log.Printf("Failed to fetch website config: %v", err)
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicNewBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		result, err := engine.ScrapeNewBook(ctx, req, wc)
		if err != nil {
			log.Printf("New book scrape failed: %v", err)
			publishOrLog(ctx, producer, cfg.TopicBookFailed, models.ScrapingBookFailed{
				JobID:   req.JobID,
				Error:   "SCRAPE_FAILED",
				Message: err.Error(),
			})
			commitOrLog(ctx, consumer, msg)
			continue
		}

		publishOrLog(ctx, producer, cfg.TopicBookCompleted, result)
		log.Printf("New book completed (Job: %s)", req.JobID)
		commitOrLog(ctx, consumer, msg)
	}
}

func handleCoversRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, rdb *redis.Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicCoversRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error reading covers message: %v", err)
			continue
		}

		var req models.ScrapingCoversRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("Error unmarshaling covers message: %v | Raw: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicCoversRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		log.Printf("Processing covers request: %s (Job: %s, Covers: %d)", req.BookID, req.JobID, len(req.Covers))

		wc, err := fetchWebsiteConfig(ctx, rdb, req.WebsiteID)
		if err != nil {
			log.Printf("Failed to fetch website config: %v", err)
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicCoversRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		func(req models.ScrapingCoversRequest, msg kafka.Message) {
			defer commitOrLog(ctx, consumer, msg)

			results, cleanup := engine.ScrapeCovers(ctx, req, wc)
			defer cleanup()

			s3Paths := make([]string, 0, len(req.Covers))
			for r := range results {
				if r.Error != nil {
					log.Printf("Cover download failed (index %d): %v", r.Index, r.Error)
					continue
				}

				// Path pattern: <prefix>/<shard>/<uuid>.jpg
				id, _ := uuid.NewV7()
				imgID := id.String()
				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				if _, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg"); err != nil {
					log.Printf("Cover upload failed: %v", err)
					r.Data = nil
					continue
				}

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				s3Paths = append(s3Paths, rawPathWithBucket)

				publishOrLog(ctx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawPath:      rawPathWithBucket,
					TargetBucket: cfg.ProcessedImagesBucket,
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				// Early memory release for GC
				r.Data = nil
			}

			// Emit final completion event
			publishOrLog(ctx, producer, cfg.TopicCoversCompleted, models.ScrapingCoversCompleted{
				JobID:        req.JobID,
				BookID:       req.BookID,
				Results:      s3Paths,
			})

			log.Printf("Covers request completed: %s (Processed: %d/%d)", req.JobID, len(s3Paths), len(req.Covers))
		}(req, msg)
	}
}

func handleImagesRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, rdb *redis.Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicImagesRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error reading images message: %v", err)
			continue
		}

		var req models.ScrapingImagesRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("Error unmarshaling images message: %v | Raw: %s", err, string(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicImagesRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		log.Printf("Processing images request (Job: %s, Images: %d)", req.JobID, len(req.ImageURLs))

		wc, err := fetchWebsiteConfig(ctx, rdb, req.WebsiteID)
		if err != nil {
			log.Printf("Failed to fetch website config: %v", err)
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicImagesRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		func(req models.ScrapingImagesRequest, msg kafka.Message) {
			defer commitOrLog(ctx, consumer, msg)

			results, cleanup := engine.ScrapeImages(ctx, req, wc)
			defer cleanup()

			urlMap := make(map[string]string)
			count := 0
			for r := range results {
				if r.Error != nil {
					log.Printf("Image download failed (index %d): %v", r.Index, r.Error)
					continue
				}

				id, _ := uuid.NewV7()
				imgID := id.String()
				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				if _, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg"); err != nil {
					log.Printf("Image upload failed: %v", err)
					r.Data = nil
					continue
				}

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				publishOrLog(ctx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawPath:      rawPathWithBucket,
					TargetBucket: cfg.ProcessedImagesBucket,
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				urlMap[req.ImageURLs[r.Index]] = rawPathWithBucket
				count++

				// Early memory release for GC
				r.Data = nil
			}

			// Emit final completion event for Images batch
			publishOrLog(ctx, producer, cfg.TopicImagesCompleted, models.ScrapingImagesCompleted{
				JobID:        req.JobID,
				EntityID:     req.EntityID,
				Source:       "CHAPTER",
				Format:       "images",
				URLMap:       urlMap,
			})

			log.Printf("Images request completed: %s (Processed: %d/%d)", req.JobID, count, len(req.ImageURLs))
		}(req, msg)
	}
}

func handleTestRequests(ctx context.Context, consumer *kafka.Consumer, engine *scraper.Scraper) {
	for {
		var req models.ScrapingTestRequest
		msg, err := consumer.FetchMessage(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching test message: %v", err)
			continue
		}

		res, err := engine.ExecuteTestScript(ctx, req)
		log.Printf("Test Result: %v (Error: %v)", res, err)

		// Commit after execution regardless of result, test jobs are best-effort.
		commitOrLog(ctx, consumer, msg)
	}
}

// generateS3Keys builds the raw and processed S3 object paths for an image.
// The path uses the last 2 characters of the UUID as a shard prefix to avoid
// hot-spotting in object storage, and honours pathPrefix when set.
//
//   - rawName:    "<pathPrefix>/<shard>/<uuid>.jpg"  (uploaded immediately)
//   - targetName: "<pathPrefix>/<shard>/<uuid>.webp" (written by the image processor)
func generateS3Keys(pathPrefix, imgID string) (rawName string, targetName string) {
	shard := imgID[len(imgID)-2:]
	if pathPrefix != "" {
		rawName = fmt.Sprintf("%s/%s/%s.jpg", pathPrefix, shard, imgID)
		targetName = fmt.Sprintf("%s/%s/%s.webp", pathPrefix, shard, imgID)
	} else {
		rawName = fmt.Sprintf("%s/%s.jpg", shard, imgID)
		targetName = fmt.Sprintf("%s/%s.webp", shard, imgID)
	}
	return rawName, targetName
}
