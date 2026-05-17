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
	"github.com/google/uuid"
)

func main() {
	cfg := config.LoadConfig()

	// Storage
	log.Printf("Initializing S3: Endpoint=%s, Region=%s, SSL=%v", cfg.S3Endpoint, cfg.S3Region, cfg.S3UseSSL)
	s3, err := storage.NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		log.Fatalf("Failed to init S3: %v", err)
	}

	// Browser Pool
	pool, err := scraper.NewBrowserPool(cfg.BrowserURL, cfg.BrowserPoolSize)
	if err != nil {
		log.Fatalf("Failed to init Browser Pool: %v", err)
	}
	defer pool.Close()

	// Scraper
	engine := scraper.NewScraper(pool, cfg.CacheMaxSizeMB*1024*1024)

	// Kafka
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaWriteTimeout, cfg.KafkaRequiredAcks)
	defer producer.Close()

	chapterConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicChapterRequested, cfg.KafkaReadTimeout)
	defer chapterConsumer.Close()

	updateBookConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicUpdateBookRequested, cfg.KafkaReadTimeout)
	defer updateBookConsumer.Close()

	newBookConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicNewBookRequested, cfg.KafkaReadTimeout)
	defer newBookConsumer.Close()

	coversConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicCoversRequested, cfg.KafkaReadTimeout)
	defer coversConsumer.Close()

	imagesConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicImagesRequested, cfg.KafkaReadTimeout)
	defer imagesConsumer.Close()

	testConsumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicTestRequested, cfg.KafkaReadTimeout)
	defer testConsumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Scraper Microservice started...")

	// Launch handlers
	go handleChapterRequests(ctx, chapterConsumer, producer, engine, s3, cfg)
	go handleUpdateBookRequests(ctx, updateBookConsumer, producer, engine, cfg)
	go handleNewBookRequests(ctx, newBookConsumer, producer, engine, cfg)
	go handleCoversRequests(ctx, coversConsumer, producer, engine, s3, cfg)
	go handleImagesRequests(ctx, imagesConsumer, producer, engine, s3, cfg)
	go handleTestRequests(ctx, testConsumer, producer, engine)

	<-ctx.Done()
	log.Println("Shutting down...")
}

func handleChapterRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, cfg config.Config) {
	// Semaphore to limit concurrency (matching browser pool size roughly)
	sem := make(chan struct{}, 5) 

	for {
		var req models.ScrapingChapterRequest
		_, err := consumer.FetchMessage(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		sem <- struct{}{} // Acquire slot
		go func(req models.ScrapingChapterRequest) {
			defer func() { <-sem }() // Release slot

			log.Printf("Processing chapter request: %s (Job: %s)", req.ChapterID, req.JobID)

			title, imageUrls, results, cleanup, err := engine.ScrapeChapter(ctx, req)
			if err != nil {
				log.Printf("Scrape failed: %v", err)
				producer.Publish(ctx, cfg.TopicChapterFailed, models.ScrapingChapterFailed{
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

			producer.Publish(ctx, cfg.TopicChapterPagesExtracted, models.ScrapingChapterPagesExtracted{
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
				_, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg")
				if err != nil {
					log.Printf("Upload failed (index %d): %v", r.Index, err)
					continue
				}

				// Publish image processing request IMMEDIATELY
				producer.Publish(ctx, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawBucket:    req.UploadTarget.Bucket,
					RawPath:      rawName,
					TargetBucket: "books",
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				// Store minimal metadata for final sorting
				processedImages = append(processedImages, processedImage{
					Index: r.Index,
					Image: models.ScrapedImage{
						OriginalURL: imageUrls[r.Index],
						Path:        rawName,
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

			producer.Publish(ctx, cfg.TopicChapterCompleted, models.ScrapingChapterCompleted{
				JobID:        req.JobID,
				ChapterID:    req.ChapterID,
				ScrapedTitle: title,
				TotalImages:  len(scImages),
				Images:       scImages,
			})
			
			log.Printf("Chapter completed: %s", req.ChapterID)
		}(req)
	}
}

func handleUpdateBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicUpdateBookRequested)
	for {
		var req models.ScrapingUpdateBookRequest
		_, err := consumer.FetchMessage(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching update-book message: %v", err)
			continue
		}

		log.Printf("Processing update-book request: %s (Job: %s)", req.BookID, req.JobID)
		result, err := engine.ScrapeUpdateBook(ctx, req)
		if err != nil {
			log.Printf("Update book scrape failed: %v", err)
			continue
		}

		producer.Publish(ctx, cfg.TopicUpdateBookCompleted, result)
		log.Printf("Update book completed: %s", req.BookID)
	}
}

func handleNewBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicNewBookRequested)
	for {
		var req models.ScrapingNewBookRequest
		_, err := consumer.FetchMessage(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching new-book message: %v", err)
			continue
		}

		log.Printf("Processing new-book request (Job: %s)", req.JobID)
		result, err := engine.ScrapeNewBook(ctx, req)
		if err != nil {
			log.Printf("New book scrape failed: %v", err)
			continue
		}

		producer.Publish(ctx, cfg.TopicBookCompleted, result)
		log.Printf("New book completed (Job: %s)", req.JobID)
	}
}

func handleCoversRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicCoversRequested)
	for {
		msg, err := consumer.ReadRawMessage(ctx)
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
			continue
		}

		log.Printf("Processing covers request: %s (Job: %s, Covers: %d)", req.BookID, req.JobID, len(req.Covers))

		go func(req models.ScrapingCoversRequest) {
			results, cleanup := engine.ScrapeCovers(ctx, req)
			defer cleanup()
			
			s3Paths := make([]string, 0, len(req.Covers))
			for r := range results {
				if r.Error != nil {
					log.Printf("Cover download failed (index %d): %v", r.Index, r.Error)
					continue
				}

				// Path pattern: books/<prefix>/<uuid>.jpg
				id, _ := uuid.NewV7()
				imgID := id.String()
				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				_, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg")
				if err != nil {
					log.Printf("Cover upload failed: %v", err)
					continue
				}

				s3Paths = append(s3Paths, rawName)

				producer.Publish(ctx, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawBucket:    req.UploadTarget.Bucket,
					RawPath:      rawName,
					TargetBucket: "books",
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				// Early memory release for GC
				r.Data = nil
			}

			// Emit final completion event
			producer.Publish(ctx, cfg.TopicCoversCompleted, models.ScrapingCoversCompleted{
				JobID:   req.JobID,
				BookID:  req.BookID,
				Results: s3Paths,
			})

			log.Printf("Covers request completed: %s (Processed: %d/%d)", req.JobID, len(s3Paths), len(req.Covers))
		}(req)
	}
}

func handleImagesRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, cfg config.Config) {
	log.Printf("Starting listener for topic: %s", cfg.TopicImagesRequested)
	for {
		msg, err := consumer.ReadRawMessage(ctx)
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
			continue
		}

		log.Printf("Processing images request (Job: %s, Images: %d)", req.JobID, len(req.ImageURLs))

		go func(req models.ScrapingImagesRequest) {
			results, cleanup := engine.ScrapeImages(ctx, req)
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

				_, err := s3.Upload(ctx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg")
				if err != nil {
					log.Printf("Image upload failed: %v", err)
					continue
				}

				producer.Publish(ctx, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawBucket:    req.UploadTarget.Bucket,
					RawPath:      rawName,
					TargetBucket: "books",
					TargetPath:   targetName,
					IsBackfill:   false,
				})

				urlMap[req.ImageURLs[r.Index]] = rawName
				count++

				// Early memory release for GC
				r.Data = nil
			}

			// Emit final completion event for Images batch
			producer.Publish(ctx, cfg.TopicImagesCompleted, models.ScrapingImagesCompleted{
				JobID:    req.JobID,
				EntityID: req.EntityID,
				Source:   "CHAPTER",
				Format:   "images",
				URLMap:   urlMap,
			})

			log.Printf("Images request completed: %s (Processed: %d/%d)", req.JobID, count, len(req.ImageURLs))
		}(req)
	}
}

func handleTestRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper) {
	for {
		var req models.ScrapingTestRequest
		_, err := consumer.FetchMessage(ctx, &req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		res, err := engine.ExecuteTestScript(ctx, req)
		log.Printf("Test Result: %v (Error: %v)", res, err)
	}
}

func generateS3Keys(pathPrefix, imgID string) (rawName string, targetName string) {
	prefix := imgID[len(imgID)-2:]
	rawName = fmt.Sprintf("%s/%s.jpg", prefix, imgID)
	targetName = fmt.Sprintf("%s/%s.webp", prefix, imgID)
	return rawName, targetName
}
