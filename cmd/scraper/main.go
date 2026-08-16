package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gatuno/scraper/internal/config"
	"github.com/gatuno/scraper/internal/kafka"
	"github.com/gatuno/scraper/internal/metrics"
	"github.com/gatuno/scraper/internal/models"
	"github.com/gatuno/scraper/internal/obs"
	"github.com/gatuno/scraper/internal/scraper"
	"github.com/gatuno/scraper/internal/storage"
	"github.com/gatuno/scraper/internal/ratelimit"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	obs.Setup(obs.ParseLevel(cfg.LogLevel), cfg.LogFormat)

	// Storage
	slog.Info("initializing s3", "endpoint", cfg.S3Endpoint, "region", cfg.S3Region, "ssl", cfg.S3UseSSL)
	s3, err := storage.NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3UseSSL)
	if err != nil {
		slog.Error("failed to init s3", "error", err)
		os.Exit(1)
	}

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       0,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Browser Pool
	pool, err := scraper.NewBrowserPool(cfg.BrowserURL, cfg.BrowserPoolSize)
	if err != nil {
		slog.Error("failed to init browser pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	limiter := ratelimit.NewRedisRateLimiter(rdb)
	semaphore := ratelimit.NewRedisSemaphore(rdb, 2) // Max 2 browsers per domain globally

	// Scraper
	engine := scraper.NewScraper(pool, cfg.CacheMaxSizeMB*1024*1024, limiter, semaphore)

	// Kafka
	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaWriteTimeout, cfg.KafkaRequiredAcks, cfg.KafkaAllowAutoTopicCreation)
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

	opsServer := startOpsServer(cfg, rdb, pool)
	defer opsServer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("scraper microservice started")

	// Launch handlers
	go handleChapterRequests(ctx, chapterConsumer, producer, engine, s3, rdb, cfg)
	go handleUpdateBookRequests(ctx, updateBookConsumer, producer, engine, rdb, cfg)
	go handleNewBookRequests(ctx, newBookConsumer, producer, engine, rdb, cfg)
	go handleCoversRequests(ctx, coversConsumer, producer, engine, s3, rdb, cfg)
	go handleImagesRequests(ctx, imagesConsumer, producer, engine, s3, rdb, cfg)
	go handleTestRequests(ctx, testConsumer, engine)

	<-ctx.Done()
	slog.Info("shutting down")
}

// publishOrLog publishes a message to a Kafka topic and logs a critical error
// if delivery fails. Call sites must NOT silently discard the error.
func publishOrLog(ctx context.Context, producer *kafka.Producer, topic string, msg interface{}) {
	if err := producer.Publish(ctx, topic, msg); err != nil {
		metrics.KafkaPublishFailuresTotal.WithLabelValues(topic).Inc()
		obs.From(ctx).Error("failed to publish", "topic", topic, "error", err)
	}
}

// publishKeyedOrLog is publishOrLog with an explicit partition key. Use it
// for events scoped to the same entity (chapter/book/job) so they land on
// the same partition and downstream consumers observe them in order.
func publishKeyedOrLog(ctx context.Context, producer *kafka.Producer, topic, key string, msg interface{}) {
	if err := producer.PublishKeyed(ctx, topic, key, msg); err != nil {
		metrics.KafkaPublishFailuresTotal.WithLabelValues(topic).Inc()
		obs.From(ctx).Error("failed to publish", "topic", topic, "error", err)
	}
}

// payloadFingerprint summarizes a Kafka payload for logging without ever
// exposing its content. Request payloads embed WebsiteConfig, which carries
// live session cookies, Authorization headers and proxy credentials
// (OBS-03) that must never reach stdout or a log aggregator.
func payloadFingerprint(payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%s bytes:%d", hex.EncodeToString(sum[:8]), len(payload))
}

// redactPayload strips the websiteConfig field before a payload is
// persisted to the DLQ topic, where it would otherwise sit in the clear for
// the topic's retention period (OBS-03). Payloads that can't be parsed as a
// JSON object are never stored raw — only their fingerprint is kept.
//
// Key matching is case-insensitive (strings.EqualFold), matching
// encoding/json's own struct-unmarshal behavior: Unmarshal binds a JSON
// object key to a struct field case-insensitively when no exact match
// exists, so the application processes "WebsiteConfig", "websiteconfig",
// etc. the same as "websiteConfig". An exact-match-only redaction here
// would miss those variants and leak cookies/headers/proxy credentials to
// the DLQ whenever the top-level Unmarshal failed for an unrelated reason.
func redactPayload(payload []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return payloadFingerprint(payload)
	}
	for key := range fields {
		if strings.EqualFold(key, "websiteConfig") {
			fields[key] = json.RawMessage(`"REDACTED"`)
		}
	}
	redacted, err := json.Marshal(fields)
	if err != nil {
		return payloadFingerprint(payload)
	}
	return string(redacted)
}

// sendToDLQ routes an unprocessable message to the dead-letter queue topic so
// it can be inspected and replayed later without blocking the main consumer.
func sendToDLQ(ctx context.Context, producer *kafka.Producer, dlqTopic, originalTopic string, payload []byte, reason error) {
	metrics.DLQTotal.WithLabelValues(originalTopic).Inc()
	metrics.JobsTotal.WithLabelValues(originalTopic, "dlq").Inc()
	dlqMsg := models.DeadLetterMessage{
		OriginalTopic: originalTopic,
		Payload:       redactPayload(payload),
		Error:         reason.Error(),
	}
	if err := producer.Publish(ctx, dlqTopic, dlqMsg); err != nil {
		metrics.KafkaPublishFailuresTotal.WithLabelValues(dlqTopic).Inc()
		obs.From(ctx).Error("failed to send message to dlq", "dlq_topic", dlqTopic, "error", err, "original_error", reason)
	}
}

// commitOrLog commits a Kafka message offset and logs a critical error on failure.
func commitOrLog(ctx context.Context, consumer *kafka.Consumer, msg kafka.Message) {
	if err := consumer.Commit(ctx, msg); err != nil {
		obs.From(ctx).Error("failed to commit kafka offset", "error", err)
	}
}

func fetchWebsiteConfig(ctx context.Context, rdb *redis.Client, websiteID string, embedded *models.WebsiteConfig) (models.WebsiteConfig, error) {
	if embedded != nil {
		return *embedded, nil
	}
	var wc models.WebsiteConfig
	if websiteID == "" {
		return wc, fmt.Errorf("empty websiteId and no embedded config")
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
			obs.From(ctx).Warn("error fetching chapter message", "error", err)
			continue
		}

		var req models.ScrapingChapterRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			obs.From(ctx).Error("error deserializing chapter message", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicChapterRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		logger := slog.With(
			"job_id", req.JobID,
			"chapter_id", req.ChapterID,
			"website_id", req.WebsiteID,
			"topic", cfg.TopicChapterRequested,
		)
		jobCtx := obs.With(ctx, logger)

		func(req models.ScrapingChapterRequest, msg kafka.Message) {
			// Commit the offset after the processing finishes (at-least-once).
			// By doing this synchronously, we guarantee offsets advance strictly in order.
			defer commitOrLog(jobCtx, consumer, msg)

			obs.From(jobCtx).Info("processing chapter request")

			jobStart := time.Now()

			wc, err := fetchWebsiteConfig(jobCtx, rdb, req.WebsiteID, req.WebsiteConfig)
			if err != nil {
				obs.From(jobCtx).Error("failed to fetch website config", "error", err)
				sendToDLQ(jobCtx, producer, cfg.TopicDLQ, cfg.TopicChapterRequested, msg.Value, err)
				return
			}

			timings := &scraper.PhaseTimings{}
			title, imageUrls, results, cleanup, err := engine.ScrapeChapter(jobCtx, req, wc, timings)
			if err != nil {
				metrics.JobsTotal.WithLabelValues(cfg.TopicChapterRequested, "failed").Inc()
				metrics.JobDurationSeconds.WithLabelValues(cfg.TopicChapterRequested).Observe(time.Since(jobStart).Seconds())
				obs.From(jobCtx).Error("scrape failed", "error", err,
					"sem_wait_ms", timings.SemWaitMs, "nav_ms", timings.NavMs, "prepare_ms", timings.PrepareMs)
				publishKeyedOrLog(jobCtx, producer, cfg.TopicChapterFailed, req.ChapterID, models.ScrapingChapterFailed{
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

			publishKeyedOrLog(jobCtx, producer, cfg.TopicChapterPagesExtracted, req.ChapterID, models.ScrapingChapterPagesExtracted{
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

			downloadUploadStart := time.Now()
			var uploadMs int64
			var imagesFailed int

			for r := range results {
				if r.Error != nil {
					imagesFailed++
					metrics.ImagesTotal.WithLabelValues("download_failed").Inc()
					obs.From(jobCtx).Debug("image download failed", "image_index", r.Index, "error", r.Error)
					continue
				}

				// Generate unique ID for the image (UUIDv7 for better time-sorting/locality)
				imgID, err := newImageID()
				if err != nil {
					imagesFailed++
					obs.From(jobCtx).Warn("failed to generate image id", "image_index", r.Index, "error", err)
					r.Data = nil
					continue
				}

				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				obs.From(jobCtx).Debug("attempting upload", "image_index", r.Index, "bucket", req.UploadTarget.Bucket, "object", rawName, "bytes", len(r.Data))
				uploadStart := time.Now()
				_, uploadErr := s3.Upload(jobCtx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg")
				uploadMs += time.Since(uploadStart).Milliseconds()
				if uploadErr != nil {
					imagesFailed++
					metrics.ImagesTotal.WithLabelValues("upload_failed").Inc()
					obs.From(jobCtx).Error("upload failed", "image_index", r.Index, "error", uploadErr)
					r.Data = nil
					continue
				}
				metrics.ImagesTotal.WithLabelValues("ok").Inc()
				metrics.ImageBytesTotal.Add(float64(len(r.Data)))

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				// Publish image processing request IMMEDIATELY
				publishOrLog(jobCtx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
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

			publishKeyedOrLog(jobCtx, producer, cfg.TopicChapterCompleted, req.ChapterID, models.ScrapingChapterCompleted{
				JobID:        req.JobID,
				ChapterID:    req.ChapterID,
				ScrapedTitle: title,
				TotalImages:  len(scImages),
				Images:       scImages,
			})

			// downloadUploadMs covers both the concurrent download workers and the
			// serial upload loop, which overlap (uploads start as soon as the first
			// result arrives). uploadMs is measured directly around each s3.Upload
			// call; downloadMs is the remainder — an approximation, not a clean
			// wall-clock split, since the two phases are pipelined rather than
			// sequential.
			downloadUploadMs := time.Since(downloadUploadStart).Milliseconds()
			downloadMs := downloadUploadMs - uploadMs
			degraded := imagesFailed > 0

			for phase, ms := range map[string]int64{
				"sem_wait": timings.SemWaitMs,
				"nav":      timings.NavMs,
				"prepare":  timings.PrepareMs,
				"scroll":   timings.ScrollMs,
				"extract":  timings.ExtractMs,
				"download": downloadMs,
				"upload":   uploadMs,
			} {
				metrics.PhaseDurationSeconds.WithLabelValues(phase).Observe(float64(ms) / 1000)
			}
			metrics.JobsTotal.WithLabelValues(cfg.TopicChapterRequested, "ok").Inc()
			metrics.JobDurationSeconds.WithLabelValues(cfg.TopicChapterRequested).Observe(time.Since(jobStart).Seconds())

			obs.From(jobCtx).Info("chapter.completed",
				"duration_ms", time.Since(jobStart).Milliseconds(),
				"sem_wait_ms", timings.SemWaitMs,
				"nav_ms", timings.NavMs,
				"prepare_ms", timings.PrepareMs,
				"scroll_ms", timings.ScrollMs,
				"extract_ms", timings.ExtractMs,
				"download_ms", downloadMs,
				"upload_ms", uploadMs,
				"images_found", len(imageUrls),
				"images_ok", len(scImages),
				"images_failed", imagesFailed,
				"degraded", degraded,
			)
		}(req, msg)
	}
}

func handleUpdateBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, rdb *redis.Client, cfg config.Config) {
	obs.From(ctx).Info("starting listener", "topic", cfg.TopicUpdateBookRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			obs.From(ctx).Warn("error fetching update-book message", "error", err)
			continue
		}

		var req models.ScrapingUpdateBookRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			obs.From(ctx).Error("error deserializing update-book message", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicUpdateBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		logger := slog.With(
			"job_id", req.JobID,
			"book_id", req.BookID,
			"website_id", req.WebsiteID,
			"topic", cfg.TopicUpdateBookRequested,
		)
		jobCtx := obs.With(ctx, logger)

		obs.From(jobCtx).Info("processing update-book request")

		jobStart := time.Now()

		wc, err := fetchWebsiteConfig(jobCtx, rdb, req.WebsiteID, req.WebsiteConfig)
		if err != nil {
			obs.From(jobCtx).Error("failed to fetch website config", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(jobCtx, producer, cfg.TopicDLQ, cfg.TopicUpdateBookRequested, msg.Value, err)
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		result, err := engine.ScrapeUpdateBook(jobCtx, req, wc)
		if err != nil {
			metrics.JobsTotal.WithLabelValues(cfg.TopicUpdateBookRequested, "failed").Inc()
			metrics.JobDurationSeconds.WithLabelValues(cfg.TopicUpdateBookRequested).Observe(time.Since(jobStart).Seconds())
			obs.From(jobCtx).Error("update book scrape failed", "error", err)
			publishKeyedOrLog(jobCtx, producer, cfg.TopicBookFailed, req.BookID, models.ScrapingBookFailed{
				JobID:   req.JobID,
				BookID:  req.BookID,
				Error:   "SCRAPE_FAILED",
				Message: err.Error(),
			})
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		publishKeyedOrLog(jobCtx, producer, cfg.TopicUpdateBookCompleted, req.BookID, result)
		metrics.JobsTotal.WithLabelValues(cfg.TopicUpdateBookRequested, "ok").Inc()
		metrics.JobDurationSeconds.WithLabelValues(cfg.TopicUpdateBookRequested).Observe(time.Since(jobStart).Seconds())
		obs.From(jobCtx).Info("update book completed", "duration_ms", time.Since(jobStart).Milliseconds())
		commitOrLog(jobCtx, consumer, msg)
	}
}

func handleNewBookRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, rdb *redis.Client, cfg config.Config) {
	obs.From(ctx).Info("starting listener", "topic", cfg.TopicNewBookRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			obs.From(ctx).Warn("error fetching new-book message", "error", err)
			continue
		}

		var req models.ScrapingNewBookRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			obs.From(ctx).Error("error deserializing new-book message", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicNewBookRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		logger := slog.With(
			"job_id", req.JobID,
			"website_id", req.WebsiteID,
			"topic", cfg.TopicNewBookRequested,
		)
		jobCtx := obs.With(ctx, logger)

		obs.From(jobCtx).Info("processing new-book request")

		jobStart := time.Now()

		wc, err := fetchWebsiteConfig(jobCtx, rdb, req.WebsiteID, req.WebsiteConfig)
		if err != nil {
			obs.From(jobCtx).Error("failed to fetch website config", "error", err)
			sendToDLQ(jobCtx, producer, cfg.TopicDLQ, cfg.TopicNewBookRequested, msg.Value, err)
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		result, err := engine.ScrapeNewBook(jobCtx, req, wc)
		if err != nil {
			metrics.JobsTotal.WithLabelValues(cfg.TopicNewBookRequested, "failed").Inc()
			metrics.JobDurationSeconds.WithLabelValues(cfg.TopicNewBookRequested).Observe(time.Since(jobStart).Seconds())
			obs.From(jobCtx).Error("new book scrape failed", "error", err)
			publishKeyedOrLog(jobCtx, producer, cfg.TopicBookFailed, req.JobID, models.ScrapingBookFailed{
				JobID:   req.JobID,
				Error:   "SCRAPE_FAILED",
				Message: err.Error(),
			})
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		// No BookID exists yet for a new book, so key on JobID instead.
		publishKeyedOrLog(jobCtx, producer, cfg.TopicBookCompleted, req.JobID, result)
		metrics.JobsTotal.WithLabelValues(cfg.TopicNewBookRequested, "ok").Inc()
		metrics.JobDurationSeconds.WithLabelValues(cfg.TopicNewBookRequested).Observe(time.Since(jobStart).Seconds())
		obs.From(jobCtx).Info("new book completed", "duration_ms", time.Since(jobStart).Milliseconds())
		commitOrLog(jobCtx, consumer, msg)
	}
}

func handleCoversRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, rdb *redis.Client, cfg config.Config) {
	obs.From(ctx).Info("starting listener", "topic", cfg.TopicCoversRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			obs.From(ctx).Warn("error reading covers message", "error", err)
			continue
		}

		var req models.ScrapingCoversRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			obs.From(ctx).Error("error unmarshaling covers message", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicCoversRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		logger := slog.With(
			"job_id", req.JobID,
			"book_id", req.BookID,
			"website_id", req.WebsiteID,
			"topic", cfg.TopicCoversRequested,
		)
		jobCtx := obs.With(ctx, logger)

		obs.From(jobCtx).Info("processing covers request", "covers", len(req.Covers))

		wc, err := fetchWebsiteConfig(jobCtx, rdb, req.WebsiteID, req.WebsiteConfig)
		if err != nil {
			obs.From(jobCtx).Error("failed to fetch website config", "error", err)
			sendToDLQ(jobCtx, producer, cfg.TopicDLQ, cfg.TopicCoversRequested, msg.Value, err)
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		func(req models.ScrapingCoversRequest, msg kafka.Message) {
			defer commitOrLog(jobCtx, consumer, msg)

			jobStart := time.Now()

			results, cleanup := engine.ScrapeCovers(jobCtx, req, wc)
			defer cleanup()

			coverResults := make([]models.ScrapingCoverResult, 0, len(req.Covers))
			for r := range results {
				if r.Error != nil {
					metrics.ImagesTotal.WithLabelValues("download_failed").Inc()
					obs.From(jobCtx).Debug("cover download failed", "image_index", r.Index, "error", r.Error)
					continue
				}

				var originalURL string
				if r.Index >= 0 && r.Index < len(req.Covers) {
					originalURL = req.Covers[r.Index].URL
				}

				// Path pattern: <prefix>/<shard>/<uuid>.jpg
				imgID, err := newImageID()
				if err != nil {
					obs.From(jobCtx).Warn("failed to generate cover id", "image_index", r.Index, "error", err)
					r.Data = nil
					continue
				}
				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				if _, err := s3.Upload(jobCtx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg"); err != nil {
					metrics.ImagesTotal.WithLabelValues("upload_failed").Inc()
					obs.From(jobCtx).Error("cover upload failed", "image_index", r.Index, "error", err)
					r.Data = nil
					continue
				}
				metrics.ImagesTotal.WithLabelValues("ok").Inc()
				metrics.ImageBytesTotal.Add(float64(len(r.Data)))

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				coverResults = append(coverResults, models.ScrapingCoverResult{
					OriginalURL: originalURL,
					Path:        rawPathWithBucket,
				})

				publishOrLog(jobCtx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
					RawPath:      rawPathWithBucket,
					OriginalURL:  originalURL,
					TargetBucket: cfg.ProcessedImagesBucket,
					TargetPath:   targetName,
					IsBackfill:   false,
					Widths:       req.Widths,
				})

				// Early memory release for GC
				r.Data = nil
			}

			// Emit final completion event
			publishKeyedOrLog(jobCtx, producer, cfg.TopicCoversCompleted, req.JobID, models.ScrapingCoversCompleted{
				JobID:        req.JobID,
				BookID:       req.BookID,
				Results:      coverResults,
			})

			metrics.JobsTotal.WithLabelValues(cfg.TopicCoversRequested, "ok").Inc()
			metrics.JobDurationSeconds.WithLabelValues(cfg.TopicCoversRequested).Observe(time.Since(jobStart).Seconds())
			obs.From(jobCtx).Info("covers request completed", "processed", len(coverResults), "total", len(req.Covers), "duration_ms", time.Since(jobStart).Milliseconds())
		}(req, msg)
	}
}

func handleImagesRequests(ctx context.Context, consumer *kafka.Consumer, producer *kafka.Producer, engine *scraper.Scraper, s3 *storage.S3Client, rdb *redis.Client, cfg config.Config) {
	obs.From(ctx).Info("starting listener", "topic", cfg.TopicImagesRequested)
	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			obs.From(ctx).Warn("error reading images message", "error", err)
			continue
		}

		var req models.ScrapingImagesRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			obs.From(ctx).Error("error unmarshaling images message", "error", err, "payload", payloadFingerprint(msg.Value))
			sendToDLQ(ctx, producer, cfg.TopicDLQ, cfg.TopicImagesRequested, msg.Value, err)
			commitOrLog(ctx, consumer, msg)
			continue
		}

		logger := slog.With(
			"job_id", req.JobID,
			"entity_id", req.EntityID,
			"website_id", req.WebsiteID,
			"topic", cfg.TopicImagesRequested,
		)
		jobCtx := obs.With(ctx, logger)

		obs.From(jobCtx).Info("processing images request", "images", len(req.ImageURLs))

		wc, err := fetchWebsiteConfig(jobCtx, rdb, req.WebsiteID, req.WebsiteConfig)
		if err != nil {
			obs.From(jobCtx).Error("failed to fetch website config", "error", err)
			sendToDLQ(jobCtx, producer, cfg.TopicDLQ, cfg.TopicImagesRequested, msg.Value, err)
			commitOrLog(jobCtx, consumer, msg)
			continue
		}

		func(req models.ScrapingImagesRequest, msg kafka.Message) {
			defer commitOrLog(jobCtx, consumer, msg)

			jobStart := time.Now()

			results, cleanup := engine.ScrapeImages(jobCtx, req, wc)
			defer cleanup()

			urlMap := make(map[string]string)
			count := 0
			for r := range results {
				if r.Error != nil {
					metrics.ImagesTotal.WithLabelValues("download_failed").Inc()
					obs.From(jobCtx).Debug("image download failed", "image_index", r.Index, "error", r.Error)
					continue
				}

				imgID, err := newImageID()
				if err != nil {
					obs.From(jobCtx).Warn("failed to generate image id", "image_index", r.Index, "error", err)
					r.Data = nil
					continue
				}
				rawName, targetName := generateS3Keys(req.UploadTarget.PathPrefix, imgID)

				if _, err := s3.Upload(jobCtx, req.UploadTarget.Bucket, rawName, r.Data, "image/jpeg"); err != nil {
					metrics.ImagesTotal.WithLabelValues("upload_failed").Inc()
					obs.From(jobCtx).Error("image upload failed", "image_index", r.Index, "error", err)
					r.Data = nil
					continue
				}
				metrics.ImagesTotal.WithLabelValues("ok").Inc()
				metrics.ImageBytesTotal.Add(float64(len(r.Data)))

				rawPathWithBucket := fmt.Sprintf("%s/%s", req.UploadTarget.Bucket, rawName)

				publishOrLog(jobCtx, producer, cfg.TopicImageProcessing, models.ImageProcessingRequested{
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
			publishKeyedOrLog(jobCtx, producer, cfg.TopicImagesCompleted, req.JobID, models.ScrapingImagesCompleted{
				JobID:        req.JobID,
				EntityID:     req.EntityID,
				Source:       "CHAPTER",
				Format:       "images",
				URLMap:       urlMap,
			})

			metrics.JobsTotal.WithLabelValues(cfg.TopicImagesRequested, "ok").Inc()
			metrics.JobDurationSeconds.WithLabelValues(cfg.TopicImagesRequested).Observe(time.Since(jobStart).Seconds())
			obs.From(jobCtx).Info("images request completed", "processed", count, "total", len(req.ImageURLs), "duration_ms", time.Since(jobStart).Milliseconds())
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
			obs.From(ctx).Warn("error fetching test message", "error", err)
			continue
		}

		jobCtx := obs.With(ctx, slog.With("target_url", req.TargetURL))

		res, err := engine.ExecuteTestScript(jobCtx, req)
		obs.From(jobCtx).Info("test result", "result", res, "error", err)

		// Commit after execution regardless of result, test jobs are best-effort.
		commitOrLog(jobCtx, consumer, msg)
	}
}

// generateS3Keys builds the raw and processed S3 object paths for an image.
// The path uses the last 2 characters of the UUID as a shard prefix to avoid
// hot-spotting in object storage, and honours pathPrefix when set.
//
//   - rawName:    "<pathPrefix>/<shard>/<uuid>.jpg"  (uploaded immediately)
//   - targetName: "<pathPrefix>/<shard>/<uuid>.webp" (written by the image processor)
// uuidNewV7 is a seam for tests; production code always uses uuid.NewV7.
var uuidNewV7 = uuid.NewV7

// newImageID generates a UUIDv7 for S3 object naming (time-ordered for
// locality). A generation failure must not be swallowed: the zero UUID it
// would otherwise produce collides across every image in the job, since
// generateS3Keys derives both the shard and the object key from it (BUG-06).
func newImageID() (string, error) {
	id, err := uuidNewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

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
