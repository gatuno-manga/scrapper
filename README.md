# Gatuno Scraper Go Microservice

A stateless web scraping microservice built with Go, Kafka, Playwright, and S3.

## Architecture
- **Stateless**: No database connection. Configuration is received via Kafka events.
- **Messaging**: Uses Kafka for job requests and completion events.
- **Scraping**: Uses Playwright for browser automation and Cloudflare bypass.
- **Storage**: Uploads raw images to S3 (MinIO compatible).

## Environment Variables
- `KAFKA_BROKERS`: Comma-separated list of brokers (default: `localhost:9092`).
- `KAFKA_GROUP_ID`: Consumer group ID (default: `scraper-microservice`).
- `S3_ENDPOINT`: S3 endpoint (default: `localhost:9000`).
- `S3_ACCESS_KEY`: S3 access key.
- `S3_SECRET_KEY`: S3 secret key.
- `S3_REGION`: S3 region (default: `us-east-1`).
- `S3_USE_SSL`: Set to `true` to use SSL.

## How to Run

### Microservice
```bash
go run ./cmd/scraper/main.go
```

### CLI Test Tool
The CLI allows testing the scraper locally without Kafka/S3.

```bash
# Test a chapter scrape
./bin/cli -mode chapter -url "https://site.com/manga/c1" -title-sel "h1" -images-sel ".content img" -bypass

# Test a book info scrape
./bin/cli -mode book -url "https://site.com/manga/m1" -script "(() => ({ title: document.title, chapters: [] }))()"

# Test a generic script
./bin/cli -mode test -url "https://google.com" -script "document.title"
```

## Performance Monitoring & Profiling
The microservice includes a `pprof` server for real-time performance analysis.

### Profiling Endpoints
- **CPU Profile**: `http://localhost:6060/debug/pprof/profile`
- **Memory (Heap)**: `http://localhost:6060/debug/pprof/heap`
- **Goroutines**: `http://localhost:6060/debug/pprof/goroutine`

### How to analyze (Visual Mode)
Note: The pprof server must be enabled at build time using the `pprof` tag.

```bash
# Build with pprof enabled
go build -tags pprof -o bin/scraper ./cmd/scraper/main.go

# View Memory Usage Graph
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
```

## Kafka Topics
- **Input**:
  - `scraping.chapter.requested`
  - `scraping.new-book.requested`
  - `scraping.test`
- **Output**:
  - `scraping.chapter.completed`
  - `scraping.chapter.failed`
  - `scraping.new-book.completed`
  - `image.processing.requested` (to Image Processor service)
