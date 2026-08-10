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
go run ./cmd/scraper
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

## Ops Server: Health, Metrics & Profiling
The microservice always runs an ops HTTP server (default `:6060`, override with
`OPS_ADDR`) alongside the Kafka consumers:

- **Liveness**: `GET /healthz` — process is up, no dependency checks.
- **Readiness**: `GET /readyz` — checks Redis, the browser connection and Kafka;
  returns `503` if any dependency is unreachable.
- **Metrics**: `GET /metrics` — Prometheus exposition format.
- **Profiling**: `/debug/pprof/*` — disabled by default, no rebuild required to
  enable it.

### Profiling
Set `PPROF_ENABLED=true` (an env var, not a build tag) to expose pprof on the
same ops server. Never expose this port publicly — it has no authentication.

```bash
# Enable pprof at runtime
PPROF_ENABLED=true go run ./cmd/scraper

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
