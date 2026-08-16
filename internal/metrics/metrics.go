// Package metrics defines the process-wide Prometheus collectors (OBS-04).
// Handlers and internal packages import this package directly and call
// Inc/Observe on the package-level vars; cmd/scraper only needs to expose
// promhttp.Handler() (already wired by OBS-06) for these to be scraped.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_jobs_total",
		Help: "Total number of jobs processed, by topic and outcome (ok|failed|dlq).",
	}, []string{"topic", "outcome"})

	DLQTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_dlq_total",
		Help: "Total number of messages routed to the dead-letter queue, by originating topic.",
	}, []string{"topic"})

	KafkaPublishFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_kafka_publish_failures_total",
		Help: "Total number of failed Kafka publishes, by topic.",
	}, []string{"topic"})

	ImagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scraper_images_total",
		Help: "Total number of images processed, by outcome (ok|download_failed|upload_failed).",
	}, []string{"outcome"})

	ImageBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scraper_image_bytes_total",
		Help: "Total bytes of image data successfully uploaded to storage.",
	})

	BrowserPoolInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scraper_browser_pool_inuse",
		Help: "Number of browser-pool contexts currently checked out.",
	})

	BrowserReconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scraper_browser_reconnects_total",
		Help: "Total number of times the browser connection was lost and reestablished.",
	})

	SemaphoreWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scraper_semaphore_wait_seconds",
		Help:    "Time spent waiting to acquire the per-domain browser concurrency semaphore.",
		Buckets: prometheus.DefBuckets,
	}, []string{"domain"})

	RateLimitWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scraper_ratelimit_wait_seconds",
		Help:    "Time spent waiting on the per-domain rate limiter.",
		Buckets: prometheus.DefBuckets,
	}, []string{"domain"})
)

// maxDomainLabels caps how many distinct "domain" label values the process
// will ever emit. domain is derived from scrape-target URLs, which are
// operator/attacker-influenced input, not a fixed set — without a cap, a
// misbehaving feed of targets turns an unbounded label into unbounded
// Prometheus series (OBS-04's own cardinality warning).
const maxDomainLabels = 64

var (
	domainMu    sync.Mutex
	seenDomains = make(map[string]struct{}, maxDomainLabels)
)

// BoundedDomain returns domain unchanged for the first maxDomainLabels
// distinct values seen by this process, and "other" for every value after
// that, so domain-labeled metrics can never grow without bound.
func BoundedDomain(domain string) string {
	domainMu.Lock()
	defer domainMu.Unlock()

	if _, ok := seenDomains[domain]; ok {
		return domain
	}
	if len(seenDomains) >= maxDomainLabels {
		return "other"
	}
	seenDomains[domain] = struct{}{}
	return domain
}
