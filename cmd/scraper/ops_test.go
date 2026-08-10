package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gatuno/scraper/internal/config"
	"github.com/redis/go-redis/v9"
)

// unreachableRedis returns a client pointed at a port nothing listens on,
// so Ping fails fast and deterministically without needing a real server.
func unreachableRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
}

// TestHealthz_AlwaysOK guards OBS-06's liveness contract: /healthz must
// report the process is up with no dependency checks, so a Redis/Kafka/
// browser outage never makes the orchestrator think the process itself
// died and kill a pod that could otherwise recover.
func TestHealthz_AlwaysOK(t *testing.T) {
	mux := newOpsMux(config.Config{}, unreachableRedis(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected /healthz to return 200 regardless of dependencies, got %d", rec.Code)
	}
}

// TestReadyz_FailsWhenRedisUnreachable guards OBS-06's core motivation: the
// orchestrator must be able to detect a scraper that has lost Redis and
// stop routing work to it, instead of "happily keeping a dead pod in
// service" (the exact phrase from the audit). Redis is checked first, so a
// nil BrowserPool never gets dereferenced on this path.
func TestReadyz_FailsWhenRedisUnreachable(t *testing.T) {
	mux := newOpsMux(config.Config{}, unreachableRedis(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 503 {
		t.Fatalf("expected /readyz to return 503 when redis is unreachable, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "redis") {
		t.Fatalf("expected the failure reason to mention redis, got: %s", rec.Body.String())
	}
}

// TestMetrics_ServesPrometheusExposition guards the OBS-06/OBS-04 handoff:
// /metrics must serve valid Prometheus exposition format from the moment
// the ops server exists, before any custom business metrics (OBS-04) are
// registered — the default Go/process collectors are enough to prove the
// endpoint is wired correctly.
func TestMetrics_ServesPrometheusExposition(t *testing.T) {
	mux := newOpsMux(config.Config{}, unreachableRedis(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected /metrics to return 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "# HELP") {
		t.Fatalf("expected Prometheus exposition format (# HELP lines), got: %s", rec.Body.String()[:200])
	}
}

// TestPprof_GatedByConfig guards the security-relevant half of OBS-06:
// pprof must be opt-in via PPROF_ENABLED, not compiled in unconditionally —
// exposing pprof by default would leak heap/goroutine data to anyone who
// can reach the ops port.
func TestPprof_GatedByConfig(t *testing.T) {
	disabled := newOpsMux(config.Config{PprofEnabled: false}, unreachableRedis(), nil)
	rec := httptest.NewRecorder()
	disabled.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if rec.Code == 200 {
		t.Fatal("expected /debug/pprof/ to be unavailable when PprofEnabled=false")
	}

	enabled := newOpsMux(config.Config{PprofEnabled: true}, unreachableRedis(), nil)
	rec = httptest.NewRecorder()
	enabled.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if rec.Code != 200 {
		t.Fatalf("expected /debug/pprof/ to be available when PprofEnabled=true, got %d", rec.Code)
	}
}
