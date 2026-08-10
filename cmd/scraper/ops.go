package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/gatuno/scraper/internal/config"
	"github.com/gatuno/scraper/internal/kafka"
	"github.com/gatuno/scraper/internal/scraper"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// newOpsMux builds the ops server's routes. Split out from startOpsServer so
// the handlers are testable via httptest without opening a real listener.
func newOpsMux(cfg config.Config, rdb *redis.Client, pool *scraper.BrowserPool) *http.ServeMux {
	mux := http.NewServeMux()

	// Liveness: process is up. Cheap, no dependency checks.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Readiness: can this instance actually do work right now?
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !pool.IsConnected() {
			http.Error(w, "browser not connected", http.StatusServiceUnavailable)
			return
		}
		if err := kafka.Ping(ctx, cfg.KafkaBrokers); err != nil {
			http.Error(w, "kafka unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	mux.Handle("/metrics", promhttp.Handler())

	// Env-gated, NOT build-tag gated, so it can be flipped on in production
	// without a rebuild. Never expose this port publicly.
	if cfg.PprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	return mux
}

// startOpsServer starts the always-compiled ops HTTP server exposing
// liveness/readiness probes and Prometheus metrics (OBS-06). It replaces
// the old //go:build pprof approach (cmd/scraper/pprof.go): that tag meant
// the production image (plain `go build`, no `-tags pprof`) shipped with no
// profiling endpoint and no health check at all, so diagnosing a live
// incident required rebuilding and redeploying — by which point the
// incident state was gone.
func startOpsServer(cfg config.Config, rdb *redis.Client, pool *scraper.BrowserPool) *http.Server {
	srv := &http.Server{Addr: cfg.OpsAddr, Handler: newOpsMux(cfg, rdb, pool)}
	go func() {
		slog.Info("ops server listening", "addr", cfg.OpsAddr, "pprof_enabled", cfg.PprofEnabled)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("ops server failed", "error", err)
		}
	}()
	return srv
}
