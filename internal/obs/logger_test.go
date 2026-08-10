package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// TestWith_AttachesLoggerRetrievableViaFrom guards the core OBS-01
// mechanism: every handler attaches a job-scoped logger to the context it
// passes down into internal/scraper and internal/storage, and those
// packages retrieve it with From. If With/From ever stopped round-tripping
// the exact logger, job context would silently stop propagating.
func TestWith_AttachesLoggerRetrievableViaFrom(t *testing.T) {
	base := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := With(context.Background(), base)

	got := From(ctx)
	if got != base {
		t.Fatal("expected From to return the exact logger attached via With")
	}
}

// TestFrom_FallsBackToDefaultWhenNoneAttached guards the fallback path used
// by code that runs outside any job (e.g. BrowserPool construction at
// startup): From must never panic or return nil just because no job-scoped
// logger was attached to the context.
func TestFrom_FallsBackToDefaultWhenNoneAttached(t *testing.T) {
	got := From(context.Background())
	if got != slog.Default() {
		t.Fatal("expected From to fall back to slog.Default() when no logger is attached")
	}
}

func TestSetup_SetsProcessDefaultLogger(t *testing.T) {
	l := Setup(slog.LevelInfo, "json")
	if slog.Default() != l {
		t.Fatal("expected Setup to set the process-wide default logger")
	}
}

// TestSetup_FormatSelectsHandler guards the LOG_FORMAT contract: "json" in
// production must yield a JSON handler (parseable by log aggregators),
// anything else falls back to a human-readable text handler for local dev.
func TestSetup_FormatSelectsHandler(t *testing.T) {
	if _, ok := Setup(slog.LevelInfo, "json").Handler().(*slog.JSONHandler); !ok {
		t.Fatalf("expected *slog.JSONHandler for format=json")
	}
	if _, ok := Setup(slog.LevelInfo, "text").Handler().(*slog.TextHandler); !ok {
		t.Fatalf("expected *slog.TextHandler for format=text")
	}
}

// TestJobScopedLogger_EveryRecordCarriesJobID reproduces the exact pattern
// used at every Kafka handler entry point in cmd/scraper/main.go
// (slog.With(...) then obs.With(ctx, logger), consumed downstream via
// obs.From(ctx)) and asserts the OBS-01 acceptance criteria directly:
// every log record emitted while processing a job carries job_id, and
// LOG_FORMAT=json produces one parseable JSON object per line.
func TestJobScopedLogger_EveryRecordCarriesJobID(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	logger := base.With("job_id", "job-123", "chapter_id", "chapter-456")
	ctx := With(context.Background(), logger)

	From(ctx).Info("processing chapter request")
	From(ctx).Info("navigating", "target_url", "https://example.com")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("expected one valid JSON object per line, got error %v for line %q", err, line)
		}
		if record["job_id"] != "job-123" {
			t.Fatalf("expected job_id=job-123 in every record, got %v in %q", record["job_id"], line)
		}
		if record["chapter_id"] != "chapter-456" {
			t.Fatalf("expected chapter_id=chapter-456 in every record, got %v in %q", record["chapter_id"], line)
		}
	}
}
