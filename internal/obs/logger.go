// Package obs provides the process-wide structured logger and the
// context plumbing that lets every package attach job-scoped fields
// (job_id, chapter_id, ...) to their log records without depending on
// cmd/scraper directly (OBS-01).
package obs

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

// Setup configures the process-wide default logger.
// format: "json" (production) or "text" (development).
func Setup(level slog.Level, format string) *slog.Logger {
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	l := slog.New(h)
	slog.SetDefault(l)
	return l
}

// ParseLevel parses a LOG_LEVEL value ("debug", "info", "warn", "error",
// case-insensitive) into a slog.Level, defaulting to Info for an empty or
// unrecognized value so a typo'd env var doesn't crash startup.
func ParseLevel(s string) slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return level
}

// With attaches a logger to ctx so downstream packages inherit job context.
func With(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// From returns the logger attached to ctx, or the default logger.
func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
