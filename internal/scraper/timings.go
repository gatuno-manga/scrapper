package scraper

// PhaseTimings accumulates per-phase wall-clock time for a single chapter
// job so the caller can emit one wide completion event and feed histograms
// (OBS-05) instead of scattering timing across many narrow log lines. A nil
// *PhaseTimings is always safe to pass — every call site that doesn't need
// phase timing (cmd/cli, cover/image/book scraping, test scripts) passes
// nil and the recording calls become no-ops.
type PhaseTimings struct {
	SemWaitMs int64
	NavMs     int64
	PrepareMs int64
	ScrollMs  int64
	ExtractMs int64
}
