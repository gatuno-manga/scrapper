package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestNewImageID_PropagatesGenerationError guards against BUG-06: a failed
// UUIDv7 generation must surface as an error, not silently fall through to
// the zero UUID. Before the fix, the generation error was discarded and
// every image in the job ended up sharing the same S3 key.
func TestNewImageID_PropagatesGenerationError(t *testing.T) {
	orig := uuidNewV7
	defer func() { uuidNewV7 = orig }()

	wantErr := errors.New("rand read failed")
	uuidNewV7 = func() (uuid.UUID, error) {
		return uuid.UUID{}, wantErr
	}

	id, err := newImageID()
	if err == nil {
		t.Fatal("expected newImageID to propagate the generation error, got nil")
	}
	if id != "" {
		t.Fatalf("expected empty id on error, got %q", id)
	}
}

// TestNewImageID_NeverReturnsZeroUUID reproduces the collision scenario from
// BUG-06 directly: if newImageID ever returned the zero UUID on success,
// generateS3Keys would derive the same shard and object key for every image
// in a chapter, silently overwriting all but one in S3.
func TestNewImageID_NeverReturnsZeroUUID(t *testing.T) {
	const zeroUUID = "00000000-0000-0000-0000-000000000000"

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := newImageID()
		if err != nil {
			t.Fatalf("unexpected error from newImageID: %v", err)
		}
		if id == zeroUUID {
			t.Fatal("newImageID returned the zero UUID")
		}
		if seen[id] {
			t.Fatalf("newImageID returned a duplicate id: %s", id)
		}
		seen[id] = true
	}
}

// TestGenerateS3Keys_ZeroUUIDCollision documents why a discarded error is
// dangerous: distinct images sharing the fallback zero UUID map to the exact
// same S3 keys, which is the root failure mode BUG-06 fixes.
func TestGenerateS3Keys_ZeroUUIDCollision(t *testing.T) {
	const zeroUUID = "00000000-0000-0000-0000-000000000000"

	raw1, target1 := generateS3Keys("", zeroUUID)
	raw2, target2 := generateS3Keys("", zeroUUID)

	if raw1 != raw2 || target1 != target2 {
		t.Fatalf("expected identical keys to illustrate the collision, got (%s,%s) vs (%s,%s)", raw1, target1, raw2, target2)
	}
}

// secretBearingPayload mirrors a real chapter/covers/images request: it
// embeds a websiteConfig carrying a live session cookie, an Authorization
// header and a credentialed proxy URL — exactly the fields OBS-03 flags as
// unsafe to log or persist to the DLQ in the clear.
const secretBearingPayload = `{
	"jobId": "job-1",
	"chapterId": "chapter-1",
	"targetUrl": "https://example.com/chapter-1",
	"websiteConfig": {
		"cookies": [{"name": "session", "value": "super-secret-session-token"}],
		"headers": {"Authorization": "Bearer top-secret-api-key"},
		"proxyUrl": "http://user:hunter2@proxy.example.com:8080"
	}
}`

// TestPayloadFingerprint_NeverLeaksPayloadContent guards against OBS-03: a
// deserialization-failure log line must never contain the raw payload,
// since it embeds cookies, auth headers and proxy credentials.
func TestPayloadFingerprint_NeverLeaksPayloadContent(t *testing.T) {
	fp := payloadFingerprint([]byte(secretBearingPayload))

	for _, secret := range []string{"super-secret-session-token", "top-secret-api-key", "hunter2"} {
		if strings.Contains(fp, secret) {
			t.Fatalf("payloadFingerprint leaked secret %q: %s", secret, fp)
		}
	}
	if !strings.HasPrefix(fp, "sha256:") {
		t.Fatalf("expected fingerprint to start with sha256:, got %s", fp)
	}
}

// TestRedactPayload_StripsWebsiteConfig guards against OBS-03: the DLQ
// payload must never carry cookies, Authorization headers or proxy
// credentials, since DLQ messages persist for the topic's retention period
// and are readable by anyone with topic access.
func TestRedactPayload_StripsWebsiteConfig(t *testing.T) {
	redacted := redactPayload([]byte(secretBearingPayload))

	for _, secret := range []string{"super-secret-session-token", "top-secret-api-key", "hunter2"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redactPayload leaked secret %q: %s", secret, redacted)
		}
	}
	// Non-sensitive fields needed to identify/replay the job must survive.
	for _, want := range []string{"job-1", "chapter-1"} {
		if !strings.Contains(redacted, want) {
			t.Fatalf("redactPayload dropped non-sensitive field %q: %s", want, redacted)
		}
	}
}

// TestRedactPayload_UnparseablePayloadFallsBackToFingerprint guards the DLQ
// path used when json.Unmarshal itself failed (the common DLQ case per
// OBS-03): an unparseable payload must never be stored raw, since we can't
// selectively strip fields from something that isn't valid JSON.
func TestRedactPayload_UnparseablePayloadFallsBackToFingerprint(t *testing.T) {
	garbage := []byte(`not valid json { "cookie": "super-secret-session-token"`)

	redacted := redactPayload(garbage)

	if strings.Contains(redacted, "super-secret-session-token") {
		t.Fatalf("redactPayload leaked secret from unparseable payload: %s", redacted)
	}
	if !strings.HasPrefix(redacted, "sha256:") {
		t.Fatalf("expected fallback to payloadFingerprint format, got %s", redacted)
	}
}
