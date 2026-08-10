package main

import (
	"errors"
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
