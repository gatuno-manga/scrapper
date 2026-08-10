package kafka

import (
	"testing"

	segmentiokafka "github.com/segmentio/kafka-go"
)

// TestNewProducer_UsesHashBalancer guards against BUG-03: with the previous
// LeastBytes balancer and no message key, events for the same entity
// (e.g. scraping.chapter.pages_extracted and scraping.chapter.completed for
// the same chapterId) could land on different partitions and be observed
// out of order downstream. Hash routes by Message.Key and falls back to
// round-robin when Key is nil, so switching balancers doesn't break callers
// that don't key their messages.
func TestNewProducer_UsesHashBalancer(t *testing.T) {
	p := NewProducer([]string{"localhost:9092"}, 10, 1, false)
	defer p.writer.Close()

	if _, ok := p.writer.Balancer.(*segmentiokafka.Hash); !ok {
		t.Fatalf("expected Balancer to be *kafka.Hash, got %T", p.writer.Balancer)
	}
}

// TestNewProducer_AllowAutoTopicCreationDefaultsToOperatorChoice guards
// against BUG-03's second defect: AllowAutoTopicCreation must reflect what
// the caller configured (production should default to false so a typo'd
// TOPIC_* env var fails loudly instead of silently creating an unconsumed
// topic), not be hardcoded to true.
func TestNewProducer_AllowAutoTopicCreationDefaultsToOperatorChoice(t *testing.T) {
	off := NewProducer([]string{"localhost:9092"}, 10, 1, false)
	defer off.writer.Close()
	if off.writer.AllowAutoTopicCreation {
		t.Fatal("expected AllowAutoTopicCreation=false to be honoured")
	}

	on := NewProducer([]string{"localhost:9092"}, 10, 1, true)
	defer on.writer.Close()
	if !on.writer.AllowAutoTopicCreation {
		t.Fatal("expected AllowAutoTopicCreation=true to be honoured")
	}
}

func TestKeyedMessage_SetsKeyOnlyWhenNonEmpty(t *testing.T) {
	withKey := keyedMessage("scraping.chapter.completed", "chapter-123", []byte(`{}`))
	if string(withKey.Key) != "chapter-123" {
		t.Fatalf("expected key %q, got %q", "chapter-123", withKey.Key)
	}

	withoutKey := keyedMessage("scraping.chapter.completed", "", []byte(`{}`))
	if withoutKey.Key != nil {
		t.Fatalf("expected nil key for unkeyed publish, got %q", withoutKey.Key)
	}
}

func TestProducer_PublishUsesEmptyKey(t *testing.T) {
	// Publish is PublishKeyed with an empty key; verify at the message-
	// building level since PublishKeyed itself requires a live broker.
	msg := keyedMessage("scraping.chapter.completed", "", []byte(`{"a":1}`))
	if msg.Key != nil {
		t.Fatalf("Publish (unkeyed) must not set a partition key, got %q", msg.Key)
	}
	if msg.Topic != "scraping.chapter.completed" {
		t.Fatalf("unexpected topic: %q", msg.Topic)
	}
}
