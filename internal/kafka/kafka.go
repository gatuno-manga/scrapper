package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher is the interface for publishing messages to Kafka.
// Using this interface instead of *Producer makes handlers testable via mocks.
type Publisher interface {
	Publish(ctx context.Context, topic string, message interface{}) error
	Close() error
}

// MessageConsumer is the interface for consuming messages from Kafka.
// Fetch and Commit are separate to enable at-least-once delivery semantics:
// the offset is only committed after the message has been fully processed.
type MessageConsumer interface {
	Fetch(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
	FetchMessage(ctx context.Context, target interface{}) (Message, error)
	Close() error
}

// Message wraps a kafka-go message, exposing only the fields needed by handlers.
// This avoids leaking the segmentio/kafka-go type into the application layer.
type Message struct {
	raw   kafka.Message
	Topic string
	Value []byte
}

// --- Producer ---

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, writeTimeout int, acks int) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			WriteTimeout:           time.Duration(writeTimeout) * time.Second,
			RequiredAcks:           kafka.RequiredAcks(acks),
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, topic string, message interface{}) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: payload,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// --- Consumer ---

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID, topic string, readTimeout int) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
			MaxWait: time.Duration(readTimeout) * time.Second,
		}),
	}
}

// Fetch reads the next available message WITHOUT committing its offset.
// You MUST call Commit() after the message has been successfully processed
// to avoid reprocessing it on restart (at-least-once guarantee).
func (c *Consumer) Fetch(ctx context.Context) (Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{raw: msg, Topic: msg.Topic, Value: msg.Value}, nil
}

// Commit acknowledges that a message has been processed by committing its
// offset back to Kafka. Call this only after successful processing.
func (c *Consumer) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, msg.raw)
}

// FetchMessage reads the next message and deserializes its payload into target.
// The offset is NOT committed automatically — call Commit() after processing.
func (c *Consumer) FetchMessage(ctx context.Context, target interface{}) (Message, error) {
	msg, err := c.Fetch(ctx)
	if err != nil {
		return msg, err
	}

	if err := json.Unmarshal(msg.Value, target); err != nil {
		return msg, err
	}

	return msg, nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
