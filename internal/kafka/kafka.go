package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

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

func (c *Consumer) FetchMessage(ctx context.Context, target interface{}) (kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return msg, err
	}

	if err := json.Unmarshal(msg.Value, target); err != nil {
		return msg, err
	}

	return msg, nil
}

func (c *Consumer) ReadRawMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
