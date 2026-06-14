package main
import (
	"fmt"
	"github.com/segmentio/kafka-go"
)
func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "test",
		Topic: "test",
		StartOffset: kafka.FirstOffset,
	})
	fmt.Printf("%+v\n", r.Config().StartOffset)
}
