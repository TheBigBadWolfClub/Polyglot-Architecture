package pkg

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const server = "localhost:9092"

func Producer(messages string, topicName string) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": server})
	if err != nil {
		return err
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Producer messages to topic (asynchronously)
	topic := topicName
	data := []byte(messages)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)

	if err != nil {
		return err
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
	return nil
}
