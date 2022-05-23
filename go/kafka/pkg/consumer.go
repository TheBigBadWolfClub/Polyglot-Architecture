package pkg

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func Consumer(topics []string) error {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     server,
		"group.id":              1,
		"broker.address.family": "v4",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}

	run := true
	for run == true {
		ev := consumer.Poll(0)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))
			if e.Headers != nil {
				fmt.Printf("%% Headers: %v\n", e.Headers)
			}
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				run = false
			}
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	return nil
}
