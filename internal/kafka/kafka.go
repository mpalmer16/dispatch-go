package kafka

import (
	"fmt"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func NewProducer(bootstrapServers string) (*ckafka.Producer, error) {
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         "dispatch-go-relay",
		"acks":              "all",
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return producer, nil
}

func Publish(producer *ckafka.Producer, topic string, payload []byte) error {
	deliveryChan := make(chan ckafka.Event, 1)
	defer close(deliveryChan)

	err := producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: payload,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	event := <-deliveryChan
	message := event.(*ckafka.Message)

	if message.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", message.TopicPartition.Error)
	}

	return nil
}
