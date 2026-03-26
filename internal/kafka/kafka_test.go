package kafka

import (
	"errors"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestPublishSendsMessageToTopic(t *testing.T) {
	producer := &fakeProducer{}

	err := Publish(producer, "orders.created", []byte("payload-1"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if producer.message == nil {
		t.Fatal("producer.message = nil, want message")
	}

	if producer.message.TopicPartition.Topic == nil {
		t.Fatal("message topic = nil, want topic")
	}

	if got, want := *producer.message.TopicPartition.Topic, "orders.created"; got != want {
		t.Fatalf("topic = %q, want %q", got, want)
	}

	if got, want := string(producer.message.Value), "payload-1"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}

	if got, want := producer.message.TopicPartition.Partition, int32(ckafka.PartitionAny); got != want {
		t.Fatalf("partition = %d, want %d", got, want)
	}
}

func TestPublishWrapsProduceError(t *testing.T) {
	producer := &fakeProducer{
		produceErr: errors.New("produce failed"),
	}

	err := Publish(producer, "orders.created", []byte("payload-1"))
	if err == nil {
		t.Fatal("Publish() error = nil, want error")
	}

	if got, want := err.Error(), "produce message: produce failed"; got != want {
		t.Fatalf("Publish() error = %q, want %q", got, want)
	}
}

func TestPublishWrapsDeliveryError(t *testing.T) {
	producer := &fakeProducer{
		deliveryErr: errors.New("delivery failed"),
	}

	err := Publish(producer, "orders.created", []byte("payload-1"))
	if err == nil {
		t.Fatal("Publish() error = nil, want error")
	}

	if got, want := err.Error(), "delivery failed: delivery failed"; got != want {
		t.Fatalf("Publish() error = %q, want %q", got, want)
	}
}

type fakeProducer struct {
	message     *ckafka.Message
	produceErr  error
	deliveryErr error
}

func (p *fakeProducer) Produce(msg *ckafka.Message, deliveryChan chan ckafka.Event) error {
	p.message = msg

	if p.produceErr != nil {
		return p.produceErr
	}

	deliveryChan <- &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Error:     p.deliveryErr,
		},
		Value: msg.Value,
	}

	return nil
}
