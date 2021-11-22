package kafkaproducer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaMessageBuilder uses the builder patten to construct kafka messages.
type KafkaMessageBuilder struct {
	m kafka.Message
}

// Build returns the filled kafka message.
func (b *KafkaMessageBuilder) Build() *kafka.Message {
	return &b.m
}

// Topic sets the message's topic and destination partition ID.
func (b *KafkaMessageBuilder) Topic(topic *string, partitionID int32) *KafkaMessageBuilder {
	b.m.TopicPartition = kafka.TopicPartition{
		Topic:     topic,
		Partition: partitionID,
	}

	return b
}

// Key sets the message's key.
func (b *KafkaMessageBuilder) Key(key string) *KafkaMessageBuilder {
	b.m.Key = []byte(key)

	return b
}

// Payload sets the message's value (payload bytes).
func (b *KafkaMessageBuilder) Payload(payload []byte) *KafkaMessageBuilder {
	b.m.Value = payload

	return b
}

// Header adds a header to the message's headers array.
func (b *KafkaMessageBuilder) Header(header kafka.Header) *KafkaMessageBuilder {
	b.m.Headers = append(b.m.Headers, header)

	return b
}

// Headers appends headers to the message's headers array.
func (b *KafkaMessageBuilder) Headers(headers []kafka.Header) *KafkaMessageBuilder {
	b.m.Headers = append(b.m.Headers, headers...)

	return b
}
