package kafkaproducer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KMBuilder uses the builder patten to construct kafka messages.
type KMBuilder struct {
	m kafka.Message
}

// Build returns the filled kafka message.
func (b *KMBuilder) Build() *kafka.Message {
	return &b.m
}

// Topic sets the message's topic and destination partition ID.
func (b *KMBuilder) Topic(topic *string, partitionId int32) *KMBuilder {
	b.m.TopicPartition = kafka.TopicPartition{
		Topic:     topic,
		Partition: partitionId}

	return b
}

// Key sets the message's key.
func (b *KMBuilder) Key(key *[]byte) *KMBuilder {
	b.m.Key = *key

	return b
}

// Payload sets the message's value (payload bytes).
func (b *KMBuilder) Payload(payload *[]byte) *KMBuilder {
	b.m.Value = *payload

	return b
}
