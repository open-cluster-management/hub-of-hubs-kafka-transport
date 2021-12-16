package kafkaproducer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// newMessageBuilder creates a new instance of messageBuilder.
func newMessageBuilder() *messageBuilder {
	return &messageBuilder{}
}

// messageBuilder uses the builder patten to construct a kafka message.
type messageBuilder struct {
	message kafka.Message
}

// key sets the message key.
func (builder *messageBuilder) key(key string) *messageBuilder {
	builder.message.Key = []byte(key)

	return builder
}

// topic sets the message topic and partition ID.
func (builder *messageBuilder) topic(topic *string, partitionID int32) *messageBuilder {
	builder.message.TopicPartition = kafka.TopicPartition{
		Topic:     topic,
		Partition: partitionID,
	}

	return builder
}

// payload sets the message value (payload bytes).
func (builder *messageBuilder) payload(payload []byte) *messageBuilder {
	builder.message.Value = payload

	return builder
}

// header adds a header to the message headers.
func (builder *messageBuilder) header(header kafka.Header) *messageBuilder {
	builder.message.Headers = append(builder.message.Headers, header)

	return builder
}

// headers appends headers to the message headers.
func (builder *messageBuilder) headers(headers []kafka.Header) *messageBuilder {
	builder.message.Headers = append(builder.message.Headers, headers...)

	return builder
}

// build returns the filled kafka message.
func (builder *messageBuilder) build() *kafka.Message {
	return &builder.message
}
