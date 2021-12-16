package kafkaproducer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// newMessageBuilder creates a new instance of messageBuilder.
func newMessageBuilder(key string, topic *string, partitionID int32, headers []kafka.Header,
	payload []byte) *messageBuilder {
	return &messageBuilder{
		message: kafka.Message{
			Key: []byte(key),
			TopicPartition: kafka.TopicPartition{
				Topic:     topic,
				Partition: partitionID,
			},
			Headers: headers,
			Value:   payload,
		},
	}
}

// messageBuilder uses the builder patten to construct a kafka message.
type messageBuilder struct {
	message kafka.Message
}

// header adds a header to the message headers.
func (builder *messageBuilder) header(header kafka.Header) *messageBuilder {
	builder.message.Headers = append(builder.message.Headers, header)

	return builder
}

// build returns the filled kafka message.
func (builder *messageBuilder) build() *kafka.Message {
	return &builder.message
}
