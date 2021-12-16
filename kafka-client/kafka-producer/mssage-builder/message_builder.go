package mssage_builder

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// newMessageBuilder creates a new instance of messageBuilder.
func NewMessageBuilder(key string, topic *string, partitionID int32, headers []kafka.Header,
	payload []byte) *MessageBuilder {
	return &MessageBuilder{
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
type MessageBuilder struct {
	message kafka.Message
}

// header adds a header to the message headers.
func (builder *MessageBuilder) Header(header kafka.Header) *MessageBuilder {
	builder.message.Headers = append(builder.message.Headers, header)

	return builder
}

// build returns the filled kafka message.
func (builder *MessageBuilder) Build() *kafka.Message {
	return &builder.message
}
