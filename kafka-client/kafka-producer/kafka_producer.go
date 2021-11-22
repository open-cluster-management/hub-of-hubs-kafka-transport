package kafkaproducer

import (
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const producerFlushPeriod = 5 * int(time.Second)

var (
	errFailedToCreateProducer = errors.New("failed to create kafka producer")
	errFailedToSendMessage    = errors.New("failed to send message")
)

// NewKafkaProducer returns a new instance of KafkaProducer object. Arguments:
//
// configMap: kafka producer's configuration map.
//
// messageSizeLimit: the message size limit in bytes (payloads of higher length are broken into fragments).
//
// deliveryChan: the channel to delivery production events to.
func NewKafkaProducer(configMap *kafka.ConfigMap, messageSizeLimit int,
	deliveryChan chan kafka.Event) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("%w - %v", errFailedToCreateProducer, err)
	}

	return &KafkaProducer{
		kafkaProducer:    p,
		deliveryChan:     deliveryChan,
		messageSizeLimit: messageSizeLimit,
	}, nil
}

// KafkaProducer abstracts Confluent Kafka usage.
type KafkaProducer struct {
	kafkaProducer *kafka.Producer
	deliveryChan  chan kafka.Event
	// message size limit in bytes
	messageSizeLimit int
}

// Close closes the KafkaProducer.
func (p *KafkaProducer) Close() {
	p.kafkaProducer.Flush(producerFlushPeriod) // give the kafka-producer a chance to finish sending
	p.kafkaProducer.Close()
}

// Producer returns the wrapped Confluent KafkaProducer member.
func (p *KafkaProducer) Producer() *kafka.Producer {
	return p.kafkaProducer
}

// ProduceAsync sends a message to the kafka brokers asynchronously.
func (p *KafkaProducer) ProduceAsync(key string, topic string, partition int32, headers []kafka.Header,
	msg []byte) error {
	if len(msg) > p.messageSizeLimit {
		if err := dismantleAndSendKafkaMessage(p, key, &topic, partition, headers, msg); err != nil {
			return fmt.Errorf("%w: %v", errFailedToSendMessage, err)
		}

		return nil
	}

	builder := &KafkaMessageBuilder{}
	kafkaMessage := builder.
		Topic(&topic, partition).
		Key(key).
		Headers(headers).
		Payload(msg).Build()

	if err := p.kafkaProducer.Produce(kafkaMessage, p.deliveryChan); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSendMessage, err)
	}

	return nil
}
