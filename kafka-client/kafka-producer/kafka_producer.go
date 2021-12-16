package kafkaproducer

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const (
	producerFlushPeriod = 5 * int(time.Second)
	intSize             = 4
)

// NewKafkaProducer returns a new instance of KafkaProducer.
//
// Arguments:
// configMap: kafka producer's configuration map.
// messageSizeLimit: the message size limit in bytes (payloads of higher length are broken into fragments).
// deliveryChan: the channel to delivery production events to.
func NewKafkaProducer(configMap *kafka.ConfigMap, messageSizeLimit int,
	deliveryChan chan kafka.Event) (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer - %w", err)
	}

	return &KafkaProducer{
		kafkaProducer:    producer,
		messageSizeLimit: messageSizeLimit,
		deliveryChan:     deliveryChan,
	}, nil
}

// KafkaProducer abstracts Confluent Kafka usage.
type KafkaProducer struct {
	kafkaProducer    *kafka.Producer
	messageSizeLimit int // message size limit in bytes
	deliveryChan     chan kafka.Event
}

// Close closes the KafkaProducer.
func (producer *KafkaProducer) Close() {
	producer.kafkaProducer.Flush(producerFlushPeriod) // give the kafka-producer a chance to finish sending
	producer.kafkaProducer.Close()
}

// Producer returns the wrapped Confluent KafkaProducer.
func (producer *KafkaProducer) Producer() *kafka.Producer {
	return producer.kafkaProducer
}

// ProduceAsync sends a message to the kafka brokers asynchronously.
func (producer *KafkaProducer) ProduceAsync(key string, topic string, partition int32, headers []kafka.Header,
	msg []byte) error {
	if len(msg) > producer.messageSizeLimit {
		if err := producer.dismantleAndSendKafkaMessage(key, &topic, partition, headers, msg); err != nil {
			return fmt.Errorf("failed to send message - %w", err)
		}

		return nil
	}

	kafkaMessage := newMessageBuilder().
		topic(&topic, partition).
		key(key).
		headers(headers).
		payload(msg).
		build()

	if err := producer.kafkaProducer.Produce(kafkaMessage, producer.deliveryChan); err != nil {
		return fmt.Errorf("failed to send message - %w", err)
	}

	return nil
}

func (producer *KafkaProducer) dismantleAndSendKafkaMessage(key string, topic *string, partition int32,
	headers []kafka.Header, payload []byte) error {
	dismantlingTime := time.Now().Format(types.TimeFormat)
	dismantlingTimeBytes := []byte(dismantlingTime)

	chunks := producer.splitBufferByLimit(payload)

	for idx, chunk := range chunks {
		messageKey := fmt.Sprintf("%d_%s", idx, key)

		messageBuilder := &messageBuilder{}
		kafkaMessage := messageBuilder.
			topic(topic, partition).
			key(messageKey).
			payload(chunk).
			header(kafka.Header{
				Key: types.HeaderSizeKey, Value: toByteArray(len(payload)),
			}).
			header(kafka.Header{
				Key: types.HeaderOffsetKey, Value: toByteArray(idx * producer.messageSizeLimit),
			}).
			header(kafka.Header{
				Key: types.HeaderDismantlingTimestamp, Value: dismantlingTimeBytes,
			}).
			headers(headers).
			build()

		if err := producer.kafkaProducer.Produce(kafkaMessage, producer.deliveryChan); err != nil {
			return fmt.Errorf("failed to dismantle message - %w", err)
		}
	}

	return nil
}

func (producer *KafkaProducer) splitBufferByLimit(buf []byte) [][]byte {
	var chunk []byte

	chunks := make([][]byte, 0, len(buf)/producer.messageSizeLimit+1)

	for len(buf) >= producer.messageSizeLimit {
		chunk, buf = buf[:producer.messageSizeLimit], buf[producer.messageSizeLimit:]
		chunks = append(chunks, chunk)
	}

	if len(buf) > 0 {
		chunks = append(chunks, buf)
	}

	return chunks
}

func toByteArray(i int) []byte {
	arr := make([]byte, intSize)
	binary.BigEndian.PutUint32(arr[0:4], uint32(i))

	return arr
}
