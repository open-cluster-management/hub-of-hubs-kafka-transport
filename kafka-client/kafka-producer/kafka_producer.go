package kafkaproducer

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
	"github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const (
	envVarKafkaProducerID        = "KAFKA_PRODUCER_ID"
	envVarKafkaProducerHosts     = "KAFKA_PRODUCER_HOSTS"
	envVarKafkaProducerSSLCA     = "KAFKA_SSL_CA"
	envVarKafkaProducerTopic     = "KAFKA_PRODUCER_TOPIC"
	envVarKafkaProducerPartition = "KAFKA_PRODUCER_PARTITION"
	envVarKafkaProducerAcks      = "KAFKA_PRODUCER_ACKS"
	envVarMessageSizeLimit       = "KAFKA_MESSAGE_SIZE_LIMIT_KB"
	producerFlushPeriod          = 5 * int(time.Second)
	messageSizeLimitUnitSize     = 1000
	decimalBase                  = 10
	partitionNumberSize          = 4
)

var (
	errEnvVarNotFound         = errors.New("not found environment variable")
	errEnvVarIllegalValue     = errors.New("environment variable illegal value")
	errFailedToCreateProducer = errors.New("failed to create kafka producer")
	errFailedToSendMessage    = errors.New("failed to send message")
)

// NewKafkaProducer returns a new instance of KafkaProducer object.
func NewKafkaProducer(delChan chan kafka.Event) (*KafkaProducer, error) {
	p, clientID, hosts, topic, partition, certFileLocation, sizeLimit, err := createKafkaProducer()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFailedToCreateProducer, err)
	}

	return &KafkaProducer{
		key:              []byte(clientID),
		kafkaProducer:    p,
		deliveryChan:     delChan,
		messageSizeLimit: sizeLimit,
		topic:            topic,
		partition:        partition,
		hosts:            hosts,
		caFileLocation:   certFileLocation,
	}, nil
}

// KafkaProducer abstracts Confluent Kafka usage.
type KafkaProducer struct {
	key           []byte
	kafkaProducer *kafka.Producer
	deliveryChan  chan kafka.Event
	// message size limit in bytes
	messageSizeLimit int
	topic            string
	partition        int32
	hosts            string
	caFileLocation   string
}

func createKafkaProducer() (*kafka.Producer, string, string, string, int32, string, int, error) {
	clientID, hosts, topic, partition, acks, ca, sizeLimit, err := readEnvVars()
	if err != nil {
		return nil, "", "", "", 0, "", 0, fmt.Errorf("%w", err)
	}

	if ca != "" {
		certFileLocation := kafkaclient.GetCertificate(&ca)

		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": hosts,
			"security.protocol": "ssl",
			"ssl.ca.location":   certFileLocation,
			"client.id":         clientID,
			"acks":              acks,
		})
		if err != nil {
			return nil, "", "", "", 0, "", 0, fmt.Errorf("%w", err)
		}

		return p, clientID, hosts, topic, partition, certFileLocation, sizeLimit, nil
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
		"client.id":         clientID,
		"acks":              acks,
	})
	if err != nil {
		return nil, "", "", "", 0, "", 0, fmt.Errorf("%w", err)
	}

	return p, clientID, hosts, topic, partition, "", sizeLimit, nil
}

func readEnvVars() (string, string, string, int32, string, string, int, error) {
	id, found := os.LookupEnv(envVarKafkaProducerID)
	if !found {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerID)
	}

	hosts, found := os.LookupEnv(envVarKafkaProducerHosts)
	if !found {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerHosts)
	}

	topic, found := os.LookupEnv(envVarKafkaProducerTopic)
	if !found {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerTopic)
	}

	partitionString, found := os.LookupEnv(envVarKafkaProducerPartition)
	if !found {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerPartition)
	}

	partition, err := strconv.ParseInt(partitionString, decimalBase, partitionNumberSize)
	if err != nil {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarIllegalValue, envVarKafkaProducerPartition)
	}

	acks, found := os.LookupEnv(envVarKafkaProducerAcks)
	if !found {
		acks = "1"
	}

	sizeLimitString, found := os.LookupEnv(envVarMessageSizeLimit)
	if !found {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarMessageSizeLimit)
	}

	sizeLimit, err := strconv.Atoi(sizeLimitString)
	if err != nil {
		return "", "", "", 0, "", "", 0,
			fmt.Errorf("%w: %s", errEnvVarIllegalValue, envVarMessageSizeLimit)
	}

	ca := os.Getenv(envVarKafkaProducerSSLCA)

	return id, hosts, topic, int32(partition), acks, ca, sizeLimit * messageSizeLimitUnitSize, nil
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
func (p *KafkaProducer) ProduceAsync(msg []byte, msgID []byte, msgType []byte, msgVersion []byte) error {
	if len(msg) > p.messageSizeLimit {
		if err := dismantleAndSendKafkaMessage(p, msgID, msgType, msg); err != nil {
			return fmt.Errorf("%w: %v", errFailedToSendMessage, err)
		}

		return nil
	}

	builder := &KafkaMessageBuilder{}

	if err := p.kafkaProducer.Produce(builder.
		Topic(&p.topic, p.partition).
		Key(p.key).
		Payload(msg).
		Header(kafka.Header{
			Key: types.MsgIDKey, Value: msgID,
		}).
		Header(kafka.Header{
			Key: types.MsgTypeKey, Value: msgType,
		}).
		Header(kafka.Header{
			Key: types.MsgVersionKey, Value: msgVersion,
		}).
		Build(),
		p.deliveryChan); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSendMessage, err)
	}

	return nil
}
