package kafkaproducer

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
)

const (
	envVarKafkaProducerID    = "KAFKA_PRODUCER_ID"
	envVarKafkaProducerHosts = "KAFKA_PRODUCER_HOSTS"
	envVarKafkaProducerSSLCA = "KAFKA_SSL_CA"
	envVarKafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
	envVarKafkaProducerAcks  = "KAFKA_PRODUCER_ACKS"
	producerFlushPeriod      = 5 * int(time.Second)
)

var (
	errEnvVarNotFound         = errors.New("not found environment variable")
	errFailedToCreateProducer = errors.New("failed to create kafka producer")
	errFailedToSendMessage    = errors.New("failed to send message")
)

// NewKafkaProducer returns a new instance of KafkaProducer object.
func NewKafkaProducer(delChan chan kafka.Event) (*KafkaProducer, error) {
	p, clientID, topic, err := createKafkaProducer()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFailedToCreateProducer, err)
	}

	return &KafkaProducer{
		key:           []byte(clientID),
		kafkaProducer: p,
		topic:         topic,
		deliveryChan:  delChan,
	}, nil
}

// KafkaProducer abstracts Confluent Kafka usage.
type KafkaProducer struct {
	key           []byte
	kafkaProducer *kafka.Producer
	deliveryChan  chan kafka.Event
	topic         string
}

func createKafkaProducer() (*kafka.Producer, string, string, error) {
	clientID, hosts, topic, acks, ca, err := readEnvVars()
	if err != nil {
		return nil, "", "", fmt.Errorf("%w", err)
	}

	if ca != "" {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": hosts,
			"security.protocol": "ssl",
			"ssl.ca.location":   kafkaclient.GetCertificate(&ca),
			"client.id":         clientID,
			"acks":              acks,
		})
		if err != nil {
			return nil, "", "", fmt.Errorf("%w", err)
		}

		return p, clientID, topic, nil
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
		"client.id":         clientID,
		"acks":              acks,
	})
	if err != nil {
		return nil, "", "", fmt.Errorf("%w", err)
	}

	return p, clientID, topic, nil
}

func readEnvVars() (string, string, string, string, string, error) {
	id, found := os.LookupEnv(envVarKafkaProducerID)
	if !found {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerID)
	}

	hosts, found := os.LookupEnv(envVarKafkaProducerHosts)
	if !found {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerHosts)
	}

	topic, found := os.LookupEnv(envVarKafkaProducerTopic)
	if !found {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaProducerTopic)
	}

	acks, found := os.LookupEnv(envVarKafkaProducerAcks)
	if !found {
		acks = "1"
	}

	ca := os.Getenv(envVarKafkaProducerSSLCA)

	return id, hosts, topic, acks, ca, nil
}

// Close closes the KafkaProducer.
func (p *KafkaProducer) Close() {
	p.kafkaProducer.Flush(producerFlushPeriod) // give the kafka-producer 5 secs to send all
	p.kafkaProducer.Close()
}

// Producer returns the wrapped Confluent KafkaProducer member.
func (p *KafkaProducer) Producer() *kafka.Producer {
	return p.kafkaProducer
}

// ProduceAsync sends a message to the kafka brokers asynchronously.
func (p *KafkaProducer) ProduceAsync(msg *[]byte) error {
	builder := &KMBuilder{}

	if err := p.kafkaProducer.Produce(builder.
		Topic(&p.topic, kafka.PartitionAny).
		Key(&p.key).
		Payload(msg).
		Build(),
		p.deliveryChan); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSendMessage, err)
	}

	return nil
}
