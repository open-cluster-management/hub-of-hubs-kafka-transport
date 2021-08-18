package kafkaproducer

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
	"os"
	"time"
)

const (
	//required env vars
	kafkaProducerId    = "KAFKA_PRODUCER_ID"
	kafkaProducerHosts = "KAFKA_PRODUCER_HOSTS"
	kafkaProducerSSLCA = "KAFKA_SSL_CA"
	kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"

	//pre-set env vars
	kafkaProducerAcks = "KAFKA_PRODUCER_ACKS"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// NewKafkaProducer returns a new instance of KafkaProducer object.
func NewKafkaProducer(delChan chan kafka.Event) (*KafkaProducer, error) {
	p, clientId, topic, err := createKafkaProducer()
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		key:           []byte(clientId),
		kafkaProducer: p,
		topic:         topic,
		deliveryChan:  delChan,
	}, nil
}

// KafkaProducer abstracts Confluent Kafka usage.
type KafkaProducer struct {
	key           []byte
	kafkaProducer *kafka.Producer
	topic         string
	deliveryChan  chan kafka.Event
}

func createKafkaProducer() (*kafka.Producer, string, string, error) {
	clientId, hosts, topic, acks, ca, err := readEnvVars()
	if err != nil {
		return nil, "", "", err
	}

	if ca != "" {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": hosts,
			"security.protocol": "ssl",
			"ssl.ca.location":   kafkaclient.GetCertificate(&ca),
			"client.id":         clientId,
			"acks":              acks})
		if err != nil {
			return nil, "", "", err
		}

		return p, clientId, topic, nil
	} else {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": hosts,
			"client.id":         clientId,
			"acks":              acks})
		if err != nil {
			return nil, "", "", err
		}

		return p, clientId, topic, nil
	}
}

func readEnvVars() (string, string, string, string, string, error) {
	id := os.Getenv(kafkaProducerId)
	if id == "" {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaProducerId)
	}

	hosts := os.Getenv(kafkaProducerHosts)
	if hosts == "" {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaProducerHosts)
	}

	topic := os.Getenv(kafkaProducerTopic)
	if topic == "" {
		return "", "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaProducerTopic)
	}

	acks := os.Getenv(kafkaProducerAcks)
	if acks == "" {
		acks = "1"
	}
	ca := os.Getenv(kafkaProducerSSLCA)

	return id, hosts, topic, acks, ca, nil
}

// Close closes the KafkaProducer
func (p *KafkaProducer) Close() {
	p.kafkaProducer.Flush(5 * int(time.Second)) //give the kafka-producer 5 secs to send all
	p.kafkaProducer.Close()
}

// Producer returns the wrapped Confluent KafkaProducer member.
func (p *KafkaProducer) Producer() *kafka.Producer {
	return p.kafkaProducer
}

// ProduceAsync sends a message to the kafka brokers asynchronously.
func (p *KafkaProducer) ProduceAsync(msg *[]byte) error {
	builder := &KMBuilder{}

	return p.kafkaProducer.Produce(builder.
		Topic(&p.topic, kafka.PartitionAny).
		Key(&p.key).
		Payload(msg).
		Build(),
		p.deliveryChan)
}
