package kafkaconsumer

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
	"os"
)

const (
	//required env vars
	kafkaConsumerId    = "KAFKA_CONSUMER_ID"
	kafkaConsumerHosts = "KAFKA_CONSUMER_HOSTS"

	//pre-set env vars
	kafkaConsumerSSLCA = "KAFKA_SSL_CA"
	kafkaConsumerTopic = "KAFKA_CONSUMER_TOPIC"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// NewKafkaConsumer returns a new instance of KafkaConsumer object.
func NewKafkaConsumer(msgsChan chan *kafka.Message, log logr.Logger) (*KafkaConsumer, error) {
	c, topic, err := createKafkaConsumer()
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		kafkaConsumer: c,
		topic:         topic,
		msgChan:       msgsChan,
		stopChan:      make(chan struct{}, 1),
		log:           log,
	}, nil
}

// KafkaConsumer abstracts Confluent Kafka usage.
type KafkaConsumer struct {
	kafkaConsumer *kafka.Consumer
	topic         string
	msgChan       chan *kafka.Message
	stopChan      chan struct{}
	log           logr.Logger
}

func createKafkaConsumer() (*kafka.Consumer, string, error) {
	clientId, hosts, topic, ca, err := readEnvVars()
	if err != nil {
		return nil, "", err
	}

	if ca != "" {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  hosts,
			"security.protocol":  "ssl",
			"ssl.ca.location":    kafkaclient.GetCertificate(&ca),
			"client.id":          clientId,
			"group.id":           clientId,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "false"})
		if err != nil {
			return nil, "", err
		}

		return c, topic, nil
	} else {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  hosts,
			"client.id":          clientId,
			"group.id":           clientId,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "false"})
		if err != nil {
			return nil, "", err
		}

		return c, topic, nil
	}
}

func readEnvVars() (string, string, string, string, error) {
	id := os.Getenv(kafkaConsumerId)
	if id == "" {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaConsumerId)
	}

	hosts := os.Getenv(kafkaConsumerHosts)
	if hosts == "" {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaConsumerHosts)
	}

	topic := os.Getenv(kafkaConsumerTopic)
	if topic == "" {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, kafkaConsumerTopic)
	}

	ca := os.Getenv(kafkaConsumerSSLCA)

	return id, hosts, topic, ca, nil
}

// Close closes the KafkaConsumer
func (c *KafkaConsumer) Close() {
	_ = c.kafkaConsumer.Close()

	c.stopChan <- struct{}{}
	close(c.stopChan)
}

// Consumer returns the wrapped Confluent KafkaConsumer member.
func (c *KafkaConsumer) Consumer() *kafka.Consumer {
	return c.kafkaConsumer
}

// Subscribe starts subscription to the set topic.
func (c *KafkaConsumer) Subscribe(log logr.Logger) error {
	err := c.kafkaConsumer.Subscribe(c.topic, nil)
	if err != nil {
		return err
	}

	c.log.Info("started listening", "Topic", c.topic)

	go func() {
		for {
			select {
			case <-c.stopChan:
				_ = c.kafkaConsumer.Unsubscribe()

				return
			default:
				if msg, readErr := c.kafkaConsumer.ReadMessage(-1); readErr == nil {
					c.msgChan <- msg
				} else {
					c.log.Error(readErr, "failed to read message")
					continue
				}
			}
		}
	}()

	return nil
}
