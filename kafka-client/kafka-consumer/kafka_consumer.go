package kafkaconsumer

import (
	"errors"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
)

const (
	// required env vars.
	envVarKafkaConsumerID    = "KAFKA_CONSUMER_ID"
	envVarKafkaConsumerHosts = "KAFKA_CONSUMER_HOSTS"

	// pre-set env vars.
	envVarKafkaConsumerSSLCA = "KAFKA_SSL_CA"
	envVarKafkaConsumerTopic = "KAFKA_CONSUMER_TOPIC"
)

var (
	errEnvVarNotFound         = errors.New("not found environment variable")
	errFailedToCreateConsumer = errors.New("failed to create kafka consumer")
	errFailedToSubscribe      = errors.New("failed to subscribe to topic")
)

// NewKafkaConsumer returns a new instance of KafkaConsumer object.
func NewKafkaConsumer(msgChan chan *kafka.Message, log logr.Logger) (*KafkaConsumer, error) {
	c, topic, err := createKafkaConsumer()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFailedToCreateConsumer, err)
	}

	return &KafkaConsumer{
		kafkaConsumer: c,
		topic:         topic,
		msgChan:       msgChan,
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
	clientID, hosts, topic, ca, err := readEnvVars()
	if err != nil {
		return nil, "", fmt.Errorf("%w", err)
	}

	if ca != "" {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  hosts,
			"security.protocol":  "ssl",
			"ssl.ca.location":    kafkaclient.GetCertificate(&ca),
			"client.id":          clientID,
			"group.id":           clientID,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "false",
		})
		if err != nil {
			return nil, "", fmt.Errorf("%w", err)
		}

		return c, topic, nil
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  hosts,
		"client.id":          clientID,
		"group.id":           clientID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return nil, "", fmt.Errorf("%w", err)
	}

	return c, topic, nil
}

func readEnvVars() (string, string, string, string, error) {
	id, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	hosts, found := os.LookupEnv(envVarKafkaConsumerHosts)
	if !found {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerHosts)
	}

	topic, found := os.LookupEnv(envVarKafkaConsumerTopic)
	if !found {
		return "", "", "", "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerTopic)
	}

	ca := os.Getenv(envVarKafkaConsumerSSLCA)

	return id, hosts, topic, ca, nil
}

// Close closes the KafkaConsumer.
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
func (c *KafkaConsumer) Subscribe() error {
	if err := c.kafkaConsumer.Subscribe(c.topic, nil); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSubscribe, err)
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
