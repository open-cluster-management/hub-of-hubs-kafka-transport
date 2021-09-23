package kafkaconsumer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
	"github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const (
	envVarKafkaConsumerID         = "KAFKA_CONSUMER_ID"
	envVarKafkaConsumerHosts      = "KAFKA_CONSUMER_HOSTS"
	envVarKafkaConsumerSSLCA      = "KAFKA_SSL_CA"
	envVarKafkaConsumerTopic      = "KAFKA_CONSUMER_TOPIC"
	pollTimeoutMs                 = 100
	assemblerBufferedChannelsSize = 100
)

var (
	errEnvVarNotFound         = errors.New("not found environment variable")
	errFailedToCreateConsumer = errors.New("failed to create kafka consumer")
	errFailedToSubscribe      = errors.New("failed to subscribe to topic")
	errHeaderNotFound         = errors.New("required message header not found")
	errHeaderIllegalValue     = errors.New("message header has an illegal value")
	errCommitRejected         = errors.New("commit rejected")
)

// NewKafkaConsumer returns a new instance of KafkaConsumer object.
func NewKafkaConsumer(msgChan chan *kafka.Message, log logr.Logger) (*KafkaConsumer, error) {
	c, topics, err := createKafkaConsumer()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errFailedToCreateConsumer, err)
	}

	// create context for sub-routines
	ctx, cancelContext := context.WithCancel(context.Background())

	// create channel for passing received bundle fragments
	fragmentInfoChan := make(chan *kafkaMessageFragmentInfo, assemblerBufferedChannelsSize)

	// create fragments handler
	messageAssembler := newKafkaMessageAssembler(log, fragmentInfoChan, msgChan)
	messageAssembler.Start(ctx)

	return &KafkaConsumer{
		log:              log,
		kafkaConsumer:    c,
		messageAssembler: messageAssembler,
		topics:           topics,
		msgChan:          msgChan,
		fragmentInfoChan: fragmentInfoChan,
		ctx:              ctx,
		cancelContext:    cancelContext,
	}, nil
}

// KafkaConsumer abstracts Confluent Kafka usage.
type KafkaConsumer struct {
	log              logr.Logger
	kafkaConsumer    *kafka.Consumer
	messageAssembler *kafkaMessageAssembler
	topics           []string
	msgChan          chan *kafka.Message
	fragmentInfoChan chan *kafkaMessageFragmentInfo
	ctx              context.Context
	cancelContext    context.CancelFunc
}

func createKafkaConsumer() (*kafka.Consumer, []string, error) {
	clientID, hosts, topics, ca, err := readEnvVars()
	if err != nil {
		return nil, nil, fmt.Errorf("%w", err)
	}

	if ca != "" {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  hosts,
			"security.protocol":  "ssl",
			"ssl.ca.location":    kafkaclient.GetCertificate(&ca),
			"client.id":          clientID,
			"group.id":           clientID,
			"auto.offset.reset":  "latest",
			"enable.auto.commit": "false",
		})
		if err != nil {
			return nil, nil, fmt.Errorf("%w", err)
		}

		return c, topics, nil
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  hosts,
		"client.id":          clientID,
		"group.id":           clientID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return nil, nil, fmt.Errorf("%w", err)
	}

	return c, topics, nil
}

func readEnvVars() (string, string, []string, string, error) {
	id, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return "", "", nil, "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	hosts, found := os.LookupEnv(envVarKafkaConsumerHosts)
	if !found {
		return "", "", nil, "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerHosts)
	}

	topicString, found := os.LookupEnv(envVarKafkaConsumerTopic)
	if !found {
		return "", "", nil, "",
			fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerTopic)
	}

	topics := strings.Split(topicString, ",")

	ca := os.Getenv(envVarKafkaConsumerSSLCA)

	return id, hosts, topics, ca, nil
}

// Close closes the KafkaConsumer.
func (c *KafkaConsumer) Close() {
	c.cancelContext()
	_ = c.kafkaConsumer.Close()
}

// Consumer returns the wrapped Confluent KafkaConsumer member.
func (c *KafkaConsumer) Consumer() *kafka.Consumer {
	return c.kafkaConsumer
}

// Subscribe starts subscription to the set topic.
func (c *KafkaConsumer) Subscribe() error {
	if err := c.kafkaConsumer.SubscribeTopics(c.topics, nil); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSubscribe, err)
	}

	c.log.Info("started listening", "topics", c.topics)

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				_ = c.kafkaConsumer.Unsubscribe()
				c.log.Info("stopped listening", "topics", c.topics)
				return

			default:
				ev := c.kafkaConsumer.Poll(pollTimeoutMs)
				if ev == nil {
					continue
				}

				switch msg := ev.(type) {
				case *kafka.Message:
					fragment, msgIsFragment := c.messageIsFragment(msg)
					if !msgIsFragment {
						c.msgChan <- msg
						continue
					}

					// wrap in fragment-info
					fragInfo, err := c.createFragmentInfo(msg, fragment)
					if err != nil {
						c.log.Error(err, "failed to read message",
							"topic", msg.TopicPartition.Topic)

						continue
					}

					c.fragmentInfoChan <- fragInfo
				case kafka.Error:
					c.log.Info("kafka read error", "code", msg.Code(), "error", msg.Error())
				}
			}
		}
	}()

	return nil
}

// Commit commits a kafka message.
func (c *KafkaConsumer) Commit(msg *kafka.Message) error {
	msgIDHeader, found := c.lookupHeader(msg, types.MsgIDKey)
	if !found {
		return fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgIDKey)
	}

	msgTypeHeader, found := c.lookupHeader(msg, types.MsgTypeKey)
	if !found {
		return fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgTypeKey)
	}

	// if a consumer client is attempting to commit a message then it's safe to assume that the message was processed.
	key := fmt.Sprintf("%s_%s", string(msgIDHeader.Value), string(msgTypeHeader.Value))
	if !c.messageAssembler.CanCommitMessage(key, msg) {
		return fmt.Errorf("%w : %s", errCommitRejected, "message is an assembled collection that succeeds "+
			"other unprocessed fragments")
	}

	if _, err := c.kafkaConsumer.CommitMessage(msg); err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

func (c *KafkaConsumer) messageIsFragment(msg *kafka.Message) (*kafkaMessageFragment, bool) {
	offsetHeader, offsetFound := c.lookupHeader(msg, types.HeaderOffsetKey)
	_, sizeFound := c.lookupHeader(msg, types.HeaderSizeKey)

	if !(offsetFound && sizeFound) {
		return nil, false
	}

	return &kafkaMessageFragment{
		offset: binary.BigEndian.Uint32(offsetHeader.Value),
		bytes:  msg.Value,
	}, true
}

func (c *KafkaConsumer) lookupHeader(msg *kafka.Message, headerKey string) (*kafka.Header, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return &header, true
		}
	}

	return nil, false
}

func (c *KafkaConsumer) createFragmentInfo(msg *kafka.Message,
	fragment *kafkaMessageFragment) (*kafkaMessageFragmentInfo, error) {
	msgIDHeader, found := c.lookupHeader(msg, types.MsgIDKey)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgIDKey)
	}

	msgTypeHeader, found := c.lookupHeader(msg, types.MsgTypeKey)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgTypeKey)
	}

	timestampHeader, found := c.lookupHeader(msg, types.HeaderDismantlingTimestamp)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.HeaderDismantlingTimestamp)
	}

	sizeHeader, found := c.lookupHeader(msg, types.HeaderSizeKey)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.HeaderSizeKey)
	}

	key := fmt.Sprintf("%s_%s", string(msgIDHeader.Value), string(msgTypeHeader.Value))

	timestamp, err := time.Parse(time.RFC3339, string(timestampHeader.Value))
	if err != nil {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderIllegalValue, types.HeaderDismantlingTimestamp)
	}

	size := binary.BigEndian.Uint32(sizeHeader.Value)

	return &kafkaMessageFragmentInfo{
		key:                  key,
		totalSize:            size,
		dismantlingTimestamp: timestamp,
		fragment:             fragment,
		kafkaMessage:         msg,
	}, nil
}
