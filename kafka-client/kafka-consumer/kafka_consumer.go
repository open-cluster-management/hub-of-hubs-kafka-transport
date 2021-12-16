package kafkaconsumer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaheaders "github.com/open-cluster-management/hub-of-hubs-kafka-transport/headers"
	"github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const pollTimeoutMs = 100

var errHeaderNotFound = errors.New("required message header not found")

// NewKafkaConsumer returns a new instance of KafkaConsumer object.
func NewKafkaConsumer(configMap *kafka.ConfigMap, msgChan chan *kafka.Message,
	log logr.Logger) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer - %w", err)
	}

	return &KafkaConsumer{
		log:              log,
		kafkaConsumer:    c,
		messageAssembler: newKafkaMessageAssembler(),
		msgChan:          msgChan,
		stopChan:         make(chan struct{}, 1),
	}, nil
}

// KafkaConsumer abstracts Confluent Kafka usage.
type KafkaConsumer struct {
	log              logr.Logger
	kafkaConsumer    *kafka.Consumer
	messageAssembler *kafkaMessageAssembler
	msgChan          chan *kafka.Message
	stopChan         chan struct{}
}

// Close closes the KafkaConsumer.
func (c *KafkaConsumer) Close() {
	c.stopChan <- struct{}{}
	close(c.stopChan)
	_ = c.kafkaConsumer.Close()
}

// Consumer returns the wrapped Confluent KafkaConsumer member.
func (c *KafkaConsumer) Consumer() *kafka.Consumer {
	return c.kafkaConsumer
}

// Subscribe starts subscription to the set topic.
func (c *KafkaConsumer) Subscribe(topic string) error {
	if err := c.kafkaConsumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic - %w", err)
	}

	c.log.Info("started listening", "topic", topic)

	go func() {
		for {
			select {
			case <-c.stopChan:
				_ = c.kafkaConsumer.Unsubscribe()
				c.log.Info("stopped listening", "topic", topic)

				return

			default:
				c.pollMessage()
			}
		}
	}()

	return nil
}

func (c *KafkaConsumer) pollMessage() {
	ev := c.kafkaConsumer.Poll(pollTimeoutMs)
	if ev == nil {
		return
	}

	switch msg := ev.(type) {
	case *kafka.Message:
		fragment, msgIsFragment := c.messageIsFragment(msg)
		if !msgIsFragment {
			// fix offset in-case msg landed on a partition with open fragment collections
			c.messageAssembler.fixMessageOffset(msg)
			c.msgChan <- msg

			return
		}

		// wrap in fragment-info
		fragInfo, err := c.createFragmentInfo(msg, fragment)
		if err != nil {
			c.log.Error(err, "failed to read message", "topic", msg.TopicPartition.Topic)
			return
		}

		if assembledMessage, assembled := c.messageAssembler.processFragmentInfo(fragInfo); assembled {
			c.msgChan <- assembledMessage
		}
	case kafka.Error:
		c.log.Info("kafka read error", "code", msg.Code(), "error", msg.Error())
	}
}

// Commit commits a kafka message.
func (c *KafkaConsumer) Commit(msg *kafka.Message) error {
	if _, err := c.kafkaConsumer.CommitMessage(msg); err != nil {
		return fmt.Errorf("failed to commit - %w", err)
	}

	return nil
}

func (c *KafkaConsumer) messageIsFragment(msg *kafka.Message) (*kafkaMessageFragment, bool) {
	offsetHeader, offsetFound := c.lookupHeader(msg, kafkaheaders.Offset)
	_, sizeFound := c.lookupHeader(msg, kafkaheaders.Size)

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

	timestampHeader, found := c.lookupHeader(msg, kafkaheaders.FragmentationTimestamp)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, kafkaheaders.FragmentationTimestamp)
	}

	sizeHeader, found := c.lookupHeader(msg, kafkaheaders.Size)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, kafkaheaders.Size)
	}

	key := fmt.Sprintf("%s_%s", string(msgIDHeader.Value), string(msgTypeHeader.Value))

	timestamp, err := time.Parse(time.RFC3339, string(timestampHeader.Value))
	if err != nil {
		return nil, fmt.Errorf("header (%s) illegal value - %w", kafkaheaders.FragmentationTimestamp, err)
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
