package kafkaconsumer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const (
	pollTimeoutMs                 = 100
	assemblerBufferedChannelsSize = 100
)

var (
	errFailedToCreateConsumer = errors.New("failed to create kafka consumer")
	errFailedToSubscribe      = errors.New("failed to subscribe to topic")
	errHeaderNotFound         = errors.New("required message header not found")
	errHeaderIllegalValue     = errors.New("message header has an illegal value")
)

// NewKafkaConsumer returns a new instance of KafkaConsumer object.
func NewKafkaConsumer(configMap *kafka.ConfigMap, msgChan chan *kafka.Message,
	log logr.Logger) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("%w - %v", errFailedToCreateConsumer, err)
	}

	// create channel for passing received bundle fragments
	fragmentInfoChan := make(chan *kafkaMessageFragmentInfo, assemblerBufferedChannelsSize)

	// create fragments handler
	messageAssembler := newKafkaMessageAssembler(log, fragmentInfoChan, msgChan)
	messageAssembler.start()

	return &KafkaConsumer{
		log:              log,
		kafkaConsumer:    c,
		messageAssembler: messageAssembler,
		msgChan:          msgChan,
		fragmentInfoChan: fragmentInfoChan,
		stopChan:         make(chan struct{}, 1),
	}, nil
}

// KafkaConsumer abstracts Confluent Kafka usage.
type KafkaConsumer struct {
	log              logr.Logger
	kafkaConsumer    *kafka.Consumer
	messageAssembler *kafkaMessageAssembler
	msgChan          chan *kafka.Message
	fragmentInfoChan chan *kafkaMessageFragmentInfo
	stopChan         chan struct{}
}

// Close closes the KafkaConsumer.
func (c *KafkaConsumer) Close() {
	go func() {
		c.stopChan <- struct{}{}
		close(c.stopChan)
	}()

	c.messageAssembler.stop()
	_ = c.kafkaConsumer.Close()
}

// Consumer returns the wrapped Confluent KafkaConsumer member.
func (c *KafkaConsumer) Consumer() *kafka.Consumer {
	return c.kafkaConsumer
}

// Subscribe starts subscription to the set topic.
func (c *KafkaConsumer) Subscribe(topics []string) error {
	if err := c.kafkaConsumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("%w: %v", errFailedToSubscribe, err)
	}

	c.log.Info("started listening", "topics", topics)

	go func() {
		for {
			select {
			case <-c.stopChan:
				_ = c.kafkaConsumer.Unsubscribe()
				c.log.Info("stopped listening", "topics", topics)

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
						// fix offset in-case msg landed on a partition with open fragment collections
						c.messageAssembler.fixMessageOffset(msg)
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

	timestamp, err := time.Parse(types.TimeFormat, string(timestampHeader.Value))
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
