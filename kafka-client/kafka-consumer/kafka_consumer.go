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

// NewKafkaConsumer returns a new instance of KafkaConsumer.
func NewKafkaConsumer(configMap *kafka.ConfigMap, msgChan chan *kafka.Message,
	log logr.Logger) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer - %w", err)
	}

	return &KafkaConsumer{
		log:              log,
		kafkaConsumer:    consumer,
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
func (consumer *KafkaConsumer) Close() {
	consumer.stopChan <- struct{}{}
	close(consumer.stopChan)
	_ = consumer.kafkaConsumer.Close()
}

// Consumer returns the wrapped Confluent KafkaConsumer.
func (consumer *KafkaConsumer) Consumer() *kafka.Consumer {
	return consumer.kafkaConsumer
}

// Subscribe subscribes consumer to the given topic.
func (consumer *KafkaConsumer) Subscribe(topic string) error {
	if err := consumer.kafkaConsumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic - %w", err)
	}

	consumer.log.Info("started listening", "topic", topic)

	go func() {
		for {
			select {
			case <-consumer.stopChan:
				_ = consumer.kafkaConsumer.Unsubscribe()
				consumer.log.Info("stopped listening", "topic", topic)

				return

			default:
				consumer.pollMessage()
			}
		}
	}()

	return nil
}

func (consumer *KafkaConsumer) pollMessage() {
	event := consumer.kafkaConsumer.Poll(pollTimeoutMs)
	if event == nil {
		return
	}

	switch msg := event.(type) {
	case *kafka.Message:
		fragment, msgIsFragment := consumer.messageIsFragment(msg)
		if !msgIsFragment {
			// fix offset in-case msg landed on a partition with open fragment collections
			consumer.messageAssembler.fixMessageOffset(msg)
			consumer.msgChan <- msg

			return
		}

		// wrap in fragment-info
		fragInfo, err := consumer.createFragmentInfo(msg, fragment)
		if err != nil {
			consumer.log.Error(err, "failed to read message", "topic", msg.TopicPartition.Topic)
			return
		}

		if assembledMessage, assembled := consumer.messageAssembler.processFragmentInfo(fragInfo); assembled {
			consumer.msgChan <- assembledMessage
		}
	case kafka.Error:
		consumer.log.Info("kafka read error", "code", msg.Code(), "error", msg.Error())
	}
}

// Commit commits a kafka message.
func (consumer *KafkaConsumer) Commit(msg *kafka.Message) error {
	if _, err := consumer.kafkaConsumer.CommitMessage(msg); err != nil {
		return fmt.Errorf("failed to commit - %w", err)
	}

	return nil
}

func (consumer *KafkaConsumer) messageIsFragment(msg *kafka.Message) (*kafkaMessageFragment, bool) {
	offsetHeader, offsetFound := consumer.lookupHeader(msg, kafkaheaders.Offset)
	_, sizeFound := consumer.lookupHeader(msg, kafkaheaders.Size)

	if !(offsetFound && sizeFound) {
		return nil, false
	}

	return &kafkaMessageFragment{
		offset: binary.BigEndian.Uint32(offsetHeader.Value),
		bytes:  msg.Value,
	}, true
}

func (consumer *KafkaConsumer) lookupHeader(msg *kafka.Message, headerKey string) (*kafka.Header, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return &header, true
		}
	}

	return nil, false
}

func (consumer *KafkaConsumer) createFragmentInfo(msg *kafka.Message,
	fragment *kafkaMessageFragment) (*kafkaMessageFragmentInfo, error) {
	msgIDHeader, found := consumer.lookupHeader(msg, types.MsgIDKey)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgIDKey)
	}

	msgTypeHeader, found := consumer.lookupHeader(msg, types.MsgTypeKey)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, types.MsgTypeKey)
	}

	timestampHeader, found := consumer.lookupHeader(msg, kafkaheaders.FragmentationTimestamp)
	if !found {
		return nil, fmt.Errorf("%w : header key - %s", errHeaderNotFound, kafkaheaders.FragmentationTimestamp)
	}

	sizeHeader, found := consumer.lookupHeader(msg, kafkaheaders.Size)
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
