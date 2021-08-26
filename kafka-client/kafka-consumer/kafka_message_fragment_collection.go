package kafkaconsumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var errFragmentsAreIncomplete = fmt.Errorf("some fragments are missing")

func newKafkaMessageFragmentsCollection(size uint32, timestamp time.Time) *kafkaMessageFragmentsCollection {
	return &kafkaMessageFragmentsCollection{
		totalMessageSize:         size,
		accumulatedFragmentsSize: 0,
		dismantlingTimestamp:     timestamp,
		messageFragments:         make(map[uint32]*kafkaMessageFragment),
		latestKafkaMessage:       nil,
		highestOffset:            0,
		lowestOffset:             -1,
		lock:                     sync.Mutex{},
	}
}

// kafkaMessageFragmentsCollection holds a collection of kafkaMessageFragment and maintains it until completion.
type kafkaMessageFragmentsCollection struct {
	totalMessageSize         uint32
	accumulatedFragmentsSize uint32
	dismantlingTimestamp     time.Time
	messageFragments         map[uint32]*kafkaMessageFragment

	latestKafkaMessage *kafka.Message
	highestOffset      kafka.Offset
	lowestOffset       kafka.Offset
	lock               sync.Mutex
}

// kafkaMessageFragment represents one fragment of a kafka message.
type kafkaMessageFragment struct {
	offset uint32
	bytes  []byte
}

// kafkaMessageFragmentInfo wraps a fragment with info to pass in channels.
type kafkaMessageFragmentInfo struct {
	key                  string
	totalSize            uint32
	dismantlingTimestamp time.Time
	fragment             *kafkaMessageFragment
	kafkaMessage         *kafka.Message
}

func (fc *kafkaMessageFragmentsCollection) AddFragment(fragInfo *kafkaMessageFragmentInfo) {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if fragInfo.fragment.offset > fc.totalMessageSize {
		return
	}

	// add fragment to collection, don't write if already exists.
	if _, found := fc.messageFragments[fragInfo.fragment.offset]; !found {
		fc.messageFragments[fragInfo.fragment.offset] = fragInfo.fragment
	}

	// update accumulated size.
	fc.accumulatedFragmentsSize += uint32(len(fragInfo.fragment.bytes))

	// update offsets.
	if fragInfo.kafkaMessage.TopicPartition.Offset >= fc.highestOffset {
		fc.latestKafkaMessage = fragInfo.kafkaMessage
		fc.highestOffset = fragInfo.kafkaMessage.TopicPartition.Offset
	}

	if fc.lowestOffset == -1 || fragInfo.kafkaMessage.TopicPartition.Offset <= fc.lowestOffset {
		fc.lowestOffset = fragInfo.kafkaMessage.TopicPartition.Offset
	}
}

// Assemble assembles the collection into one bundle.
// This function only runs when totalMessageSize == accumulatedFragmentsSize.
func (fc *kafkaMessageFragmentsCollection) Assemble() ([]byte, error) {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if fc.totalMessageSize != fc.accumulatedFragmentsSize {
		return nil, errFragmentsAreIncomplete
	}

	buf := make([]byte, fc.totalMessageSize)

	for offset, frag := range fc.messageFragments {
		copy(buf[offset:], frag.bytes)
		frag.bytes = nil // faster GC
	}

	return buf, nil
}
