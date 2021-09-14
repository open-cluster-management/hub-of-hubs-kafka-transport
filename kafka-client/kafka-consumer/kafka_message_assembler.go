package kafkaconsumer

import (
	"context"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
)

func newKafkaMessageAssembler(log logr.Logger, fragmentInfoChan chan *kafkaMessageFragmentInfo,
	msgChan chan *kafka.Message) *kafkaMessageAssembler {
	return &kafkaMessageAssembler{
		log:                   log,
		fragmentCollectionMap: make(map[string]*kafkaMessageFragmentsCollection),
		fragmentInfoChan:      fragmentInfoChan,
		msgChan:               msgChan,
		lock:                  sync.Mutex{},
	}
}

type kafkaMessageAssembler struct {
	log                   logr.Logger
	fragmentCollectionMap map[string]*kafkaMessageFragmentsCollection
	fragmentInfoChan      chan *kafkaMessageFragmentInfo
	msgChan               chan *kafka.Message
	lock                  sync.Mutex
}

func (assembler *kafkaMessageAssembler) Start(ctx context.Context) {
	go assembler.handleFragments(ctx)
}

func (assembler *kafkaMessageAssembler) handleFragments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			assembler.log.Info("stopped kafka message assembler - fragments handler")
			return

		case fragInfo := <-assembler.fragmentInfoChan:
			assembler.processFragmentInfo(fragInfo)
		}
	}
}

func (assembler *kafkaMessageAssembler) processFragmentInfo(fragInfo *kafkaMessageFragmentInfo) {
	assembler.lock.Lock()
	defer assembler.lock.Unlock()

	fragCollection, found := assembler.fragmentCollectionMap[fragInfo.key]
	if !found || fragCollection.dismantlingTimestamp.Before(fragInfo.dismantlingTimestamp) {
		fragCollection := newKafkaMessageFragmentsCollection(fragInfo.totalSize, fragInfo.dismantlingTimestamp)
		assembler.fragmentCollectionMap[fragInfo.key] = fragCollection
		fragCollection.AddFragment(fragInfo)

		return
	} else if fragCollection.dismantlingTimestamp.After(fragInfo.dismantlingTimestamp) {
		// fragment timestamp < collection timestamp got an outdated fragment
		return
	}

	// collection exists and the received fragment package should be added
	fragCollection.AddFragment(fragInfo)

	// check if got all and assemble
	if fragCollection.totalMessageSize == fragCollection.accumulatedFragmentsSize {
		assembler.assembleAndForwardCollection(fragInfo.key, fragCollection)
	}
}

func (assembler *kafkaMessageAssembler) CanCommitMessage(key string, message *kafka.Message) bool {
	assembler.lock.Lock()
	defer assembler.lock.Unlock()

	if collection, found := assembler.fragmentCollectionMap[key]; found {
		delete(assembler.fragmentCollectionMap, key)
		return assembler.checkIfCollectionCanBeCommitted(collection.lowestOffset, collection.highestOffset,
			message.TopicPartition.Partition)
	}

	// key is not found then this is a processed collection / a regular message. treat it like a point
	return assembler.checkIfCollectionCanBeCommitted(message.TopicPartition.Offset, message.TopicPartition.Offset,
		message.TopicPartition.Partition)
}

func (assembler *kafkaMessageAssembler) assembleAndForwardCollection(key string,
	collection *kafkaMessageFragmentsCollection) {
	// overtake the collection's latest kafka message, fill it with combined bundle and forward
	fragmentsCount := len(collection.messageFragments)

	assembledBundle, _ := collection.Assemble() // no error because checked assembling size requirement
	collection.latestKafkaMessage.Value = assembledBundle

	// forward assembled bundle onwards (it has fragmented key identifier but complete content)
	assembler.msgChan <- collection.latestKafkaMessage

	assembler.log.Info("assembled and forwarded fragments collection",
		"collection key", key,
		"collection size (bytes)", collection.totalMessageSize,
		"fragments count", fragmentsCount)

	// release fragments but keep collection info for commit
	collection.messageFragments = nil
}

func (assembler *kafkaMessageAssembler) checkIfCollectionCanBeCommitted(startOffset, endOffset kafka.Offset,
	partition int32) bool {
	// check if the collection's [start, end] intersects with any other collection's.
	for _, collection := range assembler.fragmentCollectionMap {
		// the following condition assumes that each range's end >= start.
		// proof: https://stackoverflow.com/a/325964
		if collection.latestKafkaMessage.TopicPartition.Partition == partition &&
			startOffset <= collection.highestOffset && endOffset >= collection.lowestOffset {
			return false
		}
	}

	return true
}
