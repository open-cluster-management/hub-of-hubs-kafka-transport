package kafkaconsumer

import (
	"context"
	"math"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
)

func newKafkaMessageAssembler(log logr.Logger, fragmentInfoChan chan *kafkaMessageFragmentInfo,
	msgChan chan *kafka.Message) *kafkaMessageAssembler {
	return &kafkaMessageAssembler{
		log:                            log,
		partitionFragmentCollectionMap: make(map[int32]map[string]*kafkaMessageFragmentsCollection),
		partitionLowestOffsetMap:       make(map[int32]kafka.Offset),
		fragmentInfoChan:               fragmentInfoChan,
		msgChan:                        msgChan,
		lock:                           sync.Mutex{},
	}
}

type kafkaMessageAssembler struct {
	log                            logr.Logger
	partitionFragmentCollectionMap map[int32]map[string]*kafkaMessageFragmentsCollection
	partitionLowestOffsetMap       map[int32]kafka.Offset
	fragmentInfoChan               chan *kafkaMessageFragmentInfo
	msgChan                        chan *kafka.Message
	lock                           sync.Mutex
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

	partition := fragInfo.kafkaMessage.TopicPartition.Partition

	fragmentCollectionMap, found := assembler.partitionFragmentCollectionMap[partition]
	if !found {
		fragmentCollectionMap = make(map[string]*kafkaMessageFragmentsCollection)
		assembler.partitionFragmentCollectionMap[partition] = fragmentCollectionMap
	}

	fragCollection, found := fragmentCollectionMap[fragInfo.key]
	if !found || fragCollection.dismantlingTimestamp.Before(fragInfo.dismantlingTimestamp) {
		// fragmentCollection not found or is hosting outdated fragments
		fragCollection := newKafkaMessageFragmentsCollection(fragInfo.totalSize, fragInfo.dismantlingTimestamp)
		fragmentCollectionMap[fragInfo.key] = fragCollection
		fragCollection.AddFragment(fragInfo)

		// update lowest-offset on partition
		if lowestOffset, found := assembler.partitionLowestOffsetMap[partition]; !found || (found &&
			fragCollection.lowestOffset < lowestOffset) {
			assembler.partitionLowestOffsetMap[partition] = fragCollection.lowestOffset
		}

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

func (assembler *kafkaMessageAssembler) assembleAndForwardCollection(key string,
	collection *kafkaMessageFragmentsCollection) {
	assembledBundle, _ := collection.Assemble() // no error because checked assembling size requirement
	collection.latestKafkaMessage.Value = assembledBundle
	partition := collection.latestKafkaMessage.TopicPartition.Partition

	// delete collection from map
	delete(assembler.partitionFragmentCollectionMap[partition], key)

	// update message offset: set it to that of the lowest (incomplete) collection's offset on partition, or to this
	// collection's highest offset if it is the lowest on partition.
	if lowestOffset := assembler.partitionLowestOffsetMap[partition]; lowestOffset != collection.lowestOffset {
		collection.latestKafkaMessage.TopicPartition.Offset = lowestOffset
	} else {
		// update partition offset cache map
		if newLowestOffset := assembler.findLowestOffsetOnPartition(partition); newLowestOffset < 0 {
			// no collection left on partition
			delete(assembler.partitionLowestOffsetMap, partition)
		} else {
			assembler.partitionLowestOffsetMap[partition] = newLowestOffset
		}
	}

	// forward assembled bundle onwards (it has fragmented key identifier but complete content)
	go func() {
		assembler.msgChan <- collection.latestKafkaMessage
	}()

	assembler.log.Info("assembled and forwarded fragments collection",
		"collection key", key,
		"collection size (bytes)", collection.totalMessageSize,
		"fragments count", len(collection.messageFragments))
}

// FixMessageOffset corrects the offset of the received message so that it does not allow for unsafe committing.
func (assembler *kafkaMessageAssembler) FixMessageOffset(msg *kafka.Message) {
	assembler.lock.Lock()
	defer assembler.lock.Unlock()

	partition := msg.TopicPartition.Partition

	lowestOffsetOnPartition, found := assembler.partitionLowestOffsetMap[partition]
	if !found {
		// no collection on map
		return
	}
	// fix offset so that it does not allow for unsafe committing after processing.
	msg.TopicPartition.Offset = lowestOffsetOnPartition
}

func (assembler *kafkaMessageAssembler) findLowestOffsetOnPartition(partition int32) kafka.Offset {
	var lowest kafka.Offset = math.MaxInt64

	if collectionMap, found := assembler.partitionFragmentCollectionMap[partition]; found {
		for _, collection := range collectionMap {
			if collection.lowestOffset < lowest {
				lowest = collection.lowestOffset
			}
		}
	} else {
		return -1
	}

	return lowest
}
