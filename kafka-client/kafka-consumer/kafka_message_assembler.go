package kafkaconsumer

import (
	"context"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
)

func newKafkaMessageAssembler(log logr.Logger,
	fragmentInfoChan chan *kafkaMessageFragmentsInfo, msgChan chan *kafka.Message) *kafkaMessageAssembler {
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
	fragmentInfoChan      chan *kafkaMessageFragmentsInfo
	msgChan               chan *kafka.Message
	lock                  sync.Mutex
}

func (kma *kafkaMessageAssembler) Start(ctx context.Context) {
	go kma.handleFragments(ctx)
}

func (kma *kafkaMessageAssembler) handleFragments(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			kma.log.Info("stopped kafka message assembler")
			return

		case fragInfo := <-kma.fragmentInfoChan:
			fragCollection, found := kma.fragmentCollectionMap[fragInfo.key]
			if !found || fragCollection.dismantlingTimestamp.Before(fragInfo.dismantlingTimestamp) {
				// if collection is not mapped/if a new version is coming in, replace mapping with a fresh collection.
				fragCollection = newKafkaMessageFragmentsCollection(fragInfo.totalSize, fragInfo.dismantlingTimestamp)
				kma.fragmentCollectionMap[fragInfo.key] = fragCollection
				fragCollection.AddFragment(fragInfo)

				continue
			} else if fragCollection.dismantlingTimestamp.After(fragInfo.dismantlingTimestamp) {
				// got an outdated fragment
				continue
			}

			// collection exists and the received fragment package should be added
			fragCollection.AddFragment(fragInfo)

			// check if got all and assemble
			if fragCollection.totalMessageSize == fragCollection.accumulatedFragmentsSize {
				// overtake the collection's latest kafka message, fill it with combined bundle and forward
				fragmentsCount := len(fragCollection.messageFragments)

				assembledBundle, _ := fragCollection.Assemble() // no error because checked assembling size requirement
				fragCollection.latestKafkaMessage.Value = assembledBundle

				// forward assembled bundle onwards (it has fragmented key identifier but complete content)
				kma.msgChan <- fragCollection.latestKafkaMessage

				kma.log.Info("assembled and forwarded fragments collection",
					"collection key", fragInfo.key,
					"collection size (bytes)", fragInfo.totalSize,
					"fragments count", fragmentsCount)

				// delete collection & info
				delete(kma.fragmentCollectionMap, fragInfo.key)
			}
		}
	}
}
