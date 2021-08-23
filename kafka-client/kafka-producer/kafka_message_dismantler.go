package kafkaproducer

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	types "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
)

const intSize = 4

var errFailedToDismantleMessage = fmt.Errorf("failed to dismantle message")

func dismantleAndSendKafkaMessage(producer *KafkaProducer,
	msgID []byte, msgType []byte, payload []byte) error {
	dismantlingTime := time.Now().Format(types.TimeFormat)
	dismantlingTimeBytes := []byte(dismantlingTime)

	chunks := splitBufferByLimit(payload, producer.messageSizeLimit)

	for idx, chunk := range chunks {
		messageKey := fmt.Sprintf("%d_%s", idx, string(producer.key))
		messageKeyBytes := []byte(messageKey)

		messageBuilder := &KafkaMessageBuilder{}
		kafkaMessage := messageBuilder.
			Topic(&producer.topic, kafka.PartitionAny).
			Key(messageKeyBytes).
			Payload(chunk).
			Header(kafka.Header{
				Key: types.MsgTypeKey, Value: msgType,
			}).
			Header(kafka.Header{
				Key: types.MsgIDKey, Value: msgID,
			}).
			Header(kafka.Header{
				Key: types.HeaderSizeKey, Value: toByteArray(len(payload)),
			}).
			Header(kafka.Header{
				Key: types.HeaderOffsetKey, Value: toByteArray(idx * producer.messageSizeLimit),
			}).
			Header(kafka.Header{
				Key: types.HeaderDismantlingTimestamp, Value: dismantlingTimeBytes,
			}).
			Build()

		if err := producer.kafkaProducer.Produce(kafkaMessage, producer.deliveryChan); err != nil {
			return fmt.Errorf("%w: %v", errFailedToDismantleMessage, err)
		}
	}

	return nil
}

func splitBufferByLimit(buf []byte, lim int) [][]byte {
	var chunk []byte

	chunks := make([][]byte, 0, len(buf)/lim+1)

	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}

	if len(buf) > 0 {
		chunks = append(chunks, buf)
	}

	return chunks
}

func toByteArray(i int) []byte {
	arr := make([]byte, intSize)
	binary.BigEndian.PutUint32(arr[0:4], uint32(i))

	return arr
}
