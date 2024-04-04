package queue

import (
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/IBM/sarama"
)

// kafka 主题消息队列，简单包装kafka接口
type kafkaQueue struct {
	*MQueue
	client   sarama.Client
	producer sarama.SyncProducer
}

func NewKfQueue(client sarama.Client, opts ...Option) *kafkaQueue {
	return &kafkaQueue{
		client: client,
		MQueue: NewMQueue(opts...),
	}
}

func (k *kafkaQueue) AddTopic(topicName string, handle ConsumeHandle, handleErr ConsumeErrHandle) error {
	if _, ok := k.topics[topicName]; ok {
		return nil
	}
	if k.producer == nil {
		producer, err := sarama.NewSyncProducerFromClient(k.client)
		if err != nil {
			return err
		}
		k.producer = producer
	}
	k.MQueue.AddTopic(topicName, handle, handleErr)
	return nil
}

func (k *kafkaQueue) Produce(topicName string, ee ...Entry) error {
	var msg []*sarama.ProducerMessage
	for _, e := range ee {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return err
		}
		kmsg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(e.Key),
			Value: sarama.ByteEncoder(data),
		}
		msg = append(msg, kmsg)
	}
	return k.producer.SendMessages(msg)
}

func (k *kafkaQueue) Consume(topicName string) {
	_, ok := k.topics[topicName]
	if !ok {
		return
	}
	consumer, err := sarama.NewConsumerFromClient(k.client)
	if err != nil && k.topics[topicName].topic.handleErr != nil {
		k.topics[topicName].topic.handleErr(topicName, batchEntry{}, err)
		return
	}

	topicEntry := k.topics[topicName]

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}

	go k.MQueue.Consume(topicName)

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topicName, partition, k.client.Config().Consumer.Offsets.Initial)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		go func(p sarama.PartitionConsumer) {
			for msg := range p.Messages() {
				e := Entry{
					Key:  string(msg.Key),
					Data: msg.Value,
				}
				topicEntry.insertEntry([]Entry{e}, k.cfg.batchSize, unsafe.Pointer(&k.mux))
			}
		}(pc)
	}
}
