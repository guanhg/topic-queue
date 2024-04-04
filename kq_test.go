package queue

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func Test(t *testing.T) {
	// TODO
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	// config.Producer.RequiredAcks = sarama.WaitForLocal     // Only wait for the leader to ack
	// config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	config.Producer.Flush.Frequency = 5 * time.Millisecond // Flush batches every 500ms
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		t.Error(err)
	}
	topicName := "belike"
	kq := NewKfQueue(client, WithBatchSize(5))
	kq.AddTopic(topicName, func(topicName string, batch batchEntry) error {
		for _, e := range batch.entries {
			var i int
			if err := json.Unmarshal(e.Data.([]byte), &i); err != nil {
				t.Error(err)
			}

			fmt.Printf("%s: %d ", e.Key, i)
		}
		fmt.Println("batch: ", len(batch.entries))
		return nil
	}, stdHandleErr)

	go kq.Consume(topicName)

	for i := 0; i < 4; i++ {
		if err := kq.Produce(topicName, Entry{Key: "kmsg", Data: time.Now().Nanosecond()}); err != nil {
			t.Error(err)
		}
	}

	timer := time.NewTimer(time.Second * 3)
	<-timer.C

	kq.Wait()
}
