package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	count, size := int32(0), 74
	topic := "belike"
	q := NewMQueue()
	q.AddTopic(topic, func(name string, batch batchEntry) error {
		for _, e := range batch.entries {
			i := e.Data.(int32)
			count += i
		}
		return fmt.Errorf("[%d] errHandle", count)
	}, stdHandleErr)

	go q.Consume(topic)

	for i := 0; i < size; i++ {
		q.Produce(topic, Entry{Data: int32(1)})
	}

	timer := time.NewTimer(time.Second * 3)
	<-timer.C

	q.Wait()
	if count != int32(size) {
		t.Errorf("count: %d, expected: %d", count, size)
	}

	topic = "unlike"
	q.AddTopic(topic, func(name string, batch batchEntry) error {
		for _, e := range batch.entries {
			i := e.Data.(int32)
			count -= i
		}
		time.Sleep(25 * time.Millisecond)
		return nil
	}, nil)

	go q.Consume(topic)

	for i := 0; i < size; i++ {
		q.Produce(topic, Entry{Data: int32(1)})
	}

	timer = time.NewTimer(time.Second * 3)
	<-timer.C

	q.Wait()
	if count != 0 {
		t.Errorf("count: %d, expected: %d", count, 0)
	}
}
