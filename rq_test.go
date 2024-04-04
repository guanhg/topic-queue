package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	rds "github.com/redis/go-redis/v9"
)

type msg struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Marrage bool   `json:"marrage"`
}

func TestRdsQueue(t *testing.T) {
	client := rds.NewClient(&rds.Options{Addr: "localhost:6379", Password: "", DB: 0})
	count, size := int32(0), 8
	topic := "belike"
	q := NewRdsQueue(client)
	q.AddTopic(topic, func(name string, batch batchEntry) error {
		time.Sleep(23 * time.Millisecond)
		for _, e := range batch.entries {
			count += 1
			var m msg
			err := json.Unmarshal(([]byte)(e.Data.(string)), &m)
			if err != nil {
				return err
			}
		}

		return nil
	}, stdHandleErr)

	go q.Consume(topic)

	for i := 0; i < size; i++ {
		q.Produce(
			topic,
			Entry{
				Key:  "",
				Data: msg{Name: fmt.Sprintf("msg[%d]", i), Age: rand.Intn(100), Marrage: i%2 == 0}})
	}

	timer := time.NewTimer(time.Second * 3)
	<-timer.C

	q.Wait()

	if count != int32(size) {
		t.Errorf("count: %d, expected: %d", count, size)
	}
}
