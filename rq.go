package queue

import (
	"context"
	"encoding/json"
	"time"
	"unsafe"

	rds "github.com/redis/go-redis/v9"
)

// redis模式 主题消息队列
// Produce 使用 内存模式主题消息队列 来缓存消息，等待batchSize的批量消息，依次插入redis.list，key=topicName
// Consume 每个topic最多从redis缓存batchSize个消息到内存中，等消费完后，再从redis中获取

var _ Queue = (*RdsQueue)(nil)

type RdsQueue struct {
	*MQueue
	client *rds.Client
}

const (
	rdsInsertBatchTopic string = "rdsInsertBatchTopic"
)

func NewRdsQueue(client *rds.Client, opts ...Option) *RdsQueue {
	rq := &RdsQueue{
		MQueue: NewMQueue(opts...),
		client: client,
	}

	rq.AddTopic(rdsInsertBatchTopic, rq.insertBatchHandle, stdHandleErr)
	go rq.MQueue.Consume(rdsInsertBatchTopic)
	return rq
}

func (r *RdsQueue) Produce(topicName string, ee ...Entry) error {
	var entries []Entry
	for _, e := range ee {
		entries = append(entries, Entry{Key: topicName, Data: e.Data})
	}
	return r.MQueue.Produce(rdsInsertBatchTopic, entries...)
}

func (r *RdsQueue) insertBatchHandle(topicName string, batch batchEntry) error {
	topicKey := batch.entries[0].Key
	var dts []interface{} = make([]interface{}, 0)
	for _, e := range batch.entries {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return err
		}
		dts = append(dts, data)
	}
	return r.client.LPush(context.TODO(), topicKey, dts...).Err()
}

func (r *RdsQueue) fetchBatchHandle(topicName string) error {
	rply, err := r.client.RPop(context.TODO(), topicName).Result()
	if err != nil {
		return err
	}

	e := Entry{Key: topicName, Data: rply}
	topicEntry := r.topics[topicName]
	batch := topicEntry.insertEntry([]Entry{e}, r.cfg.batchSize, unsafe.Pointer(&r.mux))

	if len(batch.entries) >= r.cfg.batchSize {
		// 阻塞，直到消费完batchSize个消息
		<-batch.done
	}

	return nil
}

func (r *RdsQueue) Consume(topicName string) {
	_, ok := r.topics[topicName]
	if !ok {
		return
	}

	go r.MQueue.Consume(topicName)

	for attempt := 0; true; attempt++ {
		// 没有消息时最多休眠3s
		sleep(attempt, r.cfg.delayMin, r.cfg.delayMax)
		if err := r.fetchBatchHandle(topicName); err == nil {
			attempt = 0
		}
	}
}

func sleep(attempt int, minTime, maxTime time.Duration) {
	var duration time.Duration = time.Duration(attempt*attempt) * minTime
	if duration == 0 {
		return
	}
	if duration > maxTime {
		duration = maxTime
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()

	<-timer.C
}
