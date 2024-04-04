
# topic-message queue

生产者(Producer)和消费者(Consumer)发送/消费`批量消息(batch message)`

- **[memory模式](./mq.go)**
    消息队列存储在内存中
    1. 当队列中消息数量满足batchSize(默认10)时，被发送/消费
    2. 当队列中的数据缓存超时时，被强制发送/消费

- **[redis模式](./rq.go)**
    消息被持久化到redis list中，每个主题一个list
    1. 生产者`消息被暂时缓存`，满足条件时批量发送，减少redis网络请求
    2. 消费者`缓存消息`，满足条件时批量消费
    每次最多获取batchSize个消息，`如果没有消费完，阻塞获取请求`

- **[kafka模式](./kq.go)**
    kafka接口的简单包装

> Example
```go
// new queue
// memory
q := NewMQueue(WithBatchSize(5))

// redis
// client := rds.NewClient(&rds.Options{Addr: "localhost:6379"})
// q := NewRdsQueue(client, WithBatchSize(5))

// kafka
// client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, nil)
// q := NewKfQueue(client, WithBatchSize(5))

topicName := "belike"
q.AddTopic(topicName, func(topicName string, batch batchEntry) error {
    // consume handle
    for _, e := range batch.entries {
        var i int
        if err := json.Unmarshal(e.Data.([]byte), &i); err != nil {
            t.Error(err)
        }
        fmt.Printf("%s: %d \n", e.Key, i)
    }
    fmt.Println("batchSize: ", len(batch.entries))
    return nil
}, stdHandleErr)

for i := 0; i < 46; i++ {
    q.Produce(topicName, Entry{Key: "kmsg", Data: time.Now().Nanosecond()})
}

timer := time.NewTimer(time.Second * 3)
<-timer.C

q.Wait()
```