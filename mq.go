package queue

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/sirupsen/logrus"
)

// 内存模式 主题消息队列
var _ Queue = (*MQueue)(nil)

type MQueue struct {
	topics
	wg  sync.WaitGroup
	mux sync.Mutex

	cfg Config
}

func NewMQueue(opts ...Option) *MQueue {
	mq := &MQueue{
		topics: make(topics),
		cfg:    NewConfig(opts...),
	}

	return mq
}

func (m *MQueue) AddTopic(topicName string, handle ConsumeHandle, handleErr ConsumeErrHandle) {
	if te := m.topics.addTopic(topicName, handle, handleErr); te != nil {
		te.bchan = make(chan batchEntry, 1)
	}
}

func (m *MQueue) Produce(topicName string, ee ...Entry) error {
	if _, ok := m.topics[topicName]; !ok {
		return fmt.Errorf("Topic '%s' is not existed", topicName)
	}

	topicEntry := m.topics[topicName]
	topicEntry.insertEntry(ee, m.cfg.batchSize, unsafe.Pointer(&m.mux))
	return nil
}

func (m *MQueue) Consume(topicName string) {
	te, ok := m.topics[topicName]
	if !ok {
		return
	}

	for b := range te.bchan {
		m.wg.Add(1)
		if err := te.topic.handle(topicName, b); err != nil && te.topic.handleErr != nil {
			te.topic.handleErr(topicName, b, err)
		}
		m.wg.Done()
	}
}

func (m *MQueue) Wait() {
	m.wg.Wait()
}

func stdHandleErr(topicName string, batch batchEntry, err error) {
	logrus.Errorf("[%s] %d %v", topicName, batch._id, err)
}
