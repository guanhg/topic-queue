package queue

import (
	"sync"
	"time"
	"unsafe"
)

type (
	// 主题集合，每个队列中包含一个集合
	topics map[string]*topicEntry

	// 主题对象
	// topic 主题的回调：自定义消费函数、自定义错误处理函数
	// echan 队列消息channel
	// currBatch 消息缓存，等待批量处理
	topicEntry struct {
		topic     Topic
		bchan     chan batchEntry
		currBatch *batchEntry
	}

	// 批量缓存对象
	batchEntry struct {
		_id     uint64
		done    chan struct{}
		ready   chan struct{}
		state   batchState
		entries []Entry
	}

	// 缓存状态
	batchState int
)

const (
	CurrState batchState = 1 << 1
	WaitState batchState = 1 << 2
	DoneState batchState = 1 << 3
)

func (t topics) addTopic(topicName string, handle ConsumeHandle, handleErr ConsumeErrHandle) *topicEntry {
	if _, ok := t[topicName]; ok || handle == nil {
		return nil
	}
	t[topicName] = &topicEntry{
		topic: Topic{name: topicName, handle: handle, handleErr: handleErr},
	}
	return t[topicName]
}

func (te *topicEntry) newBatchEntry() *batchEntry {
	te.currBatch = &batchEntry{
		_id:   uint64(time.Now().Nanosecond()),
		ready: make(chan struct{}),
		done:  make(chan struct{}),
		state: CurrState,
	}
	return te.currBatch
}

/*
主要思路：把一个topic下的所有消息切割为多个batchEntry(容量为batchSize)
当开始插入消息时，创建一个batchEntry对象，启动异步协程监控该对象是否已满或待满超时(1s)
当batchEntry已满时，该对象已完全放在后台自动发送消息，无法再获取该对象
当batchEntry发送消息结束后，等待系统自动垃圾回收
*/

func (te *topicEntry) insertEntry(e []Entry, batchSize int, lock unsafe.Pointer) *batchEntry {
	lo := (*sync.Mutex)(lock)
	lo.Lock()
	defer lo.Unlock()

	var batch *batchEntry = te.currBatch
	if batch == nil {
		batch = te.newBatchEntry()
		go batch.await(te.bchan, lock)
	}

	batch.entries = append(batch.entries, e...)
	if len(batch.entries) >= batchSize {
		te.currBatch = nil
		// ready 满了
		batch.Ready()
	}

	return batch
}

func (b *batchEntry) await(bchan chan batchEntry, lock unsafe.Pointer) {
	lo := (*sync.Mutex)(lock)

	select {
	case <-b.ready: //  满的batch
		bchan <- *b
		b.Done()
	case <-time.After(time.Second): // 当前batch，没有满
		var entries []Entry
		lo.Lock()
		if b != nil && (b.state&CurrState == CurrState) {
			entries = make([]Entry, len(b.entries))
			copy(entries, b.entries)
			b = nil
		}
		lo.Unlock()

		bchan <- batchEntry{entries: entries, state: DoneState}
	}
}

func (b *batchEntry) Ready() {
	b.state |= WaitState
	close(b.ready)
}

func (b *batchEntry) Done() {
	b.state |= DoneState
	close(b.done)
}
