package queue

import "time"

const (
	defaultBatchSize      = 10
	defaultDelayMin       = 10 * time.Millisecond
	defaultDelayMax       = 3 * time.Second
	defaultCommitInterval = time.Second
	defaultMaxWait        = time.Second
	defaultQueueCapacity  = 1000
)

type Config struct {
	batchSize int
	delayMin  time.Duration // 消费等待最小延时读
	delayMax  time.Duration // 消费等待最大延时读
}

func NewConfig(opts ...Option) Config {
	ops := Config{
		batchSize: defaultBatchSize,
		delayMin:  defaultDelayMin,
		delayMax:  defaultDelayMax,
	}
	for _, opt := range opts {
		opt(&ops)
	}

	return ops
}

type Option func(q *Config)

func WithBatchSize(size int) Option {
	return func(opt *Config) {
		opt.batchSize = size
	}
}

func WithDelayMin(d time.Duration) Option {
	return func(q *Config) {
		q.delayMin = d
	}
}

func WithDelayMax(d time.Duration) Option {
	return func(q *Config) {
		q.delayMax = d
	}
}
