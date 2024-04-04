package queue

type (
	Entry struct {
		Key  string      `json:"key"`
		Data interface{} `json:"data"`
	}

	Queue interface {
		Produce(topicName string, e ...Entry) error
		Consume(topicName string)
	}

	Topic struct {
		name      string
		handle    ConsumeHandle
		handleErr ConsumeErrHandle
	}

	ConsumeHandle    func(topicName string, e batchEntry) error
	ConsumeErrHandle func(topicName string, e batchEntry, err error)
)
