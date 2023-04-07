package kafka

import (
	"github.com/Shopify/sarama"
)

type IConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
}

func NewConsumer(host string, topic string) IConsumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer([]string{host}, config)
	if err != nil {
		panic(err)
	}

	partitionConn, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	return partitionConn
}
