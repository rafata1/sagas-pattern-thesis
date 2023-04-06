package kafka

import (
	"github.com/Shopify/sarama"
)

type IProducer interface {
	Push(messages [][]byte) error
}

type producer struct {
	host  string
	topic string
	conn  sarama.SyncProducer
}

func NewProducer(host string, topic string) IProducer {
	saramaConf := sarama.NewConfig()
	saramaConf.Producer.Return.Successes = true
	saramaConf.Producer.Return.Errors = true
	saramaConf.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewClient([]string{host}, saramaConf)
	if err != nil {
		panic(err)
	}

	conn, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	return &producer{
		conn:  conn,
		host:  host,
		topic: topic,
	}
}

func (p producer) Push(messages [][]byte) error {
	return p.conn.SendMessages(toKafkaMessages(messages, p.topic))
}

func toKafkaMessages(messages [][]byte, topic string) []*sarama.ProducerMessage {
	var res []*sarama.ProducerMessage
	for _, message := range messages {
		res = append(res, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(message),
		})
	}
	return res
}
