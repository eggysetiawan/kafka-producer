package config

import (
	"github.com/Shopify/sarama"
	"log"
)

func NewKafkaConnect(topicName string) (sarama.SyncProducer, *string, error) {

	brokerList := kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic := kingpin.Flag("topic", "Topic name").Default(topicName).String()
	maxRetry := kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()

	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = *maxRetry
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(*brokerList, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	return producer, topic, nil
}
