package app

import (
	"fmt"
	"github.com/Shopify/sarama"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type Body struct {
	Message string      `json:"message"`
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
}

func NewBody(message string) Body {
	return Body{
		Message: message,
		Code:    http.StatusOK,
		Data:    []string{},
	}
}

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092", "localhost:9093", "localhost:9094").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("counter13").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

func Produce() {
	//producer, topic, err := config.NewKafkaConnect("EJLOG")
	//
	//brokerList := kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	//topic := kingpin.Flag("topic", "Topic name").Default("EJLOG").String()
	//maxRetry := kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()

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

	if err != nil {
		return
	}

	//b := NewBody("test")
	//
	//wm, err := json.Marshal(b)
	//if err != nil {
	//	return
	//}

	var mutex sync.Mutex

	var group = &sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		go produceData(producer, group, &mutex, i)
	}

	group.Wait()

	fmt.Println("completed")

	//for i := 0; i <= 20000; i++ {
	//
	//}
}

func produceData(producer sarama.SyncProducer, group *sync.WaitGroup, mutex *sync.Mutex, i int) {

	group.Add(1)

	defer group.Done()

	content := map[string]string{}

	//content["key"] = "A"
	//content["value"] = "DATA EJLOG A"

	content["key"] = strconv.Itoa(i)
	content["value"] = strconv.Itoa(1)

	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder(content["key"]),
		Value: sarama.StringEncoder(content["value"]),
	}
	mutex.Lock()
	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		log.Panic(err)
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)

	mutex.Unlock()

}
