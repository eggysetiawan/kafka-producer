package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
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
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("EJLOG").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

func main() {
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

	//b := NewBody("test")
	//
	//wm, err := json.Marshal(b)
	//if err != nil {
	//	return
	//}

	content := map[string]string{}

	//content["key"] = "A"
	//content["value"] = "DATA EJLOG A"

	content["key"] = "B"
	content["value"] = "DATA EJLOG B"

	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder(content["key"]),
		Value: sarama.StringEncoder(content["value"]),
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		log.Panic(err)
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)

}

func unixTime() string {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	n, _ := fmt.Print(r1.Intn(100), ",")

	return strconv.Itoa(n)

}
