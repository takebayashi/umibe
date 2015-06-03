package main

import (
	"flag"
	"github.com/Shopify/sarama"
	"log"
	"strings"
	"time"
)

var (
	brokers = flag.String("brokers", "", "Kafka broker list, comma-separated")
	topic   = flag.String("topic", "", "topic name")
	size    = flag.Int("size", 10, "message size")
)

func main() {
	flag.Parse()
	c := sarama.NewConfig()
	p, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), c)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	var count int64 = 0
	ti := time.NewTicker(time.Second)
	go func() {
		for range ti.C {
			log.Println(count)
			count = 0
		}
	}()
	for {
		_, _, err = p.SendMessage(getMessage())
		count++
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func getMessage() *sarama.ProducerMessage {
	s := ""
	for i := 0; i < *size; i++ {
		s += "a"
	}
	return &sarama.ProducerMessage{
		Topic: *topic,
		Value: sarama.StringEncoder(s),
	}
}
