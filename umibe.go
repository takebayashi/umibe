package main

import (
	"bufio"
	"flag"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"time"
)

var (
	brokers  = flag.String("brokers", "", "Kafka broker list, comma-separated")
	topic    = flag.String("topic", "", "topic name")
	input    = flag.String("input", "", "file name to input")
	key      = flag.String("key", "", "message key")
	sendsRaw = flag.Bool("raw", false, "sending without extra encoding")
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
		var last int64 = 0
		for range ti.C {
			log.Printf("qps:%d\ttotal:%d\n", count-last, count)
			last = count
		}
	}()
	for {
		count += sendAll(p)
	}
}

func sendAll(p sarama.SyncProducer) int64 {
	var c int64 = 0
	f, err := os.Open(*input)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ke, ve sarama.Encoder
		if *key != "" {
			ke = sarama.StringEncoder(*key)
		}
		if *sendsRaw {
			ve = sarama.ByteEncoder(sc.Bytes())
		} else {
			ve = sarama.StringEncoder(sc.Text())
		}
		m := &sarama.ProducerMessage{
			Topic: *topic,
			Key:   ke,
			Value: ve,
		}
		_, _, err = p.SendMessage(m)
		c++
		if err != nil {
			log.Fatalln(err)
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalln(err)
	}
	return c
}
