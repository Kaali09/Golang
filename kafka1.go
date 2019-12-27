package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"log"
	"os"
	"time"
)

const (
	zookeeperConn = "11.2.1.15:2181"
	cgroup        = "Test1"
	topic1        = "sunbirddev.analytics_metrics"
	topic2        = "sunbirddev.pipeline_metrics"
)

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	//	fmt.Println(sarama.Logger)

	// init consumer
	cg, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	//	fmt.Println(cg)
	defer cg.Close()

	// run consumer
	consume(cg)
}

func initConsumer() (*consumergroup.ConsumerGroup, error) {
	// consumer config
	config := consumergroup.NewConfig()
	/*
		printing config var
		fmt.Println("Printing config var")
		fmt.Println(config)
		fmt.Println("printing sarama offsetoldest")
		fmt.Println(sarama.OffsetOldest)
	*/
	config.Offsets.Initial = sarama.OffsetOldest
	/*
		fmt.Println("printing config.Offsers.initial")
		fmt.Println(config.Offsets.Initial)
	*/
	config.Offsets.ProcessingTimeout = 10 * time.Second

	// join to consumer group
	cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{topic1, topic2}, []string{zookeeperConn}, config)
	if err != nil {
		return nil, err
	}

	return cg, err
}

func consume(cg *consumergroup.ConsumerGroup) {
	for {
		select {
		case msg := <-cg.Messages():
			// messages coming through chanel
			// only take messages from subscribed topic
			//		if msg.Topic != topic1 {
			//			continue
			//		}
			/*
				fmt.Println("Printinf msg var")
				fmt.Println(msg)
			*/
			fmt.Println("Topic: ", msg.Topic)
			fmt.Println("Value: ", string(msg.Value))

			// commit to zookeeper that message is read
			// this prevent read message multiple times after restart
			err := cg.CommitUpto(msg)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
