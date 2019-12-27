package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	// "github.com/prometheus/client_golang/prometheus" "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	zookeeperConn = "11.2.1.15:2181"
	cgroup        = "Test1"
	topic1        = "sunbirddev.analytics_metrics"
	topic2        = "sunbirddev.pipeline_metrics"
)

var msg string

// var (
//     gauge = prometheus.NewGauge(
//         prometheus.GaugeOpts{
//             Namespace: "golang",
//             Name:      "my_gauge",
//             Help:      "This is my gauge",
//         })
// )

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	// prometheus.MustRegister(gauge)
	http.HandleFunc("/", helloWorld)

	// init consumer
	cg, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("####### Printing cg #########")
	fmt.Println(cg)
	defer cg.Close()
	// run consumer
	fmt.Println("##### Printing consume #######")
	fmt.Println(consume)
	fmt.Println("#### end of consume ####")
	go consume(cg)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "\"HTTPVersion\": %q\n", msg)
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
		case message := <-cg.Messages():
			msg = string(message.Value)
			//	msg := <-fmt.Sprintf("Value: ", (message.Value))
			fmt.Println("got message")
			err := cg.CommitUpto(message)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
