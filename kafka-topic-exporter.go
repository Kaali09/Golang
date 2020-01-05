package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	zookeeperConn = "11.2.1.15:2181"
	cgroup        = "metrics.read"
	topic1        = "sunbirddev.analytics_metrics"
	topic2        = "sunbirddev.pipeline_metrics"
)

type metrics struct {
	job_name  string
	partition int
	metrics   map[string]interface{}
}

var prometheusMetrics []metrics
var msg []string

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	// prometheus.MustRegister(gauge)
	http.HandleFunc("/metrics", serve)
	// init consumer
	cg, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	defer cg.Close()
	// run consumer
	go consume(cg)
	log.Fatal(http.ListenAndServe(":8000", nil))
}
func serve(w http.ResponseWriter, r *http.Request) {
	for _, value := range prometheusMetrics {
		for k, j := range value.metrics {
			// tmp, _ := k.(string)
			fmt.Fprintf(w, "samza_metrics_%v{job_name=\"%v\",partition=\"%v\"} %v\n", strings.ReplaceAll(k, "-", "_"), value.job_name, value.partition, j)
		}
	}
}
func initConsumer() (*consumergroup.ConsumerGroup, error) {
	// consumer config
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	// join to consumer group
	cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{topic1, topic2}, []string{zookeeperConn}, config)
	if err != nil {
		return nil, err
	}
	return cg, err
}
func convertor(jsons []byte) {
	var m map[string]interface{}
	err := json.Unmarshal(jsons, &m)
	if err != nil {
		panic(err)
	}
	job_name, _ := m["job-name"].(string)
	partition, _ := m["partition"].(int)
	delete(m, "metricts")
	delete(m, "job-name")
	delete(m, "partition")
	prometheusMetrics = append(prometheusMetrics, metrics{job_name, partition, m})
}

/*
metrics reference
samza_metrics_asset_enrichment {"partition": 1, "consumer-lag" : 2, "failed_message_count": 2}
*/
func consume(cg *consumergroup.ConsumerGroup) {
	for {
		select {
		case message := <-cg.Messages():
			convertor(message.Value)
			// msg = append(msg, string(message.Value))
			err := cg.CommitUpto(message)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
