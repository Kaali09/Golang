package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	zookeeperConn = "11.2.1.15:2181"
	cgroup        = "Test1"
	topic1        = "sunbirddev.analytics_metrics"
	topic2        = "sunbirddev.pipeline_metrics"
)

var msg []string
var message map[string]interface{}

//var msg []string = nil

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	// prometheus.MustRegister(gauge)
	http.HandleFunc("/", serve)

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

func serve(w http.ResponseWriter, r *http.Request) {
	//	msg := <-fmt.Sprintf("Value: ", (message.Value))
	for _, value := range message {
		fmt.Fprintf(w, "Value: %q", string(value))
	}

	// fmt.Fprintf(w, "\"HTTPVersion\": %q\n", msg)
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

/*
Dictionary prom_data{
	metric_name{samza_metric_druid_events_validator:
	  {
}
*/

func convertor(jsons []byte) {
	var m map[string]interface{}
	err := json.Unmarshal(jsons, &m)
	if err != nil {
		panic(err)
	}
	for k, _ := range m {
		message = fmt.Printf("samza_job_%v{ job-name: %v, partition: %v} %v \n", k, m["job-name"], m["partition"], m[k])
	}
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
