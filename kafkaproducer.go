package main
import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	kafka "github.com/segmentio/kafka-go"
)
func helloError(res http.ResponseWriter, r *http.Request) {
	res.WriteHeader(500)
	res.Write([]byte("Boom!"))
}
func kafkaObject() *kafka.Conn {
	// Getting kafka host
	kafka_host := "50.1.0.5:9092" // os.Getenv("kafka_host")
	// Getting kafka topic
	kafka_topic := "devcon.iot.metrics" //os.Getenv("kafka_topic")
	// Checking for mandatory variables
	if kafka_topic == "" || kafka_host == "" {
		log.Panic("kafka_topic or kafka_host environment variables not set")
	}
	// Creating Kafka connection
	conn, _ := kafka.DialLeader(context.Background(), "tcp", kafka_host, kafka_topic, 0)
	return conn
}
func helloWorld(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		conn := kafkaObject()
		// defer conn.Close()
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		// If got post, put it into kakfa
		go func(reqBody []byte, conn *kafka.Conn) {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := conn.WriteMessages(
				kafka.Message{Value: reqBody},
			)
			if err != nil {
				log.Fatal(err)
			}
			conn.Close()
		}(reqBody, conn)
		fmt.Printf("%s\n", reqBody)
		fmt.Fprintf(w, "Body: %s\n", reqBody)
	default:
		for k, v := range r.Header {
			fmt.Fprintf(w, "%s: %s\n", k, v)
		}
	}
	fmt.Fprintf(w, "HTTPVersion: %s\n", r.Proto)
	fmt.Fprintf(w, "RequestPath: %s\n", r.URL.Path)
	fmt.Fprintf(w, "Version: v3")
}
func main() {
	http.HandleFunc("/", helloWorld)
	http.HandleFunc("/error", helloError)
	if err := http.ListenAndServe(":4000", nil); err != nil {
		log.Fatal(err)
	}
}
