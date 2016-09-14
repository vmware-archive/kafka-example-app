package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/cloudfoundry-community/go-cfenv"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"gopkg.in/Shopify/sarama.v1"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatalln("must set PORT")
	}

	app, err := cfenv.Current()
	if err != nil {
		log.Fatalf("error getting app env: %s", err)
	}

	kafkaTaggedServices, err := app.Services.WithTag("kafka")
	if err != nil {
		log.Fatalf("error getting app env: %s", err)
	}
	if len(kafkaTaggedServices) != 1 {
		log.Fatalf("err finding kafka service: %v", kafkaTaggedServices)
	}
	kafkaServersInterfaces := kafkaTaggedServices[0].Credentials["bootstrap_servers"].([]interface{})
	kafkaServers := []string{}
	for _, server := range kafkaServersInterfaces {
		kafkaServers = append(kafkaServers, server.(string))
	}

	router := mux.NewRouter()

	router.HandleFunc("/queues/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
		errs := func(msg string, err error) {
			w.WriteHeader(500)
			fmt.Fprintf(w, "error %s: %s", msg, err)
		}
		queueName := mux.Vars(r)["queue_name"]
		producer, err := sarama.NewSyncProducer(kafkaServers, sarama.NewConfig())
		if err != nil {
			errs("can't create a producer", err)
			return
		}

		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			errs("can't read message", err)
			return
		}
		defer r.Body.Close()

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: queueName,
			Value: sarama.ByteEncoder(value),
		})
		if err != nil {
			w.WriteHeader(500)
			errs("can't push message", err)
			return
		}
	}).Methods("POST")

	router.HandleFunc("/queues/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
		errs := func(msg string, err error) {
			w.WriteHeader(500)
			fmt.Fprintf(w, "error %s: %s", msg, err)
		}

		queueName := mux.Vars(r)["queue_name"]

		broker, err := findLeader(kafkaServers, queueName)
		if err != nil {
			errs("cant find leader", err)
			return
		}

		err = broker.Open(sarama.NewConfig())
		if err != nil {
			errs("cant connect to topic leader broker", err)
			return
		}
		defer broker.Close()

		offset, err := fetchOffsetFromOffsetAPI(broker, queueName)
		if err != nil {
			errs("cant fetch offset", err)
			return
		}

		if offset <= 0 {
			offset, err = fetchOldestOffsetForTopic(kafkaServers, queueName)
			if err != nil {
				errs("cant fetch oldest topic offset", err)
				return
			}
		}

		log.Printf("fetching messages for offset %d", offset)
		messageSet, err := fetchMessageSetForOffset(broker, queueName, offset)

		if len(messageSet.Messages) == 0 {
			w.WriteHeader(404)
			return
		}

		message := messageSet.Messages[0]

		err = saveOffset(broker, queueName, message.Offset+1)
		if err != nil {
			errs("cant save offsets", err)
			return
		}

		w.Write(messageSet.Messages[0].Msg.Value)

	}).Methods("GET")

	server := negroni.Classic()
	server.UseHandler(router)
	server.Run(fmt.Sprintf(":%s", port))
}

func saveOffset(broker *sarama.Broker, queueName string, offset int64) error {
	offsetCommitRequest := &sarama.OffsetCommitRequest{}
	offsetCommitRequest.ConsumerGroup = "test-client-consumer-group"
	offsetCommitRequest.AddBlock(queueName, 0, offset, -1, "")
	_, err := broker.CommitOffset(offsetCommitRequest)
	return err
}

func fetchMessageSetForOffset(broker *sarama.Broker, queueName string, offset int64) (sarama.MessageSet, error) {
	fetchRequest := &sarama.FetchRequest{}
	fetchRequest.AddBlock(queueName, 0, offset, 10000)
	fetchResponse, err := broker.Fetch(fetchRequest)
	if err != nil {
		return sarama.MessageSet{}, err
	}
	fetchBlock := fetchResponse.GetBlock(queueName, 0)
	return fetchBlock.MsgSet, nil
}

func fetchOldestOffsetForTopic(kafkaServers []string, queueName string) (int64, error) {
	client, err := sarama.NewClient(kafkaServers, sarama.NewConfig())
	if err != nil {
		return 0, err
	}
	return client.GetOffset(queueName, 0, sarama.OffsetOldest)
}

func fetchOffsetFromOffsetAPI(broker *sarama.Broker, queueName string) (int64, error) {
	fetchOffsetRequest := &sarama.OffsetFetchRequest{ConsumerGroup: "test-client-consumer-group"}
	fetchOffsetRequest.AddPartition(queueName, 0)
	fetchOffsetResponse, err := broker.FetchOffset(fetchOffsetRequest)
	if err != nil {
		return 0, err
	}
	block := fetchOffsetResponse.GetBlock(queueName, 0)
	offset := block.Offset
	return offset, nil
}

func findLeader(kafkaServers []string, queueName string) (*sarama.Broker, error) {
	metadata, err := findMetadata(kafkaServers, queueName)
	if err != nil {
		return nil, err
	}
	return metadata.Brokers[metadata.Topics[0].Partitions[0].Leader], nil
}

func findMetadata(kafkaServers []string, queueName string) (*sarama.MetadataResponse, error) {
	broker := sarama.NewBroker(kafkaServers[0])
	err := broker.Open(sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	defer broker.Close()

	metadata, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{queueName}})
	if err != nil {
		return nil, err
	}
	return metadata, nil
}
