package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"webserver/clickhouse"
	"webserver/model"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v2"
)

func main() {
	// Connect to NATS

	nc, err := nats.Connect(nats.DefaultURL)
	checkErr(err)
	log.Println(nc)
	js, err := nc.JetStream()
	log.Print(js)
	checkErr(err)

	stream, err := js.StreamInfo("GITHUB")
	checkErr(err)
	log.Println(stream)

	//Get clickhouse connection
	connection, err := clickhouse.GetClickHouseConnection()
	if err != nil {
		log.Fatal(err)
	}

	//Create schema
	clickhouse.CreateSchema(connection)

	//Get db data
	// data, err := clickhouse.RetrieveEvent(connection)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("DB: %s", data)

	// Create durable consumer monitor
	js.Subscribe("GITHUB.event", func(msg *nats.Msg) {
		msg.Ack()
		var metrics model.GithubEvent
		err := json.Unmarshal(msg.Data, &metrics)
		if err != nil {
			log.Fatal(err)
		}
		y, err := yaml.Marshal(metrics.Repository.Owner.Name)
		if err != nil {
			fmt.Printf("err: %v\n", err)
		}
		//fmt.Printf("Add event: %s \n", y)
		//log.Printf("Metrics received - subject: %s, ID: %v, Type: %s, Event: %s\n", msg.Subject, metrics.Repository.ID, metrics.Repository.Owner.Name, y)
		// Insert event
		clickhouse.InsertEvent(connection, metrics)
		log.Println()
	}, nats.Durable("EVENTS_CONSUMER"), nats.ManualAck())

	runtime.Goexit()
}
func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
