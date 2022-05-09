package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"webserver/model"

	"github.com/nats-io/nats.go"
)

const (
	streamName     = "GITHUB"
	streamSubjects = "GITHUB.*"
	eventSubject   = "GITHUB.event"
	allSubject     = "GITHUB.all"
)

var webhookData = model.GithubEvent{}

func handleWebhook(w http.ResponseWriter, r *http.Request) {

	err := json.NewDecoder(r.Body).Decode(&webhookData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("repository id: %v \n", webhookData.Repository.ID)
	fmt.Printf("repository Name: %v \n", webhookData.Repository.Name)
	fmt.Printf("Pusher Name: %v \n", webhookData.Pusher.Name)
}

func main() {
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	checkErr(err)
	// Creates JetStreamContext
	js, err := nc.JetStream()
	checkErr(err)
	// Creates stream
	err = createStream(js)
	checkErr(err)
	ok, err := publishGithubMetrics(webhookData, js)
	if ok {
		checkErr(err)
	}

	log.Println("server started")
	http.HandleFunc("/webhooks", handleWebhook)
	log.Fatal(http.ListenAndServe(":8000", nil))
}
func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func createStream(js nats.JetStreamContext) error {
	// Check if the METRICS stream already exists; if not, create it.
	stream, err := js.StreamInfo(streamName)
	log.Printf("Retrieved stream %s", fmt.Sprintf("%v", stream))
	if err != nil {
		log.Printf("Error getting stream %s", err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q", streamName, streamSubjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		checkErr(err)
	}
	return nil
}
func publishGithubMetrics(data model.GithubEvent, js nats.JetStreamContext) (bool, error) {
	githubMetricsJson, _ := json.Marshal(data)
	_, err := js.Publish(eventSubject, githubMetricsJson)
	if err != nil {
		return true, err
	}
	log.Printf("Github Repository with Name:%s has been published\n", data.Repository.Name)
	return false, nil
}
