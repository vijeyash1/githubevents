package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"webserver/model"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/google/uuid"
)

func GetClickHouseConnection() (*sql.DB, error) {
	connect, err := sql.Open("clickhouse", "tcp://localhost:9000?debug=true")
	//connect, err := sql.Open("clickhouse", "tcp://kubviz-client-clickhouse:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil, err
	}
	return connect, nil
}
func CreateSchema(connect *sql.DB) {
	_, err := connect.Exec(`
		CREATE TABLE IF NOT EXISTS githubevents (
			id           UUID,
			reponame     String,
			pushername   String,
		) engine=File(TabSeparated)
	`)

	if err != nil {
		log.Fatal(err)
	}
}
func InsertEvent(connect *sql.DB, metrics model.GithubEvent) {
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO events (id, reponame, pushername) VALUES (?, ?, ?)")
	)
	defer stmt.Close()

	if _, err := stmt.Exec(uuid.New(), metrics.Repository.Name, metrics.Pusher.Name); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
func RetrieveEvent(connect *sql.DB) ([]model.GithubEvent, error) {
	rows, err := connect.Query("SELECT id, reponame, pushername FROM githubevents")
	if err != nil {
		log.Printf("Error: %s", err)
		return nil, err
	}
	defer rows.Close()
	var events []model.GithubEvent
	for rows.Next() {
		var dbEvent model.GithubEvent
		if err := rows.Scan(&dbEvent.Repository.ID, &dbEvent.Repository.Name, &dbEvent.Pusher.Name); err != nil {
			log.Printf("Error: %s", err)
			return nil, err
		}
		eventJson, _ := json.Marshal(dbEvent)
		log.Printf("DB Event: %s", string(eventJson))
		events = append(events, dbEvent)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error: %s", err)
		return nil, err
	}
	return events, nil
}
