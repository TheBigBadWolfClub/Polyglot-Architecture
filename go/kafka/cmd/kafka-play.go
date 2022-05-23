package main

import (
	"encoding/json"
	"fmt"
	"github.com/TheBigBadWolfClub/polyglot-architecture/go/kafka/pkg"
	"time"
)

type Data struct {
	Id          int
	Message     string
	Source      string
	Destination string
	T           string
}

type Data1 struct {
	Message string
}

func main() {

	signal := make(chan bool, 2)

	topicName := "myTopic"
	go func() {

		for {
			data := []Data{
				{Id: 1, Message: "World", Source: "1", Destination: "A", T: time.Now().String()},
				{Id: 2, Message: "Earth", Source: "1", Destination: "B", T: time.Now().String()},
				{Id: 3, Message: "Planets", Source: "2", Destination: "C", T: time.Now().String()},
			}
			stringJson, _ := json.Marshal(data)
			pkg.Producer(string(stringJson), topicName)

			time.Sleep(10 * time.Second)
		}

		signal <- true
	}()

	go func() {
		pkg.Consumer([]string{topicName})
		signal <- true
	}()

	fmt.Println(<-signal)
	fmt.Println(<-signal)
}
