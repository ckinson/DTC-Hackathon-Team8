package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	heartbeatStart   = 55
	temperatureStart = 36.7
)

type personStatus struct {
	Heartbeat   int
	Temperature float64
	Name        *string
	StatusTime  string
}

func main() {
	name := flag.String("name", "Carl", "Person Name")
	stream := flag.String("stream", "autismdxc", "Stream Name")

	flag.Parse()

	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				SharedConfigState: session.SharedConfigEnable,
			},
		),
	)

	stimulateData(kinesis.New(sess), name, stream)
}

func stimulateData(client *kinesis.Kinesis, name, stream *string) {
	rand.Seed(time.Now().UnixNano())

	temperature := temperatureStart
	heartbeat := heartbeatStart
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		temperature = nextTemperature(temperature)
		heartbeat = nextHeartbeat(heartbeat)
		status, _ := json.Marshal(
			&personStatus{
				Heartbeat:   heartbeat,
				Temperature: temperature,
				Name:        name,
				StatusTime:  time.Now().Format("2006-01-02 15:04:05.000"),
			},
		)
		putRecordInput := &kinesis.PutRecordInput{
			Data:         append([]byte(status), "\n"...),
			PartitionKey: name,
			StreamName:   stream,
		}

		if _, err := client.PutRecord(putRecordInput); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Print(".")
	}
}

func nextTemperature(points float64) float64 {
	x := 0.1

	if rand.Int()%2 == 0 {
		x = -0.1
	}

	return points + x
}

func nextHeartbeat(points int) int {
	x := 1

	if rand.Int()%2 == 0 {
		x = -1
	}

	return points + x
}
