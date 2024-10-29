package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"math/rand"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer channel.Close()

	err = channel.Qos(4, 0, false)
	if err != nil {
		panic(err)
	}

	queue, err := channel.QueueDeclare(
		"go-queue",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": "go-queue-dlq",
		},
	)
	if err != nil {
		panic(err)
	}

	_, err = channel.QueueDeclare(
		"go-queue-dlq",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": "go-queue",
			"x-message-ttl":             int32(1000),
		},
	)
	if err != nil {
		panic(err)
	}

	// run goroutine consumer
	go ConsumeMessage(channel, &queue)
	go ConsumeMessage(channel, &queue)

	http.HandleFunc("/publish-message", func(w http.ResponseWriter, r *http.Request) {
		Publish(channel, &queue)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "success publish message",
		})
	})
	http.ListenAndServe(":8888", nil)
}

type Player struct {
	Name string
	Age  int
}

func Publish(channel *amqp.Channel, queue *amqp.Queue) {
	players := []Player{
		{Name: "Lamine Yamal", Age: 17},
		{Name: "Kylian Mbappe", Age: 25},
		{Name: "Lionel Messi", Age: 37},
		{Name: "Cristiano Ronaldo", Age: 39},
	}
	for _, player := range players {
		body, err := json.Marshal(player)
		if err != nil {
			fmt.Println(err)
			continue
		}
		err = channel.Publish(
			"",
			queue.Name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			})
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}

func ConsumeMessage(channel *amqp.Channel, queue *amqp.Queue) {
	payloads, err := channel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan struct{})

	go func() {
		for payload := range payloads {
			var player Player
			err := json.Unmarshal(payload.Body, &player)
			if err != nil {
				fmt.Println(err)
				payload.Ack(false)
				continue
			}
			rand.New(rand.NewSource(time.Now().UnixNano()))
			randomInt := rand.Intn(100)
			time.Sleep(1 * time.Second)
			currentTime := time.Now()
			if randomInt < 50 {
				fmt.Printf("%s: error %s\n", currentTime.Format("2006-01-02 15:04:05"), player.Name)
				payload.Nack(false, false)
				continue
			}
			fmt.Printf("%s: hai my name is %s, im %d years old\n", currentTime.Format("2006-01-02 15:04:05"), player.Name, player.Age)
			payload.Ack(false)
		}
	}()

	<-forever
}
