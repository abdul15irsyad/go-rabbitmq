package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

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

	queue, err := channel.QueueDeclare(
		"test-queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	// run goroutine consumer
	go ConsumeMessage(channel, &queue)

	http.HandleFunc("/publish-message", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Unable to read request body", http.StatusInternalServerError)
			return
		}

		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		if err != nil {
			http.Error(w, "Invalid JSON format", http.StatusInternalServerError)
			return
		}

		message := data["message"].(string)

		PublishMessage(channel, &queue, &message)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": &message,
		})
	})
	http.ListenAndServe(":8888", nil)
}

func PublishMessage(channel *amqp.Channel, queue *amqp.Queue, message *string) {
	err := channel.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(*message),
		})
	if err != nil {
		panic(err)
	}
}

func ConsumeMessage(channel *amqp.Channel, queue *amqp.Queue) {
	messages, err := channel.Consume(
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
		for message := range messages {
			fmt.Println(string(message.Body))
			message.Ack(false)
		}
	}()

	<-forever
}
