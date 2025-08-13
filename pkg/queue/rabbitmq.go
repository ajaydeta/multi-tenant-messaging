package queue

import (
	"log"

	"github.com/streadway/amqp"
)

func NewRabbitMQConn(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	log.Println("Successfully connected to RabbitMQ")
	return conn, nil
}
