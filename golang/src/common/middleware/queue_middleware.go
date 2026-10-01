package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"math/rand"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	consumerTag string
	consuming   bool
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	url_path := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url_path)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		false,     // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	return &QueueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
		consuming: false,
	}, nil
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.consuming {
		return ErrMessageMiddlewareMessage
	}

	err := q.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	q.consumerTag = fmt.Sprintf("%s-%d", q.queueName, rand.Uint64())

	msgs, err := q.ch.Consume(
		q.queueName,   // queue
		q.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		if q.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	q.consuming = true
	for d := range msgs {
		callbackFunc(
			Message{Body: string(d.Body)},
			func() { d.Ack(false) },
			func() { d.Nack(false, false) },
		)

	}
	q.consuming = false

	if q.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (q *QueueMiddleware) StopConsuming() error {
	if q.consuming {
		err := q.ch.Cancel(q.consumerTag, false)
		q.consuming = false
		if err != nil {
			if q.conn.IsClosed() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (q *QueueMiddleware) Send(msg Message) error {
	if q.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	err := q.ch.Publish(
		"",          // exchange
		q.queueName, // routing key
		false,       // mandatory
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if q.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *QueueMiddleware) Close() error {
	if q.ch != nil {
		err := q.ch.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	if q.conn != nil {
		err := q.conn.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	q.ch = nil
	q.conn = nil
	return nil
}
