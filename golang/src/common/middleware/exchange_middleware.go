package middleware

import (
	"fmt"
	"math/rand"
	amqp "github.com/rabbitmq/amqp091-go" 
)

type ExchangeMiddleware struct {
	conn 		*amqp.Connection
	ch			*amqp.Channel
	exchange	string
	keys		[]string	 
	consuming 	bool
	consumerTag	string
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (*ExchangeMiddleware, error) {
	url_path := fmt.Sprintf("amqp://guest:guest@%s:%d/",  connectionSettings.Hostname, connectionSettings.Port)
 	conn, err := amqp.Dial(url_path)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	err = ch.ExchangeDeclare(
		exchange,   // name
		"direct", 	// type   
		false,     	// durability
		false,    	// auto-deleted
		false,    	// internal
		false,    	// no-wait
		nil,      	// arguments
	)
    if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	return &ExchangeMiddleware {
		conn: conn,
		ch: ch,
		exchange: exchange,
		keys: keys,
		consuming: false,
	}, nil
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.consuming{
		return ErrMessageMiddlewareMessage
	}

	q, err := e.ch.QueueDeclare(
		"",    // name 
		false, // durability
		true,  // delete when unused  
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
    if err != nil {
		if e.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	for _, key := range e.keys {
		err := e.ch.QueueBind(
			q.Name, 	// queue name
			key,     	// routing key
			e.exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			if e.conn.IsClosed() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	
	e.consumerTag = fmt.Sprintf("%s-%d", e.exchange, rand.Uint64())
	msgs, err := e.ch.Consume(
			q.Name, 			// queue
			e.consumerTag,     	// consumer
			false,   			// auto-ack
			false,  			// exclusive
			false,  			// no-local
			false,  			// no-wait
			nil,    			// args
	)
	if err != nil {
		if e.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	e.consuming = true
	for d := range msgs {
		callbackFunc(
			Message{Body: string(d.Body)},
			func() { d.Ack(false) },
			func() { d.Nack(false, false)},
		)
			
	}
	e.consuming = false

	if e.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.consuming {
		err := e.ch.Cancel(e.consumerTag, false)
		e.consuming = false
		if err != nil {
			if e.conn.IsClosed() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	for _, key := range e.keys {
		if err := e.SendToKey(msg, key); err != nil{
			return err
		}
	}	
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if e.ch != nil {
		err := e.ch.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	if e.conn != nil {
		err := e.conn.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	e.ch = nil
	e.conn = nil
	return nil
}

func (e *ExchangeMiddleware) SendToKey(msg Message, key string) error {
	if e.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	err := e.ch.Publish(
		e.exchange, // exchange
		key,     	// routing key
		false, 		// mandatory
		false,  	// immediate
		amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
		})
	if err != nil {
		if e.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}	
	return nil
}