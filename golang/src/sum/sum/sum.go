package sum

import (
	"fmt"
	"log/slog"
	"sync"
	"hash/fnv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

//TODO: revisar si conviene guardar la config en el struct
type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange *middleware.ExchangeMiddleware
	fruitItemMaps  map[string]map[string]fruititem.FruitItem

	controlInQueue middleware.Middleware
	controlOutQueue middleware.Middleware
	sendingEOF map[string]bool
	mutex sync.Mutex

	aggregationAmount int
	aggregationPrefix string
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	controlInQueueName := fmt.Sprintf("%s_%d_control", config.SumPrefix, config.Id)
	controlInQueue, err := middleware.CreateQueueMiddleware(controlInQueueName, connSettings)
	if err != nil {
		inputQueue.Close() 
		return nil, err
	}

	successorId := (config.Id +1) % config.SumAmount
	controlOutQueueName := fmt.Sprintf("%s_%d_control", config.SumPrefix, successorId)
	controlOutQueue, err := middleware.CreateQueueMiddleware(controlOutQueueName, connSettings)
	if err != nil {
		inputQueue.Close() 
		controlInQueue.Close()
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.NewExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close() 
		controlInQueue.Close()
		controlOutQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMaps:  map[string]map[string]fruititem.FruitItem{},
		controlInQueue: controlInQueue,
		controlOutQueue: controlOutQueue, 
		sendingEOF:   map[string]bool{},
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})

	sum.controlInQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		sum.mutex.Lock()
		sum.sendingEOF[clientID] = true
		sum.mutex.Unlock()
		
		if err := sendEOFMessage(clientID, sum.controlOutQueue); err != nil {
			slog.Error("While sending EOF message", "err", err)
			return
		}
		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID string) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	slog.Info("Received End Of Records message")

	// TODO: revisar si se puede mandar un batch de frutas en lugar de una por una
	fruits := sum.fruitItemMaps[clientID]
	for fruit := range fruits {
		fruitRecord := []fruititem.FruitItem{fruits[fruit]}
		message, err := inner.SerializeMessage(clientID, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		key := sum.routeKeyForFruit(fruit, clientID) 
		if err := sum.outputExchange.SendToKey(*message, key); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	if err := sendEOFMessage(clientID, sum.outputExchange); err != nil {
		slog.Error("While sending EOF message", "err", err)
		return err
	}
	delete(sum.fruitItemMaps, clientID)
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if _, ok := sum.fruitItemMaps[clientID]; !ok {
		sum.fruitItemMaps[clientID] = map[string]fruititem.FruitItem{}
	}
	fruits := sum.fruitItemMaps[clientID]
	for _, fruitRecord := range fruitRecords {
		_, ok := fruits[fruitRecord.Fruit]
		if ok {
			fruits[fruitRecord.Fruit] = fruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruits[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, _, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		sum.mutex.Lock()
		if sum.sendingEOF[clientID] {
			delete(sum.sendingEOF,clientID)
			sum.mutex.Unlock()	
			return
		}
		sum.mutex.Unlock()	

		if err := sendEOFMessage(clientID, sum.controlOutQueue); err != nil {
			slog.Error("While sending EOF message", "err", err)
			return
		}

		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
}

func sendEOFMessage(clientID string, midd middleware.Middleware) error {
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientID, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := midd.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err  
	}
	return nil
}

func (sum *Sum) routeKeyForFruit(fruit string) string {
	// TODO: revisar si es necesario agragar clientID
	h := fnv.New32a()
	h.Write([]byte(fruit))
	idx := int(h.Sum32()) % sum.aggregationAmount
	return fmt.Sprintf("%s_%d", sum.aggregationPrefix, idx)
}