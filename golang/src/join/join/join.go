package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	topByClient map[string][]fruititem.FruitItem
	EOFReceived map[string]int
	aggregationAmount int
	topSize     int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue: inputQueue, 
		outputQueue: outputQueue, 
		topByClient: map[string][]fruititem.FruitItem{}, 
		EOFReceived: map[string]int{}, 
		aggregationAmount: config.AggregationAmount, 
		topSize: config.TopSize,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := join.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	join.handleDataMessage(clientID, fruitRecords)
}

func (join *Join) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	top, ok := join.topByClient[clientID]
	if !ok {
		join.topByClient[clientID] = fruitRecords
		return
	}
	
	top = append(top, fruitRecords...)
	sort.SliceStable(top, func(i, j int) bool {
		return top[j].Less(top[i])
	})
	finalTopSize := min(join.topSize, len(top))
	join.topByClient[clientID] = top[:finalTopSize]
}

func (join *Join) handleEndOfRecordsMessage(clientID string) error {
	join.EOFReceived[clientID]++ 
	if join.EOFReceived[clientID] < join.aggregationAmount {
		return nil
	}
	fruitTopRecords := join.topByClient[clientID]
	msg, err := inner.SerializeMessage(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*msg); err != nil {
		slog.Error("While sending top", "err", err)
	}
	delete(join.EOFReceived, clientID)
	delete(join.topByClient, clientID)
	return nil	
}
