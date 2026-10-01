package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMaps map[string]map[string]fruititem.FruitItem
	EOFReceived   map[string]int
	topSize       int
	sumAmount     int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMaps: map[string]map[string]fruititem.FruitItem{},
		EOFReceived:   map[string]int{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		})
	}()

	<-signals
	if err := aggregation.inputExchange.StopConsuming(); err != nil {
		slog.Error("While stopping input exchange", "err", err)
	}
	wg.Wait()
	aggregation.shutdown()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientID, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message")
	aggregation.EOFReceived[clientID]++

	if aggregation.EOFReceived[clientID] < aggregation.sumAmount {
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(clientID)
	message, err := inner.SerializeMessage(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientID, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.EOFReceived, clientID)
	delete(aggregation.fruitItemMaps, clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMaps[clientID]; !ok {
		aggregation.fruitItemMaps[clientID] = map[string]fruititem.FruitItem{}
	}
	fruits := aggregation.fruitItemMaps[clientID]
	for _, fruitRecord := range fruitRecords {
		if _, ok := fruits[fruitRecord.Fruit]; ok {
			fruits[fruitRecord.Fruit] = fruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruits[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMaps[clientID]))
	for _, item := range aggregation.fruitItemMaps[clientID] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (aggregation *Aggregation) shutdown() {
	if err := aggregation.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
