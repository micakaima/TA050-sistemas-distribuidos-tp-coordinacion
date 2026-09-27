package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const ClientIDKey string = "clientID"

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(clientID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := []interface{}{[]interface{}{ClientIDKey, clientID}}

	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	clientID := ""
	fruitRecords := []fruititem.FruitItem{}
	for i, datum := range data {
		pair, ok := datum.([]interface{})
		if !ok || len(pair) != 2 {
			return clientID, nil, false, errors.New("Invalid array")
		}

		key, ok := pair[0].(string)
		if !ok {
			return clientID, nil, false, errors.New("Invalid Key")
		}
		if i == 0 {
			if key != ClientIDKey {
				return clientID, nil, false, errors.New("First element must be client ID")
			}
			clientID, ok = pair[1].(string)
			if !ok {
			 	return clientID, nil, false, errors.New("Invalid clientID")
			}

		} else {
			amount, ok := pair[1].(float64)
			if !ok {
				return clientID, nil, false, errors.New("Amount is not a number")
			}

			fruitRecord := fruititem.FruitItem{Fruit: key, Amount: uint32(amount)}
			fruitRecords = append(fruitRecords, fruitRecord)
		}	
	}

	return clientID, fruitRecords, len(fruitRecords) == 0, nil
}
