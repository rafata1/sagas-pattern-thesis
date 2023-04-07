package saga_event

import "github.com/rafata1/sagas-pattern-thesis/model"

type OrderEvent struct {
	OrderID    int64             `json:"order_id"`
	CustomerID int64             `json:"customer_id"`
	ProductID  int64             `json:"product_id"`
	Amount     int               `json:"amount"`
	Cost       int               `json:"cost"`
	Status     model.OrderStatus `json:"status"`
}
