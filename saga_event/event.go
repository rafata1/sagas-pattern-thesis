package saga_event

import "github.com/rafata1/sagas-pattern-thesis/model"

type PrepareInventoryEvent struct {
	OrderID    int64             `json:"order_id"`
	CustomerID int64             `json:"customer_id"`
	Status     model.OrderStatus `json:"status"`
	Cost       int               `json:"cost"`
}

type CreatedOrderEvent struct {
	OrderID    int64 `json:"order_id"`
	CustomerID int64 `json:"customer_id"`
	ProductID  int64 `json:"product_id"`
	Amount     int   `json:"amount"`
}

type BillOrderEvent struct {
	OrderID int64
	Status  model.OrderStatus
}
