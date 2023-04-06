package order

type CreatedOrderEvent struct {
	OrderID    int64 `json:"order_id"`
	CustomerID int64 `json:"customer_id"`
	ProductID  int64 `json:"product_id"`
	Amount     int   `json:"amount"`
}
