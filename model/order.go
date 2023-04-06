package model

import "database/sql"

type OrderStatus int

const (
	OrderPending   OrderStatus = 1
	OrderCompleted OrderStatus = 2
	OrderFailed    OrderStatus = 3
)

type Order struct {
	ID         int64        `db:"id"`
	CustomerID int64        `db:"customer_id"`
	ProductID  int64        `db:"product_id"`
	Amount     int          `db:"amount"`
	Status     OrderStatus  `db:"status"`
	CreatedAt  sql.NullTime `db:"created_at"`
	UpdatedAt  sql.NullTime `db:"updated_at"`
}
