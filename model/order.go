package model

import "database/sql"

type OrderStatus string

const (
	OrderStatusPending                 = "PENDING"
	OrderStatusPrepared                = "PREPARED"
	OrderStatusFailedOutOfStock        = "OUT_OF_STOCK"
	OrderStatusBilled                  = "BILLED"
	OrderStatusFailedExceedCreditLimit = "EXCEED_CREDIT_LIMIT"
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
