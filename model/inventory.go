package model

import (
	"database/sql"
)

type Inventory struct {
	ProductID int64        `db:"product_id"`
	UnitPrice int          `db:"unit_price"`
	Amount    int          `db:"amount"`
	CreatedAt sql.NullTime `db:"created_at"`
	UpdatedAt sql.NullTime `db:"updated_at"`
}

type ProcessedOrder struct {
	OrderID   int64        `db:"order_id"`
	CreatedAt sql.NullTime `db:"created_at"`
}
