package model

import "database/sql"

type Inventory struct {
	ProductID int64        `db:"product_id"`
	Amount    int          `db:"amount"`
	CreatedAt sql.NullTime `db:"created_at"`
	UpdatedAt sql.NullTime `db:"updated_at"`
}
