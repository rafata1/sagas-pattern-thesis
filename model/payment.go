package model

import "database/sql"

type Account struct {
	CustomerID int64        `db:"customer_id"`
	Balance    int64        `db:"balance"`
	CreatedAt  sql.NullTime `db:"created_at"`
	UpdatedAt  sql.NullTime `db:"updated_at"`
}
