package model

import "database/sql"

type OutboxStatus int

const (
	OutboxPending   OutboxStatus = 1
	OutboxCompleted OutboxStatus = 2
)

type Outbox struct {
	ID        int64        `db:"id"`
	Content   []byte       `db:"content"`
	Status    OutboxStatus `db:"status"`
	CreatedAt sql.NullTime `db:"created_at"`
	UpdatedAt sql.NullTime `db:"updated_at"`
}
