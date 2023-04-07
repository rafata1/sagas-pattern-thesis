package payment

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/rafata1/sagas-pattern-thesis/model"
)

type IRepo interface {
	Transact(ctx context.Context, fn func(ctx context.Context) error) error
	LockAccountForUpdate(ctx context.Context, customerID int64) (model.Account, error)
	UpdateBalance(ctx context.Context, customerID int64, balance int) error
	CreateOutbox(ctx context.Context, outbox model.Outbox) error
	IsProcessed(ctx context.Context, orderID int64) (bool, error)
	MarkProcessedOrder(ctx context.Context, orderID int64) error
	CreateAccount(ctx context.Context, account model.Account) error
	GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error)
	MarkDoneOutboxes(ctx context.Context, ids []int64) error
}

type repo struct {
	db *sqlx.DB
}

func NewRepo(db *sqlx.DB) IRepo {
	return &repo{
		db: db,
	}
}

func (r repo) Transact(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			err = tx.Rollback()
			panic(r)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	err = fn(ctx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

var lockAccountForUpdateQuery = "SELECT * FROM accounts WHERE customer_id = ? FOR UPDATE"

func (r repo) LockAccountForUpdate(ctx context.Context, customerID int64) (model.Account, error) {
	var res model.Account
	err := r.db.GetContext(ctx, &res, lockAccountForUpdateQuery, customerID)
	return res, err
}

var updateBalanceQuery = "UPDATE accounts SET balance = ? WHERE customer_id = ?"

func (r repo) UpdateBalance(ctx context.Context, customerID int64, balance int) error {
	_, err := r.db.ExecContext(ctx, updateBalanceQuery, balance, customerID)
	return err
}

var createOutboxQuery = "INSERT INTO payment_outboxes(content) VALUES (:content)"

func (r repo) CreateOutbox(ctx context.Context, outbox model.Outbox) error {
	_, err := r.db.NamedExecContext(ctx, createOutboxQuery, outbox)
	return err
}

var isProcessedQuery = "SELECT count(*) FROM processed_orders WHERE order_id = ?"

func (r repo) IsProcessed(ctx context.Context, orderID int64) (bool, error) {
	var res int
	err := r.db.GetContext(ctx, &res, isProcessedQuery, orderID)
	return res > 0, err
}

var markProcessedOrderQuery = "INSERT INTO processed_orders (order_id) VALUES (?)"

func (r repo) MarkProcessedOrder(ctx context.Context, orderID int64) error {
	_, err := r.db.Exec(markProcessedOrderQuery, orderID)
	return err
}

var createAccountQuery = "INSERT INTO accounts (customer_id, balance) VALUES (:customer_id, :balance)"

func (r repo) CreateAccount(ctx context.Context, account model.Account) error {
	_, err := r.db.NamedExecContext(ctx, createAccountQuery, account)
	return err
}

var getPendingOutboxQuery = "SELECT * FROM payment_outboxes WHERE status = ? LIMIT ?"

func (r repo) GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error) {
	var res []model.Outbox
	err := r.db.SelectContext(ctx, &res, getPendingOutboxQuery, model.OutboxPending, limit)
	return res, err
}

var markDoneOutboxesQuery = "UPDATE payment_outboxes SET status = ? WHERE id IN (?)"

func (r repo) MarkDoneOutboxes(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	query, args, err := sqlx.In(markDoneOutboxesQuery, model.OutboxCompleted, ids)
	if err != nil {
		return err
	}

	_, err = r.db.ExecContext(ctx, query, args...)
	return err
}
