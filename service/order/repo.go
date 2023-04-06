package order

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rafata1/sagas-pattern-thesis/model"
)

type IRepo interface {
	Transact(ctx context.Context, fn func(ctx context.Context) error) error
	CreateOrder(ctx context.Context, order model.Order) (int64, error)
	GetOrder(ctx context.Context, id int64) (model.Order, error)
	UpdateStatus(ctx context.Context, id int64, status model.OrderStatus) error
	CreateOutbox(ctx context.Context, outbox model.Outbox) error
	GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error)
	MarkDoneOutboxes(ctx context.Context, ids []int64) error
}

func NewRepo(db *sqlx.DB) IRepo {
	return &repo{
		db: db,
	}
}

type repo struct {
	db *sqlx.DB
}

var getOrderQuery = "SELECT * FROM orders WHERE id = ?"

func (r repo) GetOrder(ctx context.Context, id int64) (model.Order, error) {
	var res model.Order
	err := r.db.GetContext(ctx, &res, getOrderQuery, id)
	return res, err
}

var createOrderQuery = "INSERT INTO orders (customer_id, product_id, amount) VALUES (:customer_id, :product_id, :amount)"

func (r repo) CreateOrder(ctx context.Context, order model.Order) (int64, error) {
	res, err := r.db.NamedExecContext(ctx, createOrderQuery, order)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

var updateStatusQuery = "UPDATE orders SET status = ? WHERE id = ?"

func (r repo) UpdateStatus(ctx context.Context, id int64, status model.OrderStatus) error {
	_, err := r.db.ExecContext(ctx, updateStatusQuery, status, id)
	return err
}

var createOutboxQuery = "INSERT INTO order_outboxes(content) VALUES (:content)"

func (r repo) CreateOutbox(ctx context.Context, outbox model.Outbox) error {
	_, err := r.db.NamedExecContext(ctx, createOutboxQuery, outbox)
	return err
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

var getPendingOutboxQuery = "SELECT * FROM order_outboxes WHERE status = ? LIMIT ?"

func (r repo) GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error) {
	var res []model.Outbox
	err := r.db.SelectContext(ctx, &res, getPendingOutboxQuery, model.OutboxPending, limit)
	return res, err
}

var markDoneOutboxesQuery = "UPDATE order_outboxes SET status = ? WHERE id IN (?)"

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
