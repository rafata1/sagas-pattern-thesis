package inventory

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/rafata1/sagas-pattern-thesis/model"
)

type IRepo interface {
	Transact(ctx context.Context, fn func(ctx context.Context) error) error
	CreateOutbox(ctx context.Context, outbox model.Outbox) error
	IsProcessed(ctx context.Context, orderID int64) (bool, error)
	LockInventoryForUpdate(ctx context.Context, productID int64) (model.Inventory, error)
	UpdateInventory(ctx context.Context, productID int64, left int) error
	CreateInventory(ctx context.Context, inventory model.Inventory) error
	GetInventory(ctx context.Context, productID int64) (model.Inventory, error)
	GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error)
	MarkDoneOutboxes(ctx context.Context, ids []int64) error
	MarkProcessedOrder(ctx context.Context, orderID int64) error
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

var createOutboxQuery = "INSERT INTO inventory_outboxes(content) VALUES (:content)"

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

var lockInventoryForUpdateQuery = "SELECT * FROM inventory WHERE product_id = ? FOR UPDATE"

func (r repo) LockInventoryForUpdate(ctx context.Context, productID int64) (model.Inventory, error) {
	var res model.Inventory
	err := r.db.GetContext(ctx, &res, lockInventoryForUpdateQuery, productID)
	return res, err
}

var updateInventoryQuery = "UPDATE inventory SET amount = ? WHERE product_id = ?"

func (r repo) UpdateInventory(ctx context.Context, productID int64, left int) error {
	_, err := r.db.ExecContext(ctx, updateInventoryQuery, left, productID)
	return err
}

var createInventoryQuery = "INSERT INTO inventory (product_id, unit_price, amount) VALUES (:product_id, :unit_price, :amount)"

func (r repo) CreateInventory(ctx context.Context, inventory model.Inventory) error {
	_, err := r.db.NamedExecContext(ctx, createInventoryQuery, inventory)
	return err
}

var getInventoryQuery = "SELECT * FROM inventory WHERE product_id = ?"

func (r repo) GetInventory(ctx context.Context, productID int64) (model.Inventory, error) {
	var res model.Inventory
	err := r.db.GetContext(ctx, &res, lockInventoryForUpdateQuery, productID)
	return res, err
}

var getPendingOutboxQuery = "SELECT * FROM inventory_outboxes WHERE status = ? LIMIT ?"

func (r repo) GetPendingOutbox(ctx context.Context, limit int) ([]model.Outbox, error) {
	var res []model.Outbox
	err := r.db.SelectContext(ctx, &res, getPendingOutboxQuery, model.OutboxPending, limit)
	return res, err
}

var markDoneOutboxesQuery = "UPDATE inventory_outboxes SET status = ? WHERE id IN (?)"

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

var markProcessedOrderQuery = "INSERT INTO processed_orders (order_id) VALUES (?)"

func (r repo) MarkProcessedOrder(ctx context.Context, orderID int64) error {
	_, err := r.db.Exec(markProcessedOrderQuery, orderID)
	return err
}
