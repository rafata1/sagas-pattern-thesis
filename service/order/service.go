package order

import (
	"context"
	"encoding/json"
	"github.com/rafata1/sagas-pattern-thesis/kafka"
	"github.com/rafata1/sagas-pattern-thesis/model"
)

type IService interface {
	CreateOrder(ctx context.Context, order model.Order) (int64, error)
	UpdateStatus(ctx context.Context, id int64, status model.OrderStatus) error
	RelayMessage(ctx context.Context, limit int) error
}

func NewService(repo IRepo, producer kafka.IProducer) IService {
	return &service{
		repo:     repo,
		producer: producer,
	}
}

type service struct {
	repo     IRepo
	producer kafka.IProducer
}

func (s service) CreateOrder(ctx context.Context, order model.Order) (int64, error) {
	var id int64
	err := s.repo.Transact(ctx, func(ctx context.Context) error {
		var err error
		// INSERT ORDER INTO DB
		id, err = s.repo.CreateOrder(ctx, order)
		if err != nil {
			return err
		}
		// INSERT EVENT INTO OUTBOX
		content, err := json.Marshal(CreatedOrderEvent{
			OrderID:    id,
			CustomerID: order.CustomerID,
			ProductID:  order.ProductID,
			Amount:     order.Amount,
		})
		if err != nil {
			return err
		}

		return s.repo.CreateOutbox(ctx, model.Outbox{Content: content})
	})
	return id, err
}

func (s service) UpdateStatus(ctx context.Context, id int64, status model.OrderStatus) error {
	//TODO implement me
	panic("implement me")
}

func (s service) RelayMessage(ctx context.Context, limit int) error {
	outboxes, err := s.repo.GetPendingOutbox(ctx, limit)
	if err != nil {
		return err
	}
	err = s.producer.Push(extractContents(outboxes))
	if err != nil {
		return err
	}

	err = s.repo.MarkDoneOutboxes(ctx, extractIDs(outboxes))
	return err
}

func extractIDs(outboxes []model.Outbox) []int64 {
	var res []int64
	for _, outbox := range outboxes {
		res = append(res, outbox.ID)
	}
	return res
}

func extractContents(outboxes []model.Outbox) [][]byte {
	var res [][]byte
	for _, outbox := range outboxes {
		res = append(res, outbox.Content)
	}
	return res
}
