package order

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rafata1/sagas-pattern-thesis/kafka"
	"github.com/rafata1/sagas-pattern-thesis/model"
	"github.com/rafata1/sagas-pattern-thesis/saga_event"
	"log"
	"time"
)

type IService interface {
	CreateOrder(ctx context.Context, order model.Order) (int64, error)
	RelayMessage(ctx context.Context, limit int) error
	ConsumeBills(ctx context.Context, stopAfter time.Duration)
	UpdateStatus(ctx context.Context, orderID int64, status model.OrderStatus) error
}

func NewService(
	repo IRepo,
	producer kafka.IProducer,
	billConsumer kafka.IConsumer,
) IService {
	return &service{
		repo:         repo,
		producer:     producer,
		billConsumer: billConsumer,
	}
}

type service struct {
	repo         IRepo
	billConsumer kafka.IConsumer
	producer     kafka.IProducer
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
		content, err := json.Marshal(saga_event.CreatedOrderEvent{
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

func (s service) ConsumeBills(ctx context.Context, stopAfter time.Duration) {
	startTime := time.Now()
	for {
		select {
		case msg := <-s.billConsumer.Messages():
			fmt.Printf("Received message: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value),
			)
			var event saga_event.BillOrderEvent
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
			err = s.UpdateStatus(ctx, event.OrderID, event.Status)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
		case err := <-s.billConsumer.Errors():
			log.Printf("Failed to consume message: %s", err)
		default:
			if stopAfter != 0 && time.Now().After(startTime.Add(stopAfter)) {
				return
			}
		}
	}
}

func (s service) UpdateStatus(ctx context.Context, orderID int64, status model.OrderStatus) error {
	return s.repo.UpdateStatus(ctx, orderID, status)
}
