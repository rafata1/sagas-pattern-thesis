package inventory

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
	ConsumeOrders(ctx context.Context, stopAfter time.Duration)
	ConsumeBills(ctx context.Context, stopAfter time.Duration)
	RelayMessage(ctx context.Context, limit int) error
}

type service struct {
	ordersConsumer kafka.IConsumer
	billConsumer   kafka.IConsumer
	producer       kafka.IProducer
	repo           IRepo
}

func NewService(
	repo IRepo, ordersConsumer kafka.IConsumer, billConsumer kafka.IConsumer, producer kafka.IProducer,
) IService {
	return &service{
		ordersConsumer: ordersConsumer,
		repo:           repo,
		producer:       producer,
		billConsumer:   billConsumer,
	}
}

func (s service) ConsumeOrders(ctx context.Context, stopAfter time.Duration) {
	startTime := time.Now()
	for {
		select {
		case msg := <-s.ordersConsumer.Messages():
			fmt.Printf("Received message: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value),
			)
			var event saga_event.OrderEvent
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
			err = s.PrepareInventory(ctx, event)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
		case err := <-s.ordersConsumer.Errors():
			log.Printf("Failed to consume message: %s", err)
		default:
			if stopAfter != 0 && time.Now().After(startTime.Add(stopAfter)) {
				return
			}
		}
	}
}

func (s service) PrepareInventory(ctx context.Context, event saga_event.OrderEvent) error {
	return s.repo.Transact(ctx, func(ctx context.Context) error {
		isOrderProcessed, err := s.repo.IsProcessed(ctx, event.OrderID)
		if err != nil {
			return err
		}

		// order is already processed
		if isOrderProcessed {
			return nil
		}

		// lock and check inventory
		inventory, err := s.repo.LockInventoryForUpdate(ctx, event.ProductID)
		if err != nil {
			return err
		}

		var publishedEvent saga_event.OrderEvent
		if inventory.Amount >= event.Amount {
			err = s.repo.UpdateInventory(ctx, event.ProductID, inventory.Amount-event.Amount)
			if err != nil {
				return err
			}

			publishedEvent = saga_event.OrderEvent{
				OrderID:    event.OrderID,
				CustomerID: event.CustomerID,
				ProductID:  event.ProductID,
				Amount:     event.Amount,
				Status:     model.OrderStatusPrepared,
				Cost:       inventory.UnitPrice * event.Amount,
			}
		} else {
			publishedEvent = saga_event.OrderEvent{
				OrderID:    event.OrderID,
				CustomerID: event.CustomerID,
				ProductID:  event.ProductID,
				Amount:     event.Amount,
				Status:     model.OrderStatusFailedOutOfStock,
			}
		}

		content, _ := json.Marshal(publishedEvent)

		err = s.repo.MarkProcessedOrder(ctx, event.OrderID)
		if err != nil {
			return err
		}

		return s.repo.CreateOutbox(ctx, model.Outbox{
			Content: content,
		})
	})
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
			var event saga_event.OrderEvent
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
			if event.Status != model.OrderStatusBilled {
				err = s.RestoreInventory(ctx, event)
				if err != nil {
					panic(err)
				}
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

func (s service) RestoreInventory(ctx context.Context, event saga_event.OrderEvent) error {
	return s.repo.Transact(ctx, func(ctx context.Context) error {
		inventory, err := s.repo.LockInventoryForUpdate(ctx, event.ProductID)
		if err != nil {
			return err
		}

		return s.repo.UpdateInventory(ctx, event.ProductID, inventory.Amount+event.Amount)
	})
}
