package payment

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
	ConsumePreparedOrders(ctx context.Context, stopAfter time.Duration)
	Pay(ctx context.Context, event saga_event.PrepareInventoryEvent) error
	RelayMessage(ctx context.Context, limit int) error
}

type service struct {
	ordersConsumer kafka.IConsumer
	producer       kafka.IProducer
	repo           IRepo
}

func NewService(repo IRepo, orderConsumer kafka.IConsumer, producer kafka.IProducer) IService {
	return &service{
		ordersConsumer: orderConsumer,
		producer:       producer,
		repo:           repo,
	}
}

func (s service) ConsumePreparedOrders(ctx context.Context, stopAfter time.Duration) {
	startTime := time.Now()
	for {
		select {
		case msg := <-s.ordersConsumer.Messages():
			fmt.Printf("Received message: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value),
			)
			var event saga_event.PrepareInventoryEvent
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				panic(err)
				// mark message on queue as not done
			}
			err = s.Pay(ctx, event)
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

func (s service) Pay(ctx context.Context, event saga_event.PrepareInventoryEvent) error {
	// only pay with prepared order
	if event.Status != model.OrderStatusPrepared {
		return nil
	}

	return s.repo.Transact(ctx, func(ctx context.Context) error {
		isProcessedOrder, err := s.repo.IsProcessed(ctx, event.OrderID)
		if err != nil {
			return err
		}

		if isProcessedOrder {
			return nil
		}

		account, err := s.repo.LockAccountForUpdate(ctx, event.CustomerID)
		if err != nil {
			return err
		}

		var publishEvent saga_event.BillOrderEvent
		if account.Balance >= event.Cost {
			err = s.repo.UpdateBalance(ctx, event.CustomerID, account.Balance-event.Cost)
			if err != nil {
				return err
			}
			publishEvent = saga_event.BillOrderEvent{
				OrderID: event.OrderID,
				Status:  model.OrderStatusBilled,
			}
		} else {
			publishEvent = saga_event.BillOrderEvent{
				OrderID: event.OrderID,
				Status:  model.OrderStatusFailedExceedCreditLimit,
			}
		}

		err = s.repo.MarkProcessedOrder(ctx, event.OrderID)
		if err != nil {
			return err
		}

		content, _ := json.Marshal(publishEvent)
		return s.repo.CreateOutbox(ctx, model.Outbox{Content: content})
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
