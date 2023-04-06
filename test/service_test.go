package test

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/rafata1/sagas-pattern-thesis/config"
	"github.com/rafata1/sagas-pattern-thesis/kafka"
	"github.com/rafata1/sagas-pattern-thesis/model"
	"github.com/rafata1/sagas-pattern-thesis/service/order"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func getTestingDB() *sqlx.DB {
	db, err := sqlx.Connect("mysql", config.DefaultConfig.OrderConfig.DatabaseDSN)
	if err != nil {
		panic(err)
	}

	db.MustExec("TRUNCATE orders")
	db.MustExec("TRUNCATE order_outboxes")
	return db
}

func Test_CreateOrder(t *testing.T) {
	db := getTestingDB()
	repo := order.NewRepo(db)
	topic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	producer := kafka.NewProducer(
		config.DefaultConfig.KafkaHost,
		topic,
	)
	svc := order.NewService(repo, producer)

	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}

	ctx := context.Background()
	id, err := svc.CreateOrder(ctx, inputOrder)
	if err != nil {
		panic(err)
	}

	actualOrder, err := repo.GetOrder(ctx, id)
	if err != nil {
		panic(err)
	}

	assert.Equal(t,
		model.Order{
			ID:         id,
			CustomerID: 1,
			ProductID:  2,
			Amount:     3,
			Status:     model.OrderPending,
			CreatedAt:  actualOrder.CreatedAt,
			UpdatedAt:  actualOrder.UpdatedAt,
		},
		actualOrder,
	)

	err = svc.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}
}

func getTopicTest(topic string) string {
	return fmt.Sprintf("%s_TEST_%d", topic, rand.Int())
}
