package test

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/rafata1/sagas-pattern-thesis/config"
	"github.com/rafata1/sagas-pattern-thesis/kafka"
	"github.com/rafata1/sagas-pattern-thesis/model"
	"github.com/rafata1/sagas-pattern-thesis/service/inventory"
	"github.com/rafata1/sagas-pattern-thesis/service/order"
	"github.com/rafata1/sagas-pattern-thesis/service/payment"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func getOrderTestingDB() *sqlx.DB {
	db, err := sqlx.Connect("mysql", config.DefaultConfig.OrderConfig.DatabaseDSN)
	if err != nil {
		panic(err)
	}

	db.MustExec("TRUNCATE orders")
	db.MustExec("TRUNCATE order_outboxes")
	return db
}

func getInventoryTestingDB() *sqlx.DB {
	db, err := sqlx.Connect("mysql", config.DefaultConfig.InventoryConfig.DatabaseDSN)
	if err != nil {
		panic(err)
	}

	db.MustExec("TRUNCATE inventory")
	db.MustExec("TRUNCATE inventory_outboxes")
	db.MustExec("TRUNCATE processed_orders")
	return db
}

func getPaymentTestingDB() *sqlx.DB {
	db, err := sqlx.Connect("mysql", config.DefaultConfig.PaymentConfig.DatabaseDSN)
	if err != nil {
		panic(err)
	}

	db.MustExec("TRUNCATE accounts")
	db.MustExec("TRUNCATE payment_outboxes")
	db.MustExec("TRUNCATE processed_orders")
	return db
}

func Test_Full_Flow(t *testing.T) {
	repo := order.NewRepo(getOrderTestingDB())

	// PREPARE TOPICS
	orderCreatedTopic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	prepareInventoryTopic := getTopicTest(config.DefaultConfig.PrepareInventoryTopic)
	orderBillTopic := getTopicTest(config.DefaultConfig.OrderBillTopic)

	orderProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	billConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderBillTopic)
	orderService := order.NewService(repo, orderProducer, billConsumer, nil)
	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}
	ctx := context.Background()
	id, err := orderService.CreateOrder(ctx, inputOrder)
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
			Status:     model.OrderStatusPending,
			CreatedAt:  actualOrder.CreatedAt,
			UpdatedAt:  actualOrder.UpdatedAt,
		},
		actualOrder,
	)

	err = orderService.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}

	ordersConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryRepo := inventory.NewRepo(getInventoryTestingDB())
	err = inventoryRepo.CreateInventory(ctx, model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    100,
	})
	if err != nil {
		panic(err)
	}

	inventoryProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	inventoryService := inventory.NewService(inventoryRepo, ordersConsumer, nil, inventoryProducer)
	inventoryService.ConsumeOrders(ctx, 1*time.Second)

	actualInventory, err := inventoryRepo.GetInventory(ctx, 2)
	if err != nil {
		panic(err)
	}

	expectedInventory := model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    97,
		CreatedAt: actualInventory.CreatedAt,
		UpdatedAt: actualInventory.UpdatedAt,
	}
	assert.Equal(t, expectedInventory, actualInventory)
	inventoryService.RelayMessage(ctx, 10)

	paymentRepo := payment.NewRepo(getPaymentTestingDB())
	err = paymentRepo.CreateAccount(ctx, model.Account{
		CustomerID: 1,
		Balance:    100,
	})

	paymentConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	paymentProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderBillTopic)
	paymentService := payment.NewService(paymentRepo, paymentConsumer, paymentProducer)
	paymentService.ConsumePreparedOrders(ctx, 1*time.Second)
	paymentService.RelayMessage(ctx, 10)

	actualAccount, err := paymentRepo.GetAccount(ctx, 1)
	if err != nil {
		panic(err)
	}

	expectedAccount := model.Account{
		CustomerID: 1,
		Balance:    85,
		CreatedAt:  actualAccount.CreatedAt,
		UpdatedAt:  actualAccount.UpdatedAt,
	}

	assert.Equal(t, expectedAccount, actualAccount)

	orderService.ConsumeBills(ctx, 1*time.Second)

	actualOrder, err = repo.GetOrder(ctx, id)
	if err != nil {
		panic(err)
	}

	expectedOrder := model.Order{
		ID:         id,
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
		Status:     model.OrderStatusBilled,
		CreatedAt:  actualOrder.CreatedAt,
		UpdatedAt:  actualOrder.UpdatedAt,
	}

	assert.Equal(t, expectedOrder, actualOrder)
}

func getTopicTest(topic string) string {
	return fmt.Sprintf("%s_TEST_%d", topic, rand.Int())
}

func Test_Case_OutOfStock(t *testing.T) {
	repo := order.NewRepo(getOrderTestingDB())

	// PREPARE TOPICS
	orderCreatedTopic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	prepareInventoryTopic := getTopicTest(config.DefaultConfig.PrepareInventoryTopic)

	orderProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	orderService := order.NewService(repo, orderProducer, nil, inventoryConsumer)
	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}
	ctx := context.Background()
	orderID, err := orderService.CreateOrder(ctx, inputOrder)
	if err != nil {
		panic(err)
	}
	err = orderService.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}

	ordersConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryRepo := inventory.NewRepo(getInventoryTestingDB())
	err = inventoryRepo.CreateInventory(ctx, model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    2, // 2 < 3 so this case is out of stock
	})
	if err != nil {
		panic(err)
	}

	inventoryProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	inventoryService := inventory.NewService(inventoryRepo, ordersConsumer, nil, inventoryProducer)
	inventoryService.ConsumeOrders(ctx, 1*time.Second)

	actualInventory, err := inventoryRepo.GetInventory(ctx, 2)
	if err != nil {
		panic(err)
	}

	expectedInventory := model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    2,
		CreatedAt: actualInventory.CreatedAt,
		UpdatedAt: actualInventory.UpdatedAt,
	}
	assert.Equal(t, expectedInventory, actualInventory)
	inventoryService.RelayMessage(ctx, 10)

	orderService.ConsumeInventory(ctx, 1*time.Second)

	actualOrder, err := repo.GetOrder(ctx, orderID)
	if err != nil {
		panic(err)
	}

	expectedOrder := model.Order{
		ID:         orderID,
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
		Status:     model.OrderStatusFailedOutOfStock,
		CreatedAt:  actualOrder.CreatedAt,
		UpdatedAt:  actualOrder.UpdatedAt,
	}

	assert.Equal(t, expectedOrder, actualOrder)
}

func Test_Case_ExceedCreditLimit(t *testing.T) {
	repo := order.NewRepo(getOrderTestingDB())

	// PREPARE TOPICS
	orderCreatedTopic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	prepareInventoryTopic := getTopicTest(config.DefaultConfig.PrepareInventoryTopic)
	orderBillTopic := getTopicTest(config.DefaultConfig.OrderBillTopic)

	orderProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	billConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderBillTopic)
	orderService := order.NewService(repo, orderProducer, billConsumer, nil)
	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}
	ctx := context.Background()
	orderID, err := orderService.CreateOrder(ctx, inputOrder)
	if err != nil {
		panic(err)
	}
	err = orderService.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}

	ordersConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryRepo := inventory.NewRepo(getInventoryTestingDB())
	err = inventoryRepo.CreateInventory(ctx, model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    100,
	})
	if err != nil {
		panic(err)
	}

	inventoryProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	inventoryService := inventory.NewService(inventoryRepo, ordersConsumer, billConsumer, inventoryProducer)
	inventoryService.ConsumeOrders(ctx, 1*time.Second)

	actualInventory, err := inventoryRepo.GetInventory(ctx, 2)
	if err != nil {
		panic(err)
	}

	expectedInventory := model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    97,
		CreatedAt: actualInventory.CreatedAt,
		UpdatedAt: actualInventory.UpdatedAt,
	}
	assert.Equal(t, expectedInventory, actualInventory)
	inventoryService.RelayMessage(ctx, 10)

	paymentRepo := payment.NewRepo(getPaymentTestingDB())
	err = paymentRepo.CreateAccount(ctx, model.Account{
		CustomerID: 1,
		Balance:    14, // cost of order is 15, customer credit limit is 14 => ExceedCreditLimit
	})

	paymentConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	paymentProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderBillTopic)
	paymentService := payment.NewService(paymentRepo, paymentConsumer, paymentProducer)
	paymentService.ConsumePreparedOrders(ctx, 1*time.Second)
	paymentService.RelayMessage(ctx, 10)

	actualAccount, err := paymentRepo.GetAccount(ctx, 1)
	if err != nil {
		panic(err)
	}

	expectedAccount := model.Account{
		CustomerID: 1,
		Balance:    14,
		CreatedAt:  actualAccount.CreatedAt,
		UpdatedAt:  actualAccount.UpdatedAt,
	}

	assert.Equal(t, expectedAccount, actualAccount)

	orderService.ConsumeBills(ctx, 1*time.Second)

	actualOrder, err := repo.GetOrder(ctx, orderID)
	if err != nil {
		panic(err)
	}

	expectedOrder := model.Order{
		ID:         orderID,
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
		Status:     model.OrderStatusFailedExceedCreditLimit,
		CreatedAt:  actualOrder.CreatedAt,
		UpdatedAt:  actualOrder.UpdatedAt,
	}

	assert.Equal(t, expectedOrder, actualOrder)
}

func Test_Full_Flow_Inventory_Idempotence(t *testing.T) {
	repo := order.NewRepo(getOrderTestingDB())

	// PREPARE TOPICS
	orderCreatedTopic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	prepareInventoryTopic := getTopicTest(config.DefaultConfig.PrepareInventoryTopic)
	orderBillTopic := getTopicTest(config.DefaultConfig.OrderBillTopic)

	orderProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	billConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderBillTopic)
	orderService := order.NewService(repo, orderProducer, billConsumer, nil)
	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}
	ctx := context.Background()
	id, err := orderService.CreateOrder(ctx, inputOrder)
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
			Status:     model.OrderStatusPending,
			CreatedAt:  actualOrder.CreatedAt,
			UpdatedAt:  actualOrder.UpdatedAt,
		},
		actualOrder,
	)

	err = orderService.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}

	ordersConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryRepo := inventory.NewRepo(getInventoryTestingDB())
	err = inventoryRepo.CreateInventory(ctx, model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    100,
	})
	if err != nil {
		panic(err)
	}

	inventoryProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	inventoryService := inventory.NewService(inventoryRepo, ordersConsumer, nil, inventoryProducer)
	// receive message 2 times
	inventoryService.ConsumeOrders(ctx, 1*time.Second)
	inventoryService.ConsumeOrders(ctx, 1*time.Second)

	actualInventory, err := inventoryRepo.GetInventory(ctx, 2)
	if err != nil {
		panic(err)
	}

	expectedInventory := model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    97,
		CreatedAt: actualInventory.CreatedAt,
		UpdatedAt: actualInventory.UpdatedAt,
	}
	assert.Equal(t, expectedInventory, actualInventory)
	inventoryService.RelayMessage(ctx, 10)

	paymentRepo := payment.NewRepo(getPaymentTestingDB())
	err = paymentRepo.CreateAccount(ctx, model.Account{
		CustomerID: 1,
		Balance:    100,
	})

	paymentConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	paymentProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderBillTopic)
	paymentService := payment.NewService(paymentRepo, paymentConsumer, paymentProducer)
	paymentService.ConsumePreparedOrders(ctx, 1*time.Second)
	paymentService.RelayMessage(ctx, 10)

	actualAccount, err := paymentRepo.GetAccount(ctx, 1)
	if err != nil {
		panic(err)
	}

	expectedAccount := model.Account{
		CustomerID: 1,
		Balance:    85,
		CreatedAt:  actualAccount.CreatedAt,
		UpdatedAt:  actualAccount.UpdatedAt,
	}

	assert.Equal(t, expectedAccount, actualAccount)

	orderService.ConsumeBills(ctx, 1*time.Second)

	actualOrder, err = repo.GetOrder(ctx, id)
	if err != nil {
		panic(err)
	}

	expectedOrder := model.Order{
		ID:         id,
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
		Status:     model.OrderStatusBilled,
		CreatedAt:  actualOrder.CreatedAt,
		UpdatedAt:  actualOrder.UpdatedAt,
	}

	assert.Equal(t, expectedOrder, actualOrder)
}

func Test_Full_Flow_Payment_Idempotence(t *testing.T) {
	repo := order.NewRepo(getOrderTestingDB())

	// PREPARE TOPICS
	orderCreatedTopic := getTopicTest(config.DefaultConfig.OrderCreatedTopic)
	prepareInventoryTopic := getTopicTest(config.DefaultConfig.PrepareInventoryTopic)
	orderBillTopic := getTopicTest(config.DefaultConfig.OrderBillTopic)

	orderProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	billConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderBillTopic)
	orderService := order.NewService(repo, orderProducer, billConsumer, nil)
	inputOrder := model.Order{
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
	}
	ctx := context.Background()
	id, err := orderService.CreateOrder(ctx, inputOrder)
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
			Status:     model.OrderStatusPending,
			CreatedAt:  actualOrder.CreatedAt,
			UpdatedAt:  actualOrder.UpdatedAt,
		},
		actualOrder,
	)

	err = orderService.RelayMessage(ctx, 10)
	if err != nil {
		panic(err)
	}

	ordersConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, orderCreatedTopic)
	inventoryRepo := inventory.NewRepo(getInventoryTestingDB())
	err = inventoryRepo.CreateInventory(ctx, model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    100,
	})
	if err != nil {
		panic(err)
	}

	inventoryProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	inventoryService := inventory.NewService(inventoryRepo, ordersConsumer, nil, inventoryProducer)
	// receive message 2 times
	inventoryService.ConsumeOrders(ctx, 1*time.Second)
	inventoryService.ConsumeOrders(ctx, 1*time.Second)

	actualInventory, err := inventoryRepo.GetInventory(ctx, 2)
	if err != nil {
		panic(err)
	}

	expectedInventory := model.Inventory{
		ProductID: 2,
		UnitPrice: 5,
		Amount:    97,
		CreatedAt: actualInventory.CreatedAt,
		UpdatedAt: actualInventory.UpdatedAt,
	}
	assert.Equal(t, expectedInventory, actualInventory)
	inventoryService.RelayMessage(ctx, 10)

	paymentRepo := payment.NewRepo(getPaymentTestingDB())
	err = paymentRepo.CreateAccount(ctx, model.Account{
		CustomerID: 1,
		Balance:    100,
	})

	paymentConsumer := kafka.NewConsumer(config.DefaultConfig.KafkaHost, prepareInventoryTopic)
	paymentProducer := kafka.NewProducer(config.DefaultConfig.KafkaHost, orderBillTopic)
	paymentService := payment.NewService(paymentRepo, paymentConsumer, paymentProducer)
	// receive prepared order 2 times
	paymentService.ConsumePreparedOrders(ctx, 1*time.Second)
	paymentService.ConsumePreparedOrders(ctx, 1*time.Second)
	paymentService.RelayMessage(ctx, 10)

	actualAccount, err := paymentRepo.GetAccount(ctx, 1)
	if err != nil {
		panic(err)
	}

	expectedAccount := model.Account{
		CustomerID: 1,
		Balance:    85,
		CreatedAt:  actualAccount.CreatedAt,
		UpdatedAt:  actualAccount.UpdatedAt,
	}

	assert.Equal(t, expectedAccount, actualAccount)

	orderService.ConsumeBills(ctx, 1*time.Second)

	actualOrder, err = repo.GetOrder(ctx, id)
	if err != nil {
		panic(err)
	}

	expectedOrder := model.Order{
		ID:         id,
		CustomerID: 1,
		ProductID:  2,
		Amount:     3,
		Status:     model.OrderStatusBilled,
		CreatedAt:  actualOrder.CreatedAt,
		UpdatedAt:  actualOrder.UpdatedAt,
	}

	assert.Equal(t, expectedOrder, actualOrder)
}
