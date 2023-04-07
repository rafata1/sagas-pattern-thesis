package config

type Config struct {
	OrderConfig           ServiceConfig
	PaymentConfig         ServiceConfig
	InventoryConfig       ServiceConfig
	KafkaHost             string
	OrderCreatedTopic     string
	PrepareInventoryTopic string
	OrderBillTopic        string
}

type ServiceConfig struct {
	Name         string
	MigrationDir string
	DatabaseDSN  string
}

var DefaultConfig = Config{
	OrderConfig: ServiceConfig{
		Name:         "order",
		MigrationDir: "migration/order",
		DatabaseDSN:  "root:1@tcp(localhost:3306)/saga_order?parseTime=true",
	},
	InventoryConfig: ServiceConfig{
		Name:         "inventory",
		MigrationDir: "migration/inventory",
		DatabaseDSN:  "root:1@tcp(localhost:3306)/saga_inventory?parseTime=true",
	},
	PaymentConfig: ServiceConfig{
		Name:         "payment",
		MigrationDir: "migration/payment",
		DatabaseDSN:  "root:1@tcp(localhost:3306)/saga_payment?parseTime=true",
	},
	KafkaHost:             "localhost:29092",
	OrderCreatedTopic:     "ORDER_CREATED_TOPIC",
	PrepareInventoryTopic: "PREPARED_INVENTORY_TOPIC",
	OrderBillTopic:        "ORDER_BILL_TOPIC",
}
