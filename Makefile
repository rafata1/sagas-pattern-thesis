.PHONY: migrate-all
migrate-all:
	go run cmd/main.go migrate-up order
	go run cmd/main.go migrate-up inventory
	go run cmd/main.go migrate-up payment
