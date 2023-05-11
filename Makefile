.PHONY: migrate-all test
migrate-all:
	go run cmd/main.go migrate-up order
	go run cmd/main.go migrate-up inventory
	go run cmd/main.go migrate-up payment

test:
	go test -count=1 -p 1 ./test