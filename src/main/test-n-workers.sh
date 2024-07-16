trap 'echo "Interrupted..." && exit(1)' INT
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so&
go run mrworker.go wc.so&
go run mrworker.go wc.so&
go run mrworker.go wc.so&