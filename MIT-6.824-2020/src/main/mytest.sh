go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-*.txt
go run mrworker.go wc.so
cat mr-out* | sort > all.txt
 diff all.txt ./mr-tmp/mr-correct-wc.txt