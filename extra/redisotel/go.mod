module github.com/redis/go-redis/extra/redisotel/v9

go 1.23.0

toolchain go1.23.2

replace github.com/dicedb/dicedb-go => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/dicedb/dicedb-go v0.0.0-20241011194507-ad62a2dfc08e
	github.com/redis/go-redis/extra/rediscmd/v9 v9.5.3
	go.opentelemetry.io/otel v1.22.0
	go.opentelemetry.io/otel/metric v1.22.0
	go.opentelemetry.io/otel/sdk v1.22.0
	go.opentelemetry.io/otel/trace v1.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	golang.org/x/sys v0.16.0 // indirect
)
