module github.com/redis/go-redis/example/redis-bloom

go 1.23.0

toolchain go1.23.2

replace github.com/dicedb/dicedb-go => ../..

require github.com/dicedb/dicedb-go v0.0.0-20241011194507-ad62a2dfc08e

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
