module github.com/redis/go-redis/extra/rediscensus/v9

go 1.23.0

toolchain go1.23.2

replace github.com/dicedb/dicedb-go => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/dicedb/dicedb-go v0.0.0-20241011194507-ad62a2dfc08e
	github.com/redis/go-redis/extra/rediscmd/v9 v9.5.3
	go.opencensus.io v0.24.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
)
