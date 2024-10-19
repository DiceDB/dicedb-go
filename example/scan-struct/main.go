package main

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"github.com/dicedb/dicedb-go"
)

type Model struct {
	Str1    string   `dicedb:"str1"`
	Str2    string   `dicedb:"str2"`
	Bytes   []byte   `dicedb:"bytes"`
	Int     int      `dicedb:"int"`
	Bool    bool     `dicedb:"bool"`
	Ignored struct{} `dicedb:"-"`
}

func main() {
	ctx := context.Background()

	rdb := dicedb.NewClient(&dicedb.Options{
		Addr: ":7379",
	})
	_ = rdb.FlushDB(ctx).Err()

	// Set some fields.
	if _, err := rdb.Pipelined(ctx, func(rdb dicedb.Pipeliner) error {
		rdb.HSet(ctx, "key", "str1", "hello")
		rdb.HSet(ctx, "key", "str2", "world")
		rdb.HSet(ctx, "key", "int", 123)
		rdb.HSet(ctx, "key", "bool", 1)
		rdb.HSet(ctx, "key", "bytes", []byte("this is bytes !"))
		return nil
	}); err != nil {
		panic(err)
	}

	var model1, model2 Model

	// Scan all fields into the model.
	if err := rdb.HGetAll(ctx, "key").Scan(&model1); err != nil {
		panic(err)
	}

	// Or scan a subset of the fields.
	if err := rdb.HMGet(ctx, "key", "str1", "int").Scan(&model2); err != nil {
		panic(err)
	}

	spew.Dump(model1)
	// Output:
	// (main.Model) {
	// 	Str1: (string) (len=5) "hello",
	// 	Str2: (string) (len=5) "world",
	// 	Bytes: ([]uint8) (len=15 cap=16) {
	// 	 00000000  74 68 69 73 20 69 73 20  62 79 74 65 73 20 21     |this is bytes !|
	// 	},
	// 	Int: (int) 123,
	// 	Bool: (bool) true,
	// 	Ignored: (struct {}) {
	// 	}
	// }

	spew.Dump(model2)
	// Output:
	// (main.Model) {
	// 	Str1: (string) (len=5) "hello",
	// 	Str2: (string) "",
	// 	Bytes: ([]uint8) <nil>,
	// 	Int: (int) 123,
	// 	Bool: (bool) false,
	// 	Ignored: (struct {}) {
	// 	}
	// }
}
