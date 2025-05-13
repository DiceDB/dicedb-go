package main

import (
	"fmt"
	"sync"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

func main() {
	client, err := dicedb.NewClient("localhost", 7379, &sync.WaitGroup{})
	if err != nil {
		fmt.Println(err)
	}
	defer client.Close()

	resp := client.Fire(&wire.Command{Cmd: "PING"})
	fmt.Println(resp)
}
