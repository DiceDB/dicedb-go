package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

func main() {
	var wg sync.WaitGroup

	client, err := dicedb.NewClient("localhost", 7379, &wg)
	if err != nil {
		fmt.Println(err)
	}

	resp := client.Fire(&wire.Command{Cmd: "PING"})
	fmt.Println(resp)

	wg.Add(1)
	go func(client *dicedb.Client) {
		dicedb.ListenForMessages(client, func(message string) {
			fmt.Println("Received message from second watcher:", message)
		})
		wg.Done()
	}(client)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k1", "v1"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k1"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k2", "v2"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k2"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k3", "v3"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k3"}})
	fmt.Println(resp)

	time.Sleep(30 * time.Second)

	resp1 := client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k1"}})
	fmt.Println("waited k1 ", resp1)

	resp2 := client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k2"}})
	fmt.Println("waited k2 ", resp2)

	resp3 := client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k3"}})
	fmt.Println("waited k3 ", resp3)

	wg.Wait()
}
