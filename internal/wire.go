package internal

import "github.com/dicedb/dicedb-go/wire"

type Wire interface {
	Send([]byte) *wire.WireError
	Receive() ([]byte, *wire.WireError)
	Close()
	IsClosed() bool
}
