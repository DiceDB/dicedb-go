// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package dicedb

import (
	"fmt"
	"net"
	"time"

	"github.com/dicedb/dicedb-go/internal"
	"github.com/dicedb/dicedb-go/wire"
)

type ClientWire struct {
	*internal.ProtobufTCPWire
}

func NewClientWire(maxMsgSize int, host string, port int) (*ClientWire, *wire.WireError) {
	// Use net.JoinHostPort to properly handle both IPv4 and IPv6 addresses
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, &wire.WireError{Kind: wire.NotEstablished, Cause: err}
	}
	w := &ClientWire{
		ProtobufTCPWire: internal.NewProtobufTCPWire(maxMsgSize, conn),
	}

	return w, nil
}

func (cw *ClientWire) Send(cmd *wire.Command) *wire.WireError {
	return cw.ProtobufTCPWire.Send(cmd)
}

func (cw *ClientWire) Receive() (*wire.Result, *wire.WireError) {
	resp := &wire.Result{}
	err := cw.ProtobufTCPWire.Receive(resp)

	return resp, err
}

func (cw *ClientWire) Close() {
	cw.ProtobufTCPWire.Close()
}

func (cw *ClientWire) IsClosed() bool {
	return cw.ProtobufTCPWire.IsClosed()
}
