// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package internal

import (
	"net"

	"github.com/dicedb/dicedb-go/wire"

	"google.golang.org/protobuf/proto"
)

type ProtobufTCPWire struct {
	tcpWire Wire
}

func NewProtobufTCPWire(maxMsgSize int, conn net.Conn) *ProtobufTCPWire {
	return &ProtobufTCPWire{
		tcpWire: NewTCPWire(maxMsgSize, conn),
	}
}

func (w *ProtobufTCPWire) Send(msg proto.Message) *wire.WireError {
	buffer, err := proto.Marshal(msg)
	if err != nil {
		w.tcpWire.Close()
		return &wire.WireError{Kind: wire.CorruptMessage, Cause: err}
	}

	return w.tcpWire.Send(buffer)
}

func (w *ProtobufTCPWire) Receive(dst proto.Message) *wire.WireError {
	buffer, err := w.tcpWire.Receive()
	if err != nil {
		return err
	}

	uerr := proto.Unmarshal(buffer, dst)
	if uerr != nil {
		w.tcpWire.Close()
		return &wire.WireError{Kind: wire.CorruptMessage, Cause: uerr}
	}

	return nil
}

func (w *ProtobufTCPWire) Close() {
	w.tcpWire.Close()
}

func (w *ProtobufTCPWire) IsClosed() bool {
	return w.tcpWire.IsClosed()
}
