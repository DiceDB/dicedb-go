// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package dicedb

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/dicedb/dicedb-go/internal"
	"github.com/dicedb/dicedb-go/wire"
)

type ServerWire struct {
	*internal.ProtobufTCPWire
}

func NewServerWire(maxMsgSize int, keepAlive int32, clientFD int) (*ServerWire, *wire.WireError) {
	file := os.NewFile(uintptr(clientFD), fmt.Sprintf("client-connection-%d", clientFD))
	if file == nil {
		return nil, &wire.WireError{
			Kind:  wire.NotEstablished,
			Cause: fmt.Errorf("failed to create file from file descriptor"),
		}
	}

	conn, err := net.FileConn(file)
	if err != nil {
		return nil, &wire.WireError{
			Kind:  wire.NotEstablished,
			Cause: err,
		}
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			return nil, &wire.WireError{
				Kind:  wire.NotEstablished,
				Cause: fmt.Errorf("failed to set TCP_NODELAY: %w", err),
			}
		}
		if err := tcpConn.SetKeepAlive(true); err != nil {
			return nil, &wire.WireError{
				Kind:  wire.NotEstablished,
				Cause: fmt.Errorf("failed to set keepalive: %w", err),
			}
		}
		if err := tcpConn.SetKeepAlivePeriod(time.Duration(keepAlive) * time.Second); err != nil {
			return nil, &wire.WireError{
				Kind:  wire.NotEstablished,
				Cause: fmt.Errorf("failed to set keepalive period: %w", err),
			}
		}
	}

	w := &ServerWire{
		ProtobufTCPWire: internal.NewProtobufTCPWire(maxMsgSize, conn),
	}

	return w, nil
}

func (sw *ServerWire) Send(ctx context.Context, resp *wire.Response) *wire.WireError {
	return sw.ProtobufTCPWire.Send(resp)
}

func (sw *ServerWire) Receive() (*wire.Command, *wire.WireError) {
	cmd := &wire.Command{}

	if err := sw.ProtobufTCPWire.Receive(cmd); err != nil {
		return nil, err
	}

	return cmd, nil
}

func (sw *ServerWire) Close() {
	sw.ProtobufTCPWire.Close()
}
