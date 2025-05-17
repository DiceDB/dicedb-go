// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package internal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go/wire"
)

const prefixSize = 4 // bytes

type Status int

const (
	Open   Status = 1
	Closed Status = 2
)

type TCPWire struct {
	status     Status
	maxMsgSize int
	readMu     sync.Mutex
	reader     *bufio.Reader
	writeMu    sync.Mutex
	conn       net.Conn
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewTCPWire(maxMsgSize int, conn net.Conn) *TCPWire {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPWire{
		status:     Open,
		maxMsgSize: maxMsgSize,
		conn:       conn,
		reader:     bufio.NewReader(conn),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (w *TCPWire) Send(msg []byte) *wire.WireError {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.IsClosed() {
		return &wire.WireError{Kind: wire.Terminated, Cause: errors.New("trying to use closed wire")}
	}

	size := len(msg)
	buffer := make([]byte, prefixSize+size)

	prefix(size, buffer)
	copy(buffer[prefixSize:], msg)

	return w.write(buffer)
}

func (w *TCPWire) Receive() ([]byte, *wire.WireError) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	size, err := w.readPrefix()
	if err != nil {
		return nil, err
	}

	if w.IsClosed() {
		// If the wire is closed, we return an empty buffer to indicate that the read was interrupted
		return make([]byte, 0), nil
	}

	if size <= 0 {
		w.Close()
		return nil, &wire.WireError{
			Kind:  wire.CorruptMessage,
			Cause: fmt.Errorf("invalid message size: %d", size),
		}
	}

	if size > uint32(w.maxMsgSize) {
		w.Close()
		return nil, &wire.WireError{
			Kind:  wire.CorruptMessage,
			Cause: fmt.Errorf("message too large: %d bytes (max: %d)", size, w.maxMsgSize),
		}
	}

	buffer, err := w.readMessage(size)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (w *TCPWire) Close() {
	if w.IsClosed() {
		return
	}

	// Cancel the context to stop the read goroutine
	w.cancel()
	w.status = Closed
	err := w.conn.Close()
	if err != nil {
		slog.Warn("error closing network connection", "error", err)
		return
	}
}

func (w *TCPWire) ReadWithInterruption(buffer []byte, ctx context.Context) (int, error) {
	done := make(chan struct{})
	var n int
	var readErr error

	go func() {
		n, readErr = io.ReadFull(w.reader, buffer)
		close(done)
	}()

	select {
	case <-done:
		return n, readErr
	case <-ctx.Done():
		<-done
		return 0, nil
	}
}

func (w *TCPWire) readPrefix() (uint32, *wire.WireError) {
	buffer := make([]byte, prefixSize)
	delay := 5 * time.Millisecond
	const maxRetries = 5

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, err := w.ReadWithInterruption(buffer, w.ctx)
		if err == nil {
			return binary.BigEndian.Uint32(buffer), nil
		}

		lastErr = err

		// Retry only on timeout or temporary errors
		var opErr *net.OpError
		if errors.As(err, &opErr) && (opErr.Timeout() || opErr.Temporary()) {
			// Exponential backoff: doubling the delay for each retry
			time.Sleep(delay)
			delay = delay * 2
			continue
		}

		// Break out of the loop on non-retryable errors
		break
	}

	// Classify the final error
	switch {
	case errors.Is(lastErr, io.EOF):
		return 0, &wire.WireError{Kind: wire.Empty, Cause: lastErr}
	case errors.Is(lastErr, io.ErrUnexpectedEOF):
		w.Close()
		return 0, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	case strings.Contains(lastErr.Error(), "use of closed network connection"):
		w.Close()
		return 0, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	case func() bool {
		var opErr *net.OpError
		return errors.As(lastErr, &opErr) && (opErr.Timeout() || opErr.Temporary())
	}():
		// This case was already checked during retries, but it falls back here if it's a fatal error
		w.Close()
		return 0, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	default:
		// Handle other unknown error types by marking the status as closed
		w.Close()
		return 0, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	}
}

func (w *TCPWire) readMessage(size uint32) ([]byte, *wire.WireError) {
	buffer := make([]byte, size)
	delay := 5 * time.Millisecond
	const maxRetries = 5

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, err := w.ReadWithInterruption(buffer, w.ctx)
		if err == nil {
			return buffer, nil
		}

		lastErr = err

		// Retry only on timeout or temporary errors or EOF in case of partial write
		var opErr *net.OpError
		if (errors.As(err, &opErr) && (opErr.Timeout() || opErr.Temporary())) || errors.Is(err, io.EOF) {
			// Exponential backoff: doubling the delay for each retry
			time.Sleep(delay)
			delay = delay * 2
			continue
		}

		// Break out of the loop on non-retryable errors
		break
	}

	// Classify the final error
	switch {
	case errors.Is(lastErr, io.EOF):
		w.status = Closed
		return buffer, &wire.WireError{Kind: wire.CorruptMessage, Cause: lastErr}
	case errors.Is(lastErr, io.ErrUnexpectedEOF):
		w.status = Closed
		return buffer, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	case strings.Contains(lastErr.Error(), "use of closed network connection"):
		w.status = Closed
		return buffer, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	case func() bool {
		var opErr *net.OpError
		return errors.As(lastErr, &opErr) && (opErr.Timeout() || opErr.Temporary())
	}():
		// This case was already checked during retries, but it falls back here if it's a fatal error
		w.status = Closed
		return buffer, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	default:
		// Handle other unknown error types by marking the status as closed
		w.status = Closed
		return buffer, &wire.WireError{Kind: wire.Terminated, Cause: lastErr}
	}
}

func (w *TCPWire) write(buffer []byte) *wire.WireError {
	var totalWritten int
	partialWriteRetries := 0
	backoffRetries := 0

	const maxPartialWriteRetries = 10
	const maxBackoffRetries = 5

	backoffDelay := 5 * time.Millisecond
	var lastRetryableErr error

	for totalWritten < len(buffer) {
		n, err := w.conn.Write(buffer[totalWritten:])
		isPartial := n == 0 || n < len(buffer[totalWritten:]) || errors.Is(err, io.ErrShortWrite)
		totalWritten += n

		if err != nil && !errors.Is(err, io.ErrShortWrite) {
			lastRetryableErr = err
			if errors.Is(err, io.ErrClosedPipe) {
				w.status = Closed
				return &wire.WireError{Kind: wire.Terminated, Cause: err}
			}

			var opErr *net.OpError
			if errors.As(err, &opErr) && (opErr.Timeout() || opErr.Temporary()) {
				if backoffRetries > maxBackoffRetries {
					w.status = Closed
					return &wire.WireError{
						Kind:  wire.Terminated,
						Cause: fmt.Errorf("max backoff retries reached: %w", lastRetryableErr),
					}
				}

				backoffRetries++
				time.Sleep(backoffDelay)
				backoffDelay *= 2
				continue
			}

			w.status = Closed
			return &wire.WireError{Kind: wire.Terminated, Cause: err}
		}

		if isPartial {
			if partialWriteRetries >= maxPartialWriteRetries {
				w.status = Closed
				return &wire.WireError{
					Kind:  wire.Terminated,
					Cause: fmt.Errorf("max partial write retries reached: %w", err),
				}
			}

			partialWriteRetries++
			continue
		}
	}

	return nil
}

func prefix(msgSize int, buffer []byte) {
	binary.BigEndian.PutUint32(buffer[:prefixSize], uint32(msgSize))
}

func (w *TCPWire) IsClosed() bool {
	return w.status == Closed
}
