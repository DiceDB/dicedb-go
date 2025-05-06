package dicedb

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
)

const maxResponseSize = 32 * 1024 * 1024 // 32 MB

type Client struct {
	id           string
	mainMu       sync.Mutex
	mainRetrier  *Retrier
	mainWire     *ClientWire
	watchRetrier *Retrier
	watchWire    *ClientWire
	watchCh      chan *wire.Result
	host         string
	port         int
}

type option func(*Client)

func WithID(id string) option {
	return func(c *Client) {
		c.id = id
	}
}

func NewClient(host string, port int, opts ...option) (*Client, error) {
	mainRetrier := NewRetrier(3, 5*time.Second)
	clientWire, err := ExecuteWithResult(mainRetrier, []wire.ErrKind{wire.NotEstablished}, func() (*ClientWire, *wire.WireError) {
		return NewClientWire(maxResponseSize, host, port)
	}, noop)

	if err != nil {
		if err.Kind == wire.NotEstablished {
			return nil, fmt.Errorf("could not connect to dicedb server after %d retries: %w", mainRetrier.maxRetries, err)
		}

		return nil, fmt.Errorf("unexpected error when establishing server connection, report this to dicedb maintainers: %w", err)
	}

	client := &Client{
		mainRetrier: mainRetrier,
		mainWire:    clientWire,
		host:        host,
		port:        port,
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.id == "" {
		client.id = uuid.New().String()
	}

	if resp := client.Fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{client.id, "command"},
	}); resp.Status == wire.Status_ERR {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Message)
	}

	return client, nil
}

func (c *Client) fire(cmd *wire.Command, clientWire *ClientWire) *wire.Result {
	c.mainMu.Lock()
	defer c.mainMu.Unlock()

	err := ExecuteVoid(c.mainRetrier, []wire.ErrKind{wire.Terminated}, func() *wire.WireError {
		return clientWire.Send(cmd)
	}, c.restoreMainWire)

	if err != nil {
		var message string

		switch err.Kind {
		case wire.Terminated:
			message = fmt.Sprintf("failied to send command, connection terminated: %s", err.Cause)
		case wire.CorruptMessage:
			message = fmt.Sprintf("failied to send command, corrupt message: %s", err.Cause)
		default:
			message = fmt.Sprintf("failed to send command: unrecognized error, this should be reported to DiceDB maintainers: %s", err.Cause)
		}

		return &wire.Result{
			Status:  wire.Status_ERR,
			Message: message,
		}
	}

	resp, err := clientWire.Receive()
	if err != nil {
		return &wire.Result{
			Status:  wire.Status_ERR,
			Message: fmt.Sprintf("failed to receive response: %s", err.Cause),
		}
	}

	return resp
}

func (c *Client) Fire(cmd *wire.Command) *wire.Result {
	return c.fire(cmd, c.mainWire)
}

func (c *Client) FireString(cmdStr string) *wire.Result {
	cmdStr = strings.TrimSpace(cmdStr)
	tokens := strings.Split(cmdStr, " ")

	var args []string
	var cmd = tokens[0]
	if len(tokens) > 1 {
		args = tokens[1:]
	}

	return c.Fire(&wire.Command{
		Cmd:  cmd,
		Args: args,
	})
}

func (c *Client) WatchCh() (<-chan *wire.Result, error) {
	var err *wire.WireError
	if c.watchCh != nil {
		return c.watchCh, nil
	}

	c.watchCh = make(chan *wire.Result)
	c.watchRetrier = NewRetrier(5, 5*time.Second)
	c.watchWire, err = NewClientWire(maxResponseSize, c.host, c.port)
	if err != nil {
		return nil, fmt.Errorf("Failed to establish watch connection with server: %w", err)
	}

	if resp := c.fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "watch"},
	}, c.watchWire); resp.Status == wire.Status_ERR {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Message)
	}

	go c.watch()

	return c.watchCh, nil
}

func (c *Client) watch() {
	for {
		resp, err := ExecuteWithResult(c.watchRetrier, []wire.ErrKind{wire.Terminated}, c.watchWire.Receive, c.restoreWatchWire)

		if err != nil {
			slog.Error("watch connection has been terminated due to an error", "err", err)
			close(c.watchCh)
			c.watchWire.Close()
			break
		}

		c.watchCh <- resp
	}
}

func (c *Client) Close() {
	c.mainWire.Close()
	if c.watchCh != nil {
		c.watchWire.Close()
		close(c.watchCh)
	}
}

func (c *Client) restoreMainWire() *wire.WireError {
	return c.restoreWire(c.mainWire)
}

func (c *Client) restoreWatchWire() *wire.WireError {
	return c.restoreWire(c.watchWire)
}

func (c *Client) restoreWire(dst *ClientWire) *wire.WireError {
	slog.Warn("trying to restore connection with server...")
	dst.Close()

	newWire, err := NewClientWire(maxResponseSize, c.host, c.port)
	if err != nil {
		slog.Warn("failed to restore connection with server", "error", err)
		return err
	}

	*dst = *newWire
	slog.Info("connection restored successfully")
	return nil
}

func noop() *wire.WireError {
	return nil
}
