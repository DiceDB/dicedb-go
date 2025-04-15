package dicedb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
)

const (
	maxResponseSize = 32 * 1024 * 1024 // 32 MB
	ioBufferSize    = 16 * 1024        // 16 KB
	idleTimeout     = 30 * time.Minute
)

var mu sync.Mutex

type Client struct {
	id             string
	transport      *ClientWire
	watchTransport *ClientWire
	watchCh        chan *wire.Response
	host           string
	port           int
}

type option func(*Client)

func WithID(id string) option {
	return func(c *Client) {
		c.id = id
	}
}

func NewClient(host string, port int, opts ...option) (*Client, error) {
	clientWire, err := NewClientWire(maxResponseSize, host, port)
	if err != nil {
		return nil, fmt.Errorf("Failed to establish connection with server: %w", err)
	}

	client := &Client{transport: clientWire, host: host, port: port}
	for _, opt := range opts {
		opt(client)
	}

	if client.id == "" {
		client.id = uuid.New().String()
	}

	if resp := client.Fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{client.id, "command"},
	}); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	return client, nil
}

func (c *Client) fire(cmd *wire.Command, transport *ClientWire) *wire.Response {
	if err := transport.Send(cmd); err != nil {
		return &wire.Response{
			Err: fmt.Sprintf("failed to send command: %s", err),
		}
	}

	resp, err := transport.Receive()
	if err != nil {
		return &wire.Response{
			Err: fmt.Sprintf("failed to receive response: %s", err),
		}
	}

	return resp
}

func (c *Client) Fire(cmd *wire.Command) *wire.Response {
	result := c.fire(cmd, c.transport)
	// TODO implement reconnect

	return result
}

func (c *Client) FireString(cmdStr string) *wire.Response {
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

func (c *Client) WatchCh() (<-chan *wire.Response, error) {
	var err error
	if c.watchCh != nil {
		return c.watchCh, nil
	}

	c.watchCh = make(chan *wire.Response)
	c.watchTransport, err = NewClientWire(maxResponseSize, c.host, c.port)
	if err != nil {
		return nil, fmt.Errorf("Failed to establish watch connection with server: %w", err)
	}

	if resp := c.fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "watch"},
	}, c.watchTransport); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	go c.watch()

	return c.watchCh, nil
}

func (c *Client) watch() {
	for {
		resp, _ := c.watchTransport.Receive()
		// TODO reconnect

		c.watchCh <- resp
	}
}

func (c *Client) Close() {
	c.transport.Close()
	if c.watchCh != nil {
		c.watchTransport.Close()
		close(c.watchCh)
	}
}
