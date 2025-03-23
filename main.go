package dicedb

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go/ironhawk"
	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
)

type Client struct {
	id     string
	cancel context.CancelFunc

	connWriteMu sync.Mutex
	conn        net.Conn

	pendingMu sync.Mutex
	pending   map[uuid.UUID]chan *wire.Response

	watchConn net.Conn
	watchCh   chan *wire.Response
	host      string
	port      int
}

type option func(*Client)

func newConn(host string, port int) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func WithID(id string) option {
	return func(c *Client) {
		c.id = id
	}
}

func NewClient(host string, port int, opts ...option) (*Client, error) {
	conn, err := newConn(host, port)
	if err != nil {
		return nil, err
	}

	pending := make(map[uuid.UUID]chan *wire.Response)

	client := &Client{conn: conn, host: host, port: port, pending: pending}
	for _, opt := range opts {
		opt(client)
	}

	if client.id == "" {
		client.id = uuid.New().String()
	}
	ctx, cancel := context.WithCancel(context.Background())
	client.cancel = cancel

	go client.readLoop(ctx)
	if resp := client.Fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{client.id, "command"},
	}); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	return client, nil
}

func (c *Client) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.conn.Close()
			return
		default:
			resp, err := ironhawk.Read(c.conn)
			if err != nil {
				panic(err)
			}

			id := uuid.MustParse(resp.GetCorrId())

			c.pendingMu.Lock()
			ch, exists := c.pending[id]
			if exists {
				delete(c.pending, id)
				c.pendingMu.Unlock()
				ch <- resp
			} else {
				c.pendingMu.Unlock()
				panic("no id response received")
			}
		}
	}
}

func (c *Client) fire(cmd *wire.Command, co net.Conn) *wire.Response {
	id := uuid.New()
	cmd.CorrId = id.String()

	ch := make(chan *wire.Response)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	c.connWriteMu.Lock()
	err := ironhawk.Write(co, cmd)
	c.connWriteMu.Unlock()

	if err != nil {
		return &wire.Response{
			Err:    err.Error(),
			CorrId: cmd.GetCorrId(),
		}
	}

	select {
	case r := <-ch:
		return r
	case <-time.After(time.Second * 10):
		return &wire.Response{
			Err:    "timeout",
			CorrId: cmd.GetCorrId(),
		}
	}
}

func (c *Client) Fire(cmd *wire.Command) *wire.Response {
	return c.fire(cmd, c.conn)
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
	c.watchConn, err = newConn(c.host, c.port)
	if err != nil {
		return nil, err
	}

	if resp := c.fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "watch"},
	}, c.watchConn); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	go c.watch()

	return c.watchCh, nil
}

func (c *Client) watch() {
	for {
		resp, err := ironhawk.Read(c.watchConn)
		if err != nil {
			// TODO: handle this better
			// send the error to the user. maybe through context?
			panic(err)
		}

		c.watchCh <- resp
	}
}

func (c *Client) Close() {
	c.cancel()
}
