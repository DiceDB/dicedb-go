package dicedb

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dicedb/dicedb-go/ironhawk"
	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
)

var mu sync.Mutex

type Client struct {
	id          string
	conn        net.Conn
	watchConn   net.Conn
	watchCancel context.CancelFunc
	watchCh     chan *wire.Result
	host        string
	port        int
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

	client := &Client{conn: conn, host: host, port: port}
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

func (c *Client) fire(cmd *wire.Command, co net.Conn) *wire.Result {
	if err := ironhawk.Write(co, cmd); err != nil {
		return &wire.Result{
			Status:  wire.Status_ERR,
			Message: err.Error(),
		}
	}

	resp, err := ironhawk.Read(co)
	if err != nil {
		return &wire.Result{
			Status:  wire.Status_ERR,
			Message: err.Error(),
		}
	}

	return resp
}

func (c *Client) Fire(cmd *wire.Command) *wire.Result {
	result := c.fire(cmd, c.conn)
	if result.Status == wire.Status_ERR {
		if c.checkAndReconnect(result.Message) {
			return c.Fire(cmd)
		}
	}
	return result
}

func (c *Client) checkAndReconnect(err string) bool {
	if err == io.EOF.Error() || strings.Contains(err, syscall.EPIPE.Error()) {
		fmt.Println("reconnecting ...")
		nc, err := getOrCreateClient(c)
		if err != nil {
			fmt.Println("failed to reconnect. error:", err)
			return false
		}
		*c = *nc
		return true
	}
	return false
}

func getOrCreateClient(c *Client) (*Client, error) {
	mu.Lock()
	defer mu.Unlock()

	if c == nil {
		return NewClient(c.host, c.port)
	}

	newClient, err := NewClient(c.host, c.port)
	if err != nil {
		return nil, err
	}

	if c.conn != nil {
		c.conn.Close()
	}

	return newClient, nil
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
	var err error
	if c.watchCh != nil {
		return c.watchCh, nil
	}

	c.watchCh = make(chan *wire.Result)
	c.watchConn, err = newConn(c.host, c.port)
	if err != nil {
		return nil, err
	}

	if resp := c.fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "watch"},
	}, c.watchConn); resp.Status == wire.Status_ERR {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Message)
	}

	ctx, watchCancel := context.WithCancel(context.Background())
	c.watchCancel = watchCancel
	go c.watch(ctx)

	return c.watchCh, nil
}

func (c *Client) watch(ctx context.Context) {
	defer func() {
		if c.watchConn != nil {
			c.watchConn.Close()
		}
		if c.watchCh != nil {
			close(c.watchCh)
			c.watchCh = nil
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.watchConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			resp, err := ironhawk.Read(c.watchConn)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// TODO: handle this better
				// send the error to the user. maybe through context?
				if !c.checkAndReconnect(err.Error()) {
					panic(err)
				}
			}

			if c.watchCh != nil {
				c.watchCh <- resp
			}
		}
	}
}

func (c *Client) Close() {
	c.conn.Close()
	c.WatchConnClose()
}

func (c *Client) WatchConnClose() {
	if(c.watchConn != nil) {
		c.watchCancel()
	}
}
