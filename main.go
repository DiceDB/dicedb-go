package dicedb

import (
	"fmt"
	"net"
	"strings"
	"time"
	"sync"
	"io"
	"syscall"

	"github.com/dicedb/dicedb-go/ironhawk"
	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
)

var mu sync.Mutex

type Client struct {
	id        string
	conn      net.Conn
	watchConn net.Conn
	watchCh   chan *wire.Response
	host      string
	port      int
}

type option func(*Client)

func newConn(host string, port int) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, 500*time.Second)
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
	}); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	return client, nil
}

func (c *Client) fire(cmd *wire.Command, co net.Conn) *wire.Response {
	if err := ironhawk.Write(co, cmd); err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	resp, err := ironhawk.Read(co)
	if err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	return resp
}

func (c *Client) Fire(cmd *wire.Command) *wire.Response {
	result := c.fire(cmd, c.conn)
	if result.Err != "" {
		if c.CheckAndReconnect(result.Err) {
			return c.Fire(cmd)
		}
	}
	return result
}

func (c *Client) CheckAndReconnect(err string) bool {
	fmt.Println(err)
	if err == io.EOF.Error() || strings.Contains(err, syscall.EPIPE.Error()) {
		fmt.Println("Error in connection. Reconnecting...")
		
		mu.Lock()
		defer mu.Unlock()
		
		cnew, err := NewClient(c.host, c.port)
		if err != nil {
			fmt.Println("Failed to reconnect:", err)
			return false
		}
		if c.conn != nil {
			c.conn.Close()
		}
		
		*c = *cnew
		return true
	}
	return false
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
			if ! c.CheckAndReconnect(err.Error()) {
				panic(err)
			}
		}

		c.watchCh <- resp
	}
}

func (c *Client) Close() {
	c.conn.Close()
}
