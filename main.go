package dicedb

import (
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

const maxPoolSize = 10 // Maximum number of connections in the pool

type Client struct {
	id        string
	watchConn net.Conn
	watchCh   chan *wire.Response
	host      string
	port      int
	poolMu      sync.Mutex
	connections []net.Conn
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

func (c *Client) initializeConnectionPool() {
	conn, err := newConn(c.host, c.port)
	if err != nil {
		fmt.Printf("Error creating initial connection: %v\n", err)
		return
	}

	// Perform handshake
	cmd := &wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "command"},
	}

	if err := ironhawk.Write(conn, cmd); err != nil {
		conn.Close()
		fmt.Printf("Error during handshake: %v\n", err)
		return
	}

	resp, err := ironhawk.Read(conn)
	if err != nil || (resp != nil && resp.Err != "") {
		conn.Close()
		if resp != nil && resp.Err != "" {
			fmt.Printf("Handshake failed: %s\n", resp.Err)
		} else {
			fmt.Printf("Error reading handshake response: %v\n", err)
		}
		return
	}

	// Add the connection to the pool
	c.poolMu.Lock()
	c.connections = append(c.connections, conn)
	c.poolMu.Unlock()
}

func NewClient(host string, port int, opts ...option) (*Client, error) {
	client := &Client{
		host:        host,
		port:        port,
		connections: make([]net.Conn, 0),
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.id == "" {
		client.id = uuid.New().String()
	}

	go client.initializeConnectionPool()

	return client, nil
}

// getConn gets a connection from the pool or creates a new one if needed
func (c *Client) getConn() (net.Conn, error) {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()

	if len(c.connections) > 0 {
		// Get a connection from the pool
		conn := c.connections[len(c.connections)-1]
		c.connections = c.connections[:len(c.connections)-1]
		return conn, nil
	}

	// Create a new connection
	conn, err := newConn(c.host, c.port)
	if err != nil {
		return nil, err
	}

	// Perform handshake for the new connection
	cmd := &wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.id, "command"},
	}

	if err := ironhawk.Write(conn, cmd); err != nil {
		conn.Close()
		return nil, err
	}

	resp, err := ironhawk.Read(conn)
	if err != nil || (resp != nil && resp.Err != "") {
		conn.Close()
		if resp != nil && resp.Err != "" {
			return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
		}
		return nil, err
	}

	return conn, nil
}

func (c *Client) releaseConn(conn net.Conn) {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()

	if len(c.connections) >= maxPoolSize {
		conn.Close()
		return
	}

	c.connections = append(c.connections, conn)
}

func (c *Client) closeAllPooledConns() {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()

	for _, conn := range c.connections {
		conn.Close()
	}
	c.connections = c.connections[:0]
}

func (c *Client) PoolSize() int {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	return len(c.connections)
}

func (c *Client) fireWithPooledConn(cmd *wire.Command) *wire.Response {
	conn, err := c.getConn()
	if err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	defer c.releaseConn(conn)

	return c.fire(cmd, conn)
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
	result := c.fireWithPooledConn(cmd)
	if result.Err != "" {
		if c.checkAndReconnect(result.Err) {
			return c.Fire(cmd)
		}
	}
	return result
}

func (c *Client) checkAndReconnect(err string) bool {
	fmt.Println(err)
	if err == io.EOF.Error() || strings.Contains(err, syscall.EPIPE.Error()) {
		fmt.Println("Error in connection. Reconnecting...")

		newClient, err := getOrCreateClient(c)
		if err != nil {
			fmt.Println("Failed to reconnect:", err)
			return false
		}

		*c = *newClient
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

	newClient, err := NewClient(c.host, c.port,
		WithID(c.id),
	)
	if err != nil {
		return nil, err
	}

	return newClient, nil
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
			if !c.checkAndReconnect(err.Error()) {
				panic(err)
			}
		}

		c.watchCh <- resp
	}
}

func (c *Client) Close() {
	// Close the watch connection if it exists
	if c.watchConn != nil {
		c.watchConn.Close()
	}

	// Close all connections in the pool
	c.closeAllPooledConns()
}
