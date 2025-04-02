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

const (
	poolSize = 10 // Maximum connections per host:port
)

var (
	connectionPools sync.Map
	fireCmdMutex    sync.Mutex
)

type Client struct {
	Id        string
	host      string
	port      int
	watchConn net.Conn
	watchCh   chan *wire.Response
}

type pool struct {
	mu        sync.Mutex
	available chan net.Conn
	host      string
	port      int
}

type option func(*Client)

func getPoolKey(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func getOrCreatePool(host string, port int) *pool {
	key := getPoolKey(host, port)

	if p, ok := connectionPools.Load(key); ok {
		return p.(*pool)
	}

	p := &pool{
		available: make(chan net.Conn, poolSize),
		host:      host,
		port:      port,
	}
	actual, _ := connectionPools.LoadOrStore(key, p)
	return actual.(*pool)
}

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
		c.Id = id
	}
}

func NewClient(host string, port int, opts ...option) (*Client, error) {
	client := &Client{
		host: host,
		port: port,
	}

	// Apply options
	for _, opt := range opts {
		opt(client)
	}

	// Generate ID if not provided
	if client.Id == "" {
		client.Id = uuid.New().String()
	}
	p := getOrCreatePool(host, port)
	conn, err := leaseConnectionFromPool(p, host, port)
	if err != nil {
		return nil, err
	}
	if resp := fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{client.Id, "command"},
	}, conn); resp.Err != "" {
		returnToConnectionPool(p, conn)
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	returnToConnectionPool(p, conn)

	return client, nil
}

func leaseConnectionFromPool(p *pool, host string, port int) (net.Conn, error) {
	// Try to get from pool first
	select {
	case conn := <-p.available:
		return conn, nil
	default:
		// Check if we can create a new connection
	}

	// Acquire lock to check/update pool state
	p.mu.Lock()
	// Check if there's now a connection in the pool (might have been added while we were waiting for the lock)
	select {
	case conn := <-p.available:
		p.mu.Unlock()
		fmt.Println("Reusing connection from pool (after acquiring lock)")
		return conn, nil
	default:
		// Still no connection available
	}

	// Check if we can create a new connection
	currentSize := len(p.available)
	if currentSize < poolSize {
		conn, err := newConn(host, port)
		// Unlock mutex before returning
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}

		select {
		case p.available <- conn:
			// Successfully added to pool try to get it back
			select {
			case poolConn := <-p.available:
				return poolConn, nil
			default:
				// Some other go-routing got created connection, just use this one directly
				return conn, nil
			}
		default:
			// Pool is full use the connection directly. This should not happen ideally
			return conn, nil
		}
	}

	p.mu.Unlock()
	// Wait with timeout for a connection
	select {
	case conn := <-p.available:
		return conn, nil
	case <-time.After(5 * time.Second):
		// This should not happen ideally as command execution is fast
		fmt.Println("Was not able to reuse connection from pool, hence creating new one")
		return newConn(host, port)
	}
}

func returnToConnectionPool(p *pool, conn net.Conn) {
	if conn == nil {
		return
	}

	// Try to return to pool if full close the connection
	select {
	case p.available <- conn:
		fmt.Println("Returned connection to pool")
		return
	default:
		fmt.Println("Pool is full, closing connection")
		conn.Close()
	}
}

func fire(cmd *wire.Command, conn net.Conn) *wire.Response {
	fireCmdMutex.Lock()
	defer fireCmdMutex.Unlock()

	if err := ironhawk.Write(conn, cmd); err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	resp, err := ironhawk.Read(conn)
	if err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	return resp
}

func (c *Client) Fire(cmd *wire.Command) *wire.Response {
	p := getOrCreatePool(c.host, c.port)
	conn, err := leaseConnectionFromPool(p, c.host, c.port)
	if err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	resp := fire(cmd, conn)
	returnToConnectionPool(p, conn)
	// On connection error retry once to get a connection from the pool again
	if resp.Err != "" && c.checkConnectionError(resp.Err) {
		conn, err := leaseConnectionFromPool(p, c.host, c.port)
		if err != nil {
			return &wire.Response{
				Err: fmt.Sprintf("reconnection failed: %s", err.Error()),
			}
		}

		resp = fire(cmd, conn)
		returnToConnectionPool(p, conn)
	}

	return resp
}

func (c *Client) checkConnectionError(err string) bool {
	return err == io.EOF.Error() || strings.Contains(err, syscall.EPIPE.Error())
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

	if resp := fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.Id, "watch"},
	}, c.watchConn); resp.Err != "" {
		return nil, fmt.Errorf("could not complete the handshake: %s", resp.Err)
	}

	go c.watch()

	return c.watchCh, nil
}

func getOrCreateClient(c *Client) (*Client, error) {
	if c == nil {
		return NewClient(c.host, c.port)
	}

	newClient, err := NewClient(c.host, c.port)
	if err != nil {
		return nil, err
	}

	if c.watchConn != nil {
		c.watchConn.Close()
	}

	return newClient, nil
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
	if c.watchConn != nil {
		c.watchConn.Close()
		c.watchConn = nil
	}
}
