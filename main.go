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
	// TODO: We can make this configurable
	poolSize = 10
)

var (
	connectionPools sync.Map
)

type Client struct {
	Id        string
	host      string
	port      int
	watchConn net.Conn
	watchCh   chan *wire.Response
}

type pool struct {
	mu             sync.Mutex
	availableConns chan *pooledConn
	host           string
	port           int
	activeConns    int
}

type pooledConn struct {
	conn     net.Conn
	clientID string // Tracks which client last used this connection
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
		availableConns: make(chan *pooledConn, poolSize),
		host:           host,
		port:           port,
		activeConns:    0,
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
	_, pConn, err := p.leaseConnectionFromPool(host, port, client.Id)
	defer p.returnToConnectionPool(pConn)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (p *pool) leaseConnectionFromPool(host string, port int, clientID string) (net.Conn, *pooledConn, error) {
	// Try to get a connection from the pool immediately
	select {
	case pConn := <-p.availableConns:
		conn := pConn.conn

		// If this connection was last used by a different client, re-handshake
		if pConn.clientID != clientID {
			if resp := fire(&wire.Command{
				Cmd:  "HANDSHAKE",
				Args: []string{clientID, "command"},
			}, conn); resp.Err != "" {
				// Handshake failed, connection might be broken
				conn.Close()
				p.mu.Lock()
				p.activeConns--
				p.mu.Unlock()
				// Try creating a new connection
				newConn, newPConn, err := p.createNewConnection(host, port, clientID)
				return newConn, newPConn, err
			}
			// Update the client ID for this connection
			pConn.clientID = clientID
		}

		return conn, pConn, nil
	default:
		// No connection available hence go ahead and acquire lock and create
	}

	// Acquire mutex to check pool state and create a new connection
	return p.createNewConnection(host, port, clientID)
}

// createNewConnection creates a new connection and associates it with the client
func (p *pool) createNewConnection(host string, port int, clientID string) (net.Conn, *pooledConn, error) {
	p.mu.Lock()

	// Check if we can create a new connection
	if p.activeConns < poolSize {
		// Increment the counter before releasing the lock
		p.activeConns++
		p.mu.Unlock()

		conn, err := newConn(host, port)
		if err != nil {
			// Decrement the counter if connection creation fails
			p.mu.Lock()
			p.activeConns--
			p.mu.Unlock()
			return nil, nil, err
		}

		// Do handshake for the new connection
		if resp := fire(&wire.Command{
			Cmd:  "HANDSHAKE",
			Args: []string{clientID, "command"},
		}, conn); resp.Err != "" {
			conn.Close()
			p.mu.Lock()
			p.activeConns--
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("handshake failed: %s", resp.Err)
		}

		pConn := &pooledConn{
			conn:     conn,
			clientID: clientID,
		}

		return conn, pConn, nil
	}

	// We've reached max pool size
	p.mu.Unlock()

	// Wait with timeout for a connection
	select {
	case pConn := <-p.availableConns:
		conn := pConn.conn

		// If this connection was last used by a different client, re-handshake
		if pConn.clientID != clientID {
			if resp := fire(&wire.Command{
				Cmd:  "HANDSHAKE",
				Args: []string{clientID, "command"},
			}, conn); resp.Err != "" {
				// Handshake failed, connection might be broken
				conn.Close()
				p.mu.Lock()
				p.activeConns--
				p.mu.Unlock()
				return nil, nil, fmt.Errorf("handshake failed on reused connection: %s", resp.Err)
			}
			// Update the client ID
			pConn.clientID = clientID
		}

		return conn, pConn, nil
	// TODO: We can make this configurable
	case <-time.After(10 * time.Second):
		return nil, nil, fmt.Errorf("timed out waiting for a connection from the pool")
	}
}

func (p *pool) returnToConnectionPool(pConn *pooledConn) {
	if pConn == nil || pConn.conn == nil {
		return
	}

	// Put the connection back in the pool
	select {
	case p.availableConns <- pConn:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		pConn.conn.Close()

		// Decrement the active connection counter
		p.mu.Lock()
		p.activeConns--
		p.mu.Unlock()
	}
}

func fire(cmd *wire.Command, conn net.Conn) *wire.Response {
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
	// Check if this is a watch-related command
	isWatchCmd := strings.Contains(cmd.Cmd, ".WATCH")

	// For watch commands, use the dedicated watch connection if available
	if isWatchCmd && c.watchConn != nil {
		return fire(cmd, c.watchConn)
	}

	// For all other commands or if watch connection not established yet
	p := getOrCreatePool(c.host, c.port)
	conn, pConn, err := p.leaseConnectionFromPool(c.host, c.port, c.Id)
	defer p.returnToConnectionPool(pConn)

	if err != nil {
		return &wire.Response{
			Err: err.Error(),
		}
	}

	resp := fire(cmd, conn)
	// On connection error retry once
	if resp.Err != "" && c.checkConnectionError(resp.Err) {
		conn, pConn, err := p.createNewConnection(c.host, c.port, c.Id)
		defer p.returnToConnectionPool(pConn)

		if err != nil {
			return &wire.Response{
				Err: fmt.Sprintf("reconnection failed: %s", err.Error()),
			}
		}

		resp = fire(cmd, conn)
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
	if c.watchCh != nil {
		return c.watchCh, nil
	}

	c.watchCh = make(chan *wire.Response)

	// Create a dedicated watch connection
	var err error
	c.watchConn, err = newConn(c.host, c.port)
	if err != nil {
		return nil, err
	}

	// Do handshake with the same client ID but for "watch" purpose
	if resp := fire(&wire.Command{
		Cmd:  "HANDSHAKE",
		Args: []string{c.Id, "watch"},
	}, c.watchConn); resp.Err != "" {
		c.watchConn.Close()
		c.watchConn = nil
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

func (c *Client) checkAndReconnect(err string) bool {
	fmt.Println(err)
	if err == io.EOF.Error() || strings.Contains(err, syscall.EPIPE.Error()) {
		fmt.Println("Error in connection. Reconnecting...")

		// Close the broken connection
		if c.watchConn != nil {
			c.watchConn.Close()
		}

		var err error
		c.watchConn, err = newConn(c.host, c.port)
		if err != nil {
			fmt.Println("Failed to reconnect:", err)
			return false
		}

		// Perform handshake
		if resp := fire(&wire.Command{
			Cmd:  "HANDSHAKE",
			Args: []string{c.Id, "watch"},
		}, c.watchConn); resp.Err != "" {
			fmt.Println("Failed to complete handshake:", resp.Err)
			c.watchConn.Close()
			c.watchConn = nil
			return false
		}

		return true
	}
	return false
}

func (c *Client) Close() {
	if c.watchConn != nil {
		c.watchConn.Close()
		c.watchConn = nil
	}
}
