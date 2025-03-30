package dicedb

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/dgryski/go-farm"
	"github.com/dicedb/dicedb-go/ironhawk"
	"github.com/dicedb/dicedb-go/wire"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
)

type Client struct {
	id        string
	conn      net.Conn
	watchConn net.Conn
	watchCh   chan *wire.Response
	host      string
	port      int
	lcache    *ristretto.Cache
	mu        sync.RWMutex // Mutex for thread-safe operations
	wg        sync.WaitGroup
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

	// Initialize Ristretto cache
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M)
		MaxCost:     1 << 30, // maximum cost of cache (1GB)
		BufferItems: 64,      // number of keys per Get buffer
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Ristretto cache: %w", err)
	}

	client := &Client{conn: conn, host: host, port: port, lcache: cache}
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

	client.wg.Add(1)
	go func(client *Client) {
		fmt.Println("Listening for messages")
		ListenForMessages(client, func(message string) {
		})
		client.wg.Done()
	}(client)

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
	// TODO: handle this better by refining read-only commands
	if cmd.Cmd == "GET" && len(cmd.Args) > 0 {
		key := cmd.Args[0]

		watch_cmd := &wire.Command{
			Cmd:  "GET.WATCH",
			Args: []string{key},
		}

		fp := Fingerprint(watch_cmd)

		// Check if the key is in the cache
		if value, found := c.lcache.Get(fp); found {
			return &wire.Response{
				Value: &wire.Response_VStr{VStr: value.(string)},
			}
		}

		// If not in cache, send the command to the server
		resp := c.fire(cmd, c.conn)

		// If the response is successful, store it in the cache
		// and subscribe to the key to keep the value updated
		if resp.Err == "" {
			c.lcache.Set(fp, resp.GetVStr(), 1) // Store only the string value
			c.lcache.Wait()                     // Ensure value is stored before proceeding

			c.wg.Add(1)
			go func() {
				Subscribe(c, key)
				c.wg.Done()
			}()
		}

		return resp
	}

	// For non-GET commands, just send to the server
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
	c.wg.Wait()
	c.conn.Close()
}

// SubscriptionManager methods
func Subscribe(client *Client, watch_key string) {
	resp := client.Fire(&wire.Command{
		Cmd:  "GET.WATCH",
		Args: []string{watch_key},
	})
	if resp.Err != "" {
		fmt.Println("error subscribing:", resp.Err)
	}
}

func ListenForMessages(client *Client, onMessage func(message string)) {
	ch, err := client.WatchCh()
	if err != nil {
		panic(err)
	}
	for resp := range ch {
		var fp uint32
		// Extract fingerprint from string_value if available
		if resp.Attrs != nil {
			if fpValue, ok := resp.Attrs.Fields["fingerprint"]; ok {
				if fpString, ok := fpValue.Kind.(*structpb.Value_StringValue); ok {
					// Convert string fingerprint to uint32
					fpUint, err := strconv.ParseUint(fpString.StringValue, 10, 32)
					if err == nil {
						fp = uint32(fpUint)
					}
				}
			}
		}

		client.lcache.Set(fp, resp.GetVStr(), 1) // Store only the string value
		client.lcache.Wait()                     // Ensure value is stored before proceeding
		onMessage(resp.GetVStr())
	}
}

func Fingerprint(c *wire.Command) uint32 {
	cmdStr := c.Cmd
	if len(c.Args) > 0 {
		cmdStr = fmt.Sprintf("%s %s", c.Cmd, strings.Join(c.Args, " "))
	}
	return farm.Fingerprint32([]byte(cmdStr))
}
