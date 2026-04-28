package ndj_flow_client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Client struct {
	Name string
	Key  string

	conn   net.Conn
	mu     sync.RWMutex
	closed bool

	responseResolverMap map[string]chan Message
	resolverMu          sync.Mutex

	OnConnect func()
	OnReceive func(message Message)
	OnClose   func(hadError bool)
	OnError   func(err error)

	sendChan chan sendTask
}

type sendTask struct {
	header SendHeader
	body   <-chan any
	err    chan error
}

func NewClient(option ClientOption) *Client {
	return &Client{
		Name:                option.Name,
		Key:                 option.Key,
		responseResolverMap: make(map[string]chan Message),
		OnConnect:           option.OnConnect,
		OnReceive:           option.OnReceive,
		OnClose:             option.OnClose,
		OnError:             option.OnError,
		sendChan:            make(chan sendTask, 100),
	}
}

func (c *Client) SetConn(conn net.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

func (c *Client) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.closed
}

func (c *Client) Send(header SendHeader, datas []any) (<-chan Message, error) {
	body := make(chan any, len(datas))
	for _, d := range datas {
		body <- d
	}
	close(body)
	return c.SendStream(header, body)
}

func (c *Client) SendStream(header SendHeader, body <-chan any) (<-chan Message, error) {
	resChan := make(chan Message, 1)

	c.resolverMu.Lock()
	c.responseResolverMap[header.ID] = resChan
	c.resolverMu.Unlock()

	errChan := make(chan error, 1)
	c.sendChan <- sendTask{
		header: header,
		body:   body,
		err:    errChan,
	}

	// In JS, errors in send are handled by reject callback.
	// Here we could return the errChan or handle it.
	return resChan, nil
}

func (c *Client) Start() {
	go c.writeLoop()
	go c.readLoop()

	// Initial Handshake
	handshake := Handshake{Name: c.Name, Key: c.Key}
	data, _ := json.Marshal(handshake)
	c.write(string(data) + "\n")

	if c.OnConnect != nil {
		c.OnConnect()
	}
}

func (c *Client) write(data string) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("NO_CONNECTION")
	}
	_, err := conn.Write([]byte(data))
	return err
}

func (c *Client) writeLoop() {
	for task := range c.sendChan {
		// Header
		hData, err := json.Marshal(task.header)
		if err != nil {
			task.err <- err
			continue
		}
		if err := c.write(string(hData) + "\n"); err != nil {
			task.err <- err
			continue
		}

		// Body
		for val := range task.body {
			vData, err := json.Marshal(val)
			if err != nil {
				// How to handle error mid-stream?
				// JS version doesn't seem to abort mid-stream easily in the code provided.
				continue
			}
			if err := c.write(string(vData) + "\n"); err != nil {
				task.err <- err
				break
			}
		}

		// Terminator
		if err := c.write("\x00\n"); err != nil {
			task.err <- err
		}
	}
}

func (c *Client) readLoop() {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return
	}

	reader := bufio.NewReader(conn)
	var currentMsg *Message
	var currentBody chan any

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			c.mu.Lock()
			if !c.closed {
				if c.OnClose != nil {
					c.OnClose(true)
				}
				if c.OnError != nil && err != net.ErrClosed {
					c.OnError(err)
				}
			}
			c.mu.Unlock()
			break
		}

		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}

		if line == "\x00" || line == "1" {
			if currentBody != nil {
				close(currentBody)
			}
			if line == "1" && currentMsg != nil {
				currentMsg.Error = true
			}
			currentMsg = nil
			currentBody = nil
		} else {
			var raw json.RawMessage
			if err := json.Unmarshal([]byte(line), &raw); err != nil {
				if c.OnError != nil {
					c.OnError(err)
				}
				continue
			}

			if currentBody != nil {
				var val any
				json.Unmarshal(raw, &val)
				currentBody <- val
			} else {
				// Try parsing as header
				var header ReceiveHeader
				if err := json.Unmarshal(raw, &header); err == nil && header.ID != "" {
					body := make(chan any, 100)
					msg := Message{
						Header: header,
						Body:   body,
						Error:  false,
					}
					currentMsg = &msg
					currentBody = body

					c.resolverMu.Lock()
					resChan, ok := c.responseResolverMap[header.ID]
					if ok {
						delete(c.responseResolverMap, header.ID)
					}
					c.resolverMu.Unlock()

					if ok {
						go func() { resChan <- msg }()
					} else if c.OnReceive != nil {
						go c.OnReceive(msg)
					}
				}
			}
		}
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
