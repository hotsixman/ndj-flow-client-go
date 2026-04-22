package ndj_flow_client

import (
	"fmt"
	"net"
)

type TCPClient struct {
	*Client
	Host string
	Port int
}

func NewTCPClient(option TCPClientOption) *TCPClient {
	return &TCPClient{
		Client: NewClient(option.ClientOption),
		Host:   option.Host,
		Port:   option.Port,
	}
}

func (c *TCPClient) Connect() error {
	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	c.SetConn(conn)
	c.Start()
	return nil
}
