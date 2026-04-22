package ndj_flow_client

import (
	"net"
)

type UDSClient struct {
	*Client
	Path string
}

func NewUDSClient(option UDSClientOption) *UDSClient {
	return &UDSClient{
		Client: NewClient(option.ClientOption),
		Path:   option.Path,
	}
}

func (c *UDSClient) Connect() error {
	conn, err := net.Dial("unix", c.Path)
	if err != nil {
		return err
	}
	c.SetConn(conn)
	c.Start()
	return nil
}
