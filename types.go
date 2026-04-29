package ndj_flow_client

import (
	"encoding/json"
)

type ClientOption struct {
	Name      string
	Key       string
	OnConnect func()
	OnReceive func(message Message)
	OnClose   func(hadError bool)
	OnError   func(err error)
}

type TCPClientOption struct {
	ClientOption
	Host string
	Port int
}

type UDSClientOption struct {
	ClientOption
	Path string
}

type SendHeader struct {
	To       string         `json:"to"`
	ID       string         `json:"id"`
	Metadata map[string]any `json:"-"`
}

func (h SendHeader) MarshalJSON() ([]byte, error) {
	type Alias SendHeader
	b, err := json.Marshal(Alias(h))
	if err != nil {
		return nil, err
	}

	var m map[string]any
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	for k, v := range h.Metadata {
		m[k] = v
	}

	return json.Marshal(m)
}

type ReceiveHeader struct {
	From     string         `json:"from"`
	To       string         `json:"to"`
	ID       string         `json:"id"`
	Metadata map[string]any `json:"-"`
}

func (h *ReceiveHeader) UnmarshalJSON(data []byte) error {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if v, ok := m["from"].(string); ok {
		h.From = v
		delete(m, "from")
	}
	if v, ok := m["to"].(string); ok {
		h.To = v
		delete(m, "to")
	}
	if v, ok := m["id"].(string); ok {
		h.ID = v
		delete(m, "id")
	}

	h.Metadata = make(map[string]any)
	for k, v := range m {
		if s, ok := v.(string); ok {
			h.Metadata[k] = s
		}
	}

	return nil
}

type Message struct {
	Header ReceiveHeader
	Body   <-chan any
	Error  bool
}

type Handshake struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}
