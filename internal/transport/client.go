package transport

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aleda145/kavla-cli/internal/runner"
	"github.com/google/uuid"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type Client struct {
	workerURL string
	roomID    string
	token     string
	conn      *websocket.Conn
	ctx       context.Context

	pendingRequests map[string]chan map[string]interface{}
	mu              sync.Mutex
}

func NewClient(workerURL, roomID, token string) *Client {
	return &Client{
		workerURL:       workerURL,
		roomID:          roomID,
		token:           token,
		pendingRequests: make(map[string]chan map[string]interface{}),
	}
}

func (c *Client) websocketDialConfig() (string, *websocket.DialOptions, error) {
	u, err := url.Parse(c.workerURL)
	if err != nil {
		return "", nil, err
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	u.Path = fmt.Sprintf("/api/data-socket/%s", c.roomID)
	q := u.Query()
	q.Set("clientType", "cli")
	u.RawQuery = q.Encode()

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+c.token)

	return u.String(), &websocket.DialOptions{HTTPHeader: headers}, nil
}

func (c *Client) Connect(ctx context.Context) error {
	wsURL, opts, err := c.websocketDialConfig()
	if err != nil {
		return err
	}

	conn, _, err := websocket.Dial(ctx, wsURL, opts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.ctx = ctx
	c.mu.Unlock()
	return nil
}

func (c *Client) Read(ctx context.Context) (map[string]interface{}, error) {
	var msg map[string]interface{}
	if err := wsjson.Read(ctx, c.conn, &msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Client) ResolvePending(msg map[string]interface{}) bool {
	requestID, ok := msg["requestId"].(string)
	if !ok || requestID == "" {
		return false
	}

	c.mu.Lock()
	ch, exists := c.pendingRequests[requestID]
	if exists {
		delete(c.pendingRequests, requestID)
	}
	c.mu.Unlock()

	if exists {
		ch <- msg
		return true
	}

	return false
}

func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.ctx = nil
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close(websocket.StatusNormalClosure, "Bye")
}

func (c *Client) SendRequest(typ string, payload interface{}) (map[string]interface{}, error) {
	requestID := uuid.New().String()
	ch := make(chan map[string]interface{}, 1)

	c.mu.Lock()
	c.pendingRequests[requestID] = ch
	c.mu.Unlock()

	msg := map[string]interface{}{
		"type":      typ,
		"payload":   payload,
		"requestId": requestID,
	}

	if err := c.SendJSON(msg); err != nil {
		c.mu.Lock()
		delete(c.pendingRequests, requestID)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case resp := <-ch:
		if errStr, ok := resp["error"].(string); ok && errStr != "" {
			return nil, fmt.Errorf("rpc error: %s", errStr)
		}
		return resp, nil
	case <-time.After(10 * time.Second):
		c.mu.Lock()
		delete(c.pendingRequests, requestID)
		c.mu.Unlock()
		return nil, fmt.Errorf("timeout")
	}
}

func (c *Client) GetR2PresignedURL(shapeID string) (string, string, error) {
	resp, err := c.SendRequest("request_result_upload_urls", map[string]string{
		"shapeId": shapeID,
	})
	if err != nil {
		return "", "", err
	}

	payload, ok := resp["payload"].(map[string]interface{})
	if !ok {
		return "", "", fmt.Errorf("invalid payload response")
	}

	fullURL, _ := payload["fullUrl"].(string)
	previewURL, _ := payload["previewUrl"].(string)
	if fullURL == "" {
		return "", "", fmt.Errorf("missing fullUrl in response")
	}

	return fullURL, previewURL, nil
}

func (c *Client) GetPresignedReadURL(r2ObjectKey string) (string, error) {
	resp, err := c.SendRequest("get_presigned_read_url", map[string]string{
		"r2ObjectKey": r2ObjectKey,
	})
	if err != nil {
		return "", err
	}

	payload, ok := resp["payload"].(string)
	if !ok || payload == "" {
		return "", fmt.Errorf("missing presigned read URL in response")
	}

	return payload, nil
}

func (c *Client) SendJSON(msg map[string]interface{}) error {
	c.mu.Lock()
	conn := c.conn
	ctx := c.ctx
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("websocket not connected")
	}
	if ctx == nil {
		return fmt.Errorf("websocket context not initialized")
	}

	return wsjson.Write(ctx, conn, msg)
}

func (c *Client) SendResultData(shapeID string, format runner.ResultFormat, data []byte) error {
	encoded := base64.StdEncoding.EncodeToString(data)
	return c.SendJSON(map[string]interface{}{
		"type":    "relay_data",
		"payload": encoded,
		"format":  string(format),
		"shapeId": shapeID,
	})
}

func (c *Client) SendOutput(line string) error {
	return c.SendJSON(map[string]interface{}{
		"type": "cli_output",
		"payload": map[string]interface{}{
			"line":      line,
			"timestamp": time.Now().UnixMilli(),
		},
	})
}
