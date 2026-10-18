// Package cdp is a minimal Chrome DevTools Protocol client.
//
// One Conn talks to the browser endpoint; targets are reached through
// flattened sessions (Target.attachToTarget with flatten=true).
package cdp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

// Error is a protocol error returned by the browser.
type Error struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e.Data != "" {
		return fmt.Sprintf("cdp: %s (%d): %s", e.Message, e.Code, e.Data)
	}
	return fmt.Sprintf("cdp: %s (%d)", e.Message, e.Code)
}

// Event is a protocol event. SessionID is empty for browser-level events.
type Event struct {
	SessionID string
	Method    string
	Params    json.RawMessage
}

type request struct {
	ID        int64  `json:"id"`
	SessionID string `json:"sessionId,omitempty"`
	Method    string `json:"method"`
	Params    any    `json:"params,omitempty"`
}

type message struct {
	ID        int64           `json:"id"`
	SessionID string          `json:"sessionId"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
	Result    json.RawMessage `json:"result"`
	Error     *Error          `json:"error"`
}

// ErrClosed is returned by calls on a closed connection.
var ErrClosed = errors.New("cdp: connection closed")

// Conn is a connection to the browser-level DevTools websocket.
//
// The websocket is only ever used with context.Background(): coder/websocket
// closes the whole connection when a Read or Write context is cancelled.
type Conn struct {
	ws *websocket.Conn

	mu       sync.Mutex
	nextID   int64
	pending  map[int64]chan *message
	nextSub  int
	handlers map[string]map[int]func(Event)
	err      error
	done     chan struct{}
}

// Dial connects to endpoint, which is either a ws:// debugger URL or an
// http://host:port address whose /json/version is used to find one.
func Dial(ctx context.Context, endpoint string) (*Conn, error) {
	wsURL := endpoint
	if strings.HasPrefix(endpoint, "http://") {
		var err error
		if wsURL, err = browserWSURL(ctx, endpoint); err != nil {
			return nil, err
		}
	}
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(-1)
	c := &Conn{
		ws:       ws,
		pending:  make(map[int64]chan *message),
		handlers: make(map[string]map[int]func(Event)),
		done:     make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func browserWSURL(ctx context.Context, endpoint string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimSuffix(endpoint, "/")+"/json/version", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var v struct {
		WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", fmt.Errorf("cdp: /json/version: %w", err)
	}
	if v.WebSocketDebuggerURL == "" {
		return "", errors.New("cdp: /json/version: no webSocketDebuggerUrl")
	}
	return v.WebSocketDebuggerURL, nil
}

func (c *Conn) readLoop() {
	var err error
	for {
		var b []byte
		if _, b, err = c.ws.Read(context.Background()); err != nil {
			break
		}
		var m message
		if err = json.Unmarshal(b, &m); err != nil {
			break
		}
		if m.ID != 0 {
			c.mu.Lock()
			ch := c.pending[m.ID]
			delete(c.pending, m.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- &m
			}
			continue
		}
		c.mu.Lock()
		hs := make([]func(Event), 0, len(c.handlers[m.SessionID]))
		for _, h := range c.handlers[m.SessionID] {
			hs = append(hs, h)
		}
		c.mu.Unlock()
		ev := Event{SessionID: m.SessionID, Method: m.Method, Params: m.Params}
		for _, h := range hs {
			h(ev)
		}
	}
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	close(c.done)
	c.ws.CloseNow()
}

// Call sends method with params on sessionID ("" for the browser) and
// decodes the result into result, if non-nil.
func (c *Conn) Call(ctx context.Context, sessionID, method string, params, result any) error {
	ch := make(chan *message, 1)
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.pending[id] = ch
	c.mu.Unlock()
	forget := func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}

	b, err := json.Marshal(request{ID: id, SessionID: sessionID, Method: method, Params: params})
	if err != nil {
		forget()
		return err
	}
	if err := c.ws.Write(context.Background(), websocket.MessageText, b); err != nil {
		forget()
		return err
	}

	select {
	case m := <-ch:
		if m.Error != nil {
			return m.Error
		}
		if result != nil {
			return json.Unmarshal(m.Result, result)
		}
		return nil
	case <-ctx.Done():
		forget()
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

// Subscribe registers fn for all events on sessionID. fn runs on the read
// goroutine and must not block or call Call synchronously.
func (c *Conn) Subscribe(sessionID string, fn func(Event)) (unsubscribe func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextSub++
	id := c.nextSub
	if c.handlers[sessionID] == nil {
		c.handlers[sessionID] = make(map[int]func(Event))
	}
	c.handlers[sessionID][id] = fn
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.handlers[sessionID], id)
		if len(c.handlers[sessionID]) == 0 {
			delete(c.handlers, sessionID)
		}
	}
}

// Done is closed when the connection is gone.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns the reason the connection ended, once Done is closed.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close closes the websocket. It does not close the browser.
func (c *Conn) Close() error {
	err := c.ws.Close(websocket.StatusNormalClosure, "")
	<-c.done
	return err
}

// Session is a flattened target session on a Conn.
type Session struct {
	Conn *Conn
	ID   string
}

func (s *Session) Call(ctx context.Context, method string, params, result any) error {
	return s.Conn.Call(ctx, s.ID, method, params, result)
}

func (s *Session) Subscribe(fn func(Event)) (unsubscribe func()) {
	return s.Conn.Subscribe(s.ID, fn)
}
