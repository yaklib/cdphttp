// Package cdphttp sends HTTP requests through a running Chrome so they
// carry the browser's fingerprint, headers and cookies.
package cdphttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"codeberg.org/yaklib/cdphttp/cdp"
)

// Transport is an http.RoundTripper backed by a browser. Cookies are the
// browser's (don't set http.Client.Jar) and responses lack Set-Cookie.
type Transport struct {
	conn *cdp.Conn

	mu   sync.Mutex
	tabs map[string]*tab // by origin
}

// DefaultEndpoint is used when no endpoint is given and $CDPHTTP_ENDPOINT is
// unset.
const DefaultEndpoint = "http://localhost:9222"

// Option configures Dial and NewClient.
type Option func(*options)

type options struct {
	endpoint string
}

// WithEndpoint sets the DevTools endpoint (http://host:port or ws:// URL).
// Default: $CDPHTTP_ENDPOINT, then DefaultEndpoint.
func WithEndpoint(endpoint string) Option {
	return func(o *options) { o.endpoint = endpoint }
}

// DialTimeout bounds connecting to the browser in Dial and NewClient.
const DialTimeout = 3 * time.Second

// Dial connects to a browser over the DevTools protocol.
func Dial(ctx context.Context, opts ...Option) (*Transport, error) {
	ctx, cancel := context.WithTimeout(ctx, DialTimeout)
	defer cancel()

	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.endpoint == "" {
		o.endpoint = os.Getenv("CDPHTTP_ENDPOINT")
	}
	if o.endpoint == "" {
		o.endpoint = DefaultEndpoint
	}
	conn, err := cdp.Dial(ctx, o.endpoint)
	if err != nil {
		return nil, err
	}
	t := &Transport{conn: conn, tabs: make(map[string]*tab)}
	conn.Subscribe("", t.onBrowserEvent)
	return t, nil
}

// NewClient returns an http.Client backed by a browser.
func NewClient(ctx context.Context, opts ...Option) (*http.Client, error) {
	t, err := Dial(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: t}, nil
}

// Close closes t's tabs and the DevTools connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	tabs := make([]*tab, 0, len(t.tabs))
	for _, tb := range t.tabs {
		tabs = append(tabs, tb)
	}
	t.tabs = make(map[string]*tab)
	t.mu.Unlock()

	closeTabs(tabs)
	return t.conn.Close()
}

// CloseIdleConnections closes tabs not in use; later requests reopen them.
func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	var idle []*tab
	for origin, tb := range t.tabs {
		if tb.active.Load() == 0 {
			delete(t.tabs, origin)
			idle = append(idle, tb)
		}
	}
	t.mu.Unlock()

	closeTabs(idle)
}

func closeTabs(tabs []*tab) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tb := range tabs {
		<-tb.ready
		if tb.err == nil {
			tb.close(ctx)
		}
	}
}

// The user may close one of our tabs; forget it so the next request reopens it.
func (t *Transport) onBrowserEvent(ev cdp.Event) {
	if ev.Method != "Target.detachedFromTarget" {
		return
	}
	var p struct {
		SessionID string `json:"sessionId"`
	}
	json.Unmarshal(ev.Params, &p)

	t.mu.Lock()
	defer t.mu.Unlock()
	for origin, tb := range t.tabs {
		select {
		case <-tb.ready:
		default:
			continue
		}
		if tb.err == nil && tb.s.ID == p.SessionID {
			delete(t.tabs, origin)
			go tb.failAll(errTabClosed)
		}
	}
}

func (t *Transport) tab(ctx context.Context, origin string) (*tab, error) {
	t.mu.Lock()
	tb := t.tabs[origin]
	if tb == nil {
		tb = newTab(t.conn, origin)
		t.tabs[origin] = tb
		go func() {
			tb.err = tb.open()
			if tb.err != nil {
				t.mu.Lock()
				if t.tabs[origin] == tb {
					delete(t.tabs, origin)
				}
				t.mu.Unlock()
			}
			close(tb.ready)
		}()
	}
	// Counted under t.mu so CloseIdleConnections can't close it from under us.
	tb.active.Add(1)
	t.mu.Unlock()

	select {
	case <-tb.ready:
	case <-ctx.Done():
		tb.active.Add(-1)
		return nil, ctx.Err()
	}
	if tb.err != nil {
		tb.active.Add(-1)
		return nil, tb.err
	}
	return tb, nil
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer req.Body.Close()
	}
	u := req.URL
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("cdphttp: unsupported scheme %q", u.Scheme)
	}
	ctx := req.Context()

	tb, err := t.tab(ctx, u.Scheme+"://"+u.Host)
	if err != nil {
		return nil, err
	}
	// Held until the response body is closed.
	release := sync.OnceFunc(func() { tb.active.Add(-1) })

	var reqBody []byte
	if req.Body != nil {
		if reqBody, err = io.ReadAll(req.Body); err != nil {
			release()
			return nil, err
		}
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	var mb [16]byte
	rand.Read(mb[:])
	marker := hex.EncodeToString(mb[:])

	p := &pending{req: req, body: reqBody, release: release, done: make(chan result, 1)}
	tb.mu.Lock()
	tb.byMarker[marker] = p
	tb.mu.Unlock()

	urlJS, _ := json.Marshal(u.String())
	optsJS, _ := json.Marshal(map[string]any{
		"method":      method,
		"headers":     map[string]string{markerHeader: marker},
		"credentials": "include",
		"redirect":    "manual",
	})
	// The promise settles only after we release the paused request, so a
	// rejection after the response was delivered is ignored by deliver.
	expr := fmt.Sprintf(`fetch(%s, %s).then(() => "", e => String(e))`, urlJS, optsJS)
	go func() {
		var r struct {
			Result struct {
				Value string `json:"value"`
			} `json:"result"`
		}
		err := tb.s.Call(context.Background(), "Runtime.evaluate",
			map[string]any{"expression": expr, "awaitPromise": true, "returnByValue": true}, &r)
		if err == nil && r.Result.Value != "" {
			err = errors.New(r.Result.Value)
		}
		if err != nil {
			p.deliver(nil, fmt.Errorf("cdphttp: %s %s: %w", method, u, err))
		}
	}()

	select {
	case r := <-p.done:
		if r.err != nil {
			release()
			return nil, r.err
		}
		if _, ok := r.resp.Body.(*body); !ok {
			release()
		}
		return r.resp, nil
	case <-ctx.Done():
		tb.forget(p)
		select {
		case r := <-p.done:
			if r.resp != nil {
				r.resp.Body.Close()
			}
		default:
		}
		release()
		return nil, ctx.Err()
	}
}
