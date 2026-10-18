package cdphttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/yaklib/cdphttp/cdp"
)

const markerHeader = "x-cdphttp-id"

// Served as the tab's document so the origin is set without loading the site.
// The icon link stops the browser from requesting /favicon.ico.
const stubHTML = `<!doctype html><html><head><link rel="icon" href="data:,"></head></html>`

var errTabClosed = errors.New("cdphttp: tab closed")

type header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// orderedHeaders decodes a JSON object keeping key order; header order is
// part of the browser fingerprint.
type orderedHeaders []header

func (h *orderedHeaders) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if _, err := dec.Token(); err != nil {
		return err
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return err
		}
		var v string
		if err := dec.Decode(&v); err != nil {
			return err
		}
		*h = append(*h, header{k.(string), v})
	}
	return nil
}

type requestPaused struct {
	RequestID string `json:"requestId"`
	Request   struct {
		URL     string         `json:"url"`
		Method  string         `json:"method"`
		Headers orderedHeaders `json:"headers"`
	} `json:"request"`
	ResourceType        string   `json:"resourceType"`
	ResponseErrorReason string   `json:"responseErrorReason"`
	ResponseStatusCode  int      `json:"responseStatusCode"`
	ResponseHeaders     []header `json:"responseHeaders"`
}

type result struct {
	resp *http.Response
	err  error
}

type pending struct {
	req     *http.Request
	body    []byte
	release func() // ends the request's hold on the tab
	once    sync.Once
	done    chan result
}

func (p *pending) deliver(resp *http.Response, err error) bool {
	ok := false
	p.once.Do(func() {
		p.done <- result{resp, err}
		ok = true
	})
	return ok
}

// tab is a background target parked on a stub document of one origin.
type tab struct {
	conn     *cdp.Conn
	origin   string
	targetID string
	s        *cdp.Session

	ready chan struct{}
	err   error

	active atomic.Int64 // requests and open response bodies

	mu       sync.Mutex
	byMarker map[string]*pending
	byReq    map[string]*pending
}

func newTab(conn *cdp.Conn, origin string) *tab {
	return &tab{
		conn:     conn,
		origin:   origin,
		ready:    make(chan struct{}),
		byMarker: make(map[string]*pending),
		byReq:    make(map[string]*pending),
	}
}

func (tb *tab) open() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer func() {
		if err != nil {
			err = fmt.Errorf("cdphttp: open tab for %s: %w", tb.origin, err)
		}
	}()

	var tgt struct {
		TargetID string `json:"targetId"`
	}
	if err := tb.conn.Call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank", "background": true, "hidden": true}, &tgt); err != nil {
		return err
	}
	tb.targetID = tgt.TargetID
	var att struct {
		SessionID string `json:"sessionId"`
	}
	if err := tb.conn.Call(ctx, "", "Target.attachToTarget", map[string]any{"targetId": tb.targetID, "flatten": true}, &att); err != nil {
		tb.close(ctx)
		return err
	}
	tb.s = &cdp.Session{Conn: tb.conn, ID: att.SessionID}

	loaded := make(chan struct{})
	var once sync.Once
	tb.s.Subscribe(func(ev cdp.Event) {
		switch ev.Method {
		case "Page.loadEventFired":
			once.Do(func() { close(loaded) })
		case "Fetch.requestPaused":
			go tb.onPaused(ev.Params)
		}
	})

	var nav struct {
		ErrorText string `json:"errorText"`
	}
	steps := []struct {
		method string
		params any
		result any
	}{
		{"Page.enable", nil, nil},
		{"Network.setBypassServiceWorker", map[string]any{"bypass": true}, nil},
		{"Fetch.enable", map[string]any{"patterns": []map[string]string{
			{"urlPattern": "*", "requestStage": "Request"},
			{"urlPattern": "*", "requestStage": "Response"},
		}}, nil},
		{"Page.navigate", map[string]any{"url": tb.origin + "/"}, &nav},
	}
	for _, st := range steps {
		if err := tb.s.Call(ctx, st.method, st.params, st.result); err != nil {
			tb.close(ctx)
			return fmt.Errorf("%s: %w", st.method, err)
		}
	}
	if nav.ErrorText != "" {
		tb.close(ctx)
		return errors.New(nav.ErrorText)
	}
	select {
	case <-loaded:
		return nil
	case <-ctx.Done():
		tb.close(context.Background())
		return ctx.Err()
	}
}

func (tb *tab) close(ctx context.Context) {
	tb.conn.Call(ctx, "", "Target.closeTarget", map[string]any{"targetId": tb.targetID}, nil)
}

// forget drops p so its late events get failed instead of delivered.
func (tb *tab) forget(p *pending) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	for k, v := range tb.byMarker {
		if v == p {
			delete(tb.byMarker, k)
		}
	}
	for k, v := range tb.byReq {
		if v == p {
			delete(tb.byReq, k)
		}
	}
}

func (tb *tab) failAll(err error) {
	tb.mu.Lock()
	ps := make([]*pending, 0, len(tb.byMarker)+len(tb.byReq))
	for _, p := range tb.byMarker {
		ps = append(ps, p)
	}
	for _, p := range tb.byReq {
		ps = append(ps, p)
	}
	tb.byMarker = make(map[string]*pending)
	tb.byReq = make(map[string]*pending)
	tb.mu.Unlock()
	for _, p := range ps {
		p.deliver(nil, err)
	}
}

func (tb *tab) onPaused(raw json.RawMessage) {
	var ev requestPaused
	if err := json.Unmarshal(raw, &ev); err != nil {
		return
	}
	if ev.ResponseStatusCode != 0 || ev.ResponseErrorReason != "" {
		tb.onResponse(&ev)
	} else {
		tb.onRequest(&ev)
	}
}

func (tb *tab) call(method string, params map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return tb.s.Call(ctx, method, params, nil)
}

func (tb *tab) onRequest(ev *requestPaused) {
	var marker string
	for _, h := range ev.Request.Headers {
		if strings.EqualFold(h.Name, markerHeader) {
			marker = h.Value
		}
	}
	tb.mu.Lock()
	p := tb.byMarker[marker]
	if p != nil {
		delete(tb.byMarker, marker)
		tb.byReq[ev.RequestID] = p
	}
	tb.mu.Unlock()

	switch {
	case p != nil:
		params := map[string]any{
			"requestId": ev.RequestID,
			"headers":   mergeHeaders(ev.Request.Headers, p.req.Header),
		}
		if len(p.body) > 0 {
			params["postData"] = base64.StdEncoding.EncodeToString(p.body)
		}
		if err := tb.call("Fetch.continueRequest", params); err != nil {
			p.deliver(nil, err)
		}
	case ev.ResourceType == "Document":
		tb.call("Fetch.fulfillRequest", map[string]any{
			"requestId":       ev.RequestID,
			"responseCode":    200,
			"responseHeaders": []header{{"Content-Type", "text/html"}},
			"body":            base64.StdEncoding.EncodeToString([]byte(stubHTML)),
		})
	default:
		// Nothing else in our tab may reach the network.
		tb.call("Fetch.failRequest", map[string]any{"requestId": ev.RequestID, "errorReason": "BlockedByClient"})
	}
}

func (tb *tab) onResponse(ev *requestPaused) {
	tb.mu.Lock()
	p := tb.byReq[ev.RequestID]
	delete(tb.byReq, ev.RequestID)
	tb.mu.Unlock()

	if p == nil {
		tb.call("Fetch.failRequest", map[string]any{"requestId": ev.RequestID, "errorReason": "Aborted"})
		return
	}
	if ev.ResponseErrorReason != "" {
		p.deliver(nil, fmt.Errorf("cdphttp: %s %s: %s", p.req.Method, p.req.URL, ev.ResponseErrorReason))
		tb.call("Fetch.continueRequest", map[string]any{"requestId": ev.RequestID})
		return
	}

	resp := &http.Response{
		Status:     strconv.Itoa(ev.ResponseStatusCode) + " " + http.StatusText(ev.ResponseStatusCode),
		StatusCode: ev.ResponseStatusCode,
		Header:     make(http.Header),
		Request:    p.req,
	}
	for _, h := range ev.ResponseHeaders {
		resp.Header.Add(h.Name, h.Value)
	}
	// The browser hands us the decoded body.
	resp.ContentLength = -1
	if resp.Header.Get("Content-Encoding") != "" {
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.Uncompressed = true
	} else if n, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64); err == nil {
		resp.ContentLength = n
	}

	var st struct {
		Stream string `json:"stream"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := tb.s.Call(ctx, "Fetch.takeResponseBodyAsStream", map[string]any{"requestId": ev.RequestID}, &st)
	cancel()
	if err != nil {
		// Bodiless responses (e.g. some redirects) have no stream.
		resp.Body = http.NoBody
		resp.ContentLength = 0
		p.deliver(resp, nil)
		tb.call("Fetch.continueRequest", map[string]any{"requestId": ev.RequestID})
		return
	}
	b := &body{tb: tb, ctx: p.req.Context(), requestID: ev.RequestID, handle: st.Stream, release: p.release}
	if p.req.Method == http.MethodHead {
		b.Close()
		resp.Body = http.NoBody
	} else {
		resp.Body = b
	}
	if !p.deliver(resp, nil) {
		b.Close()
	}
}

// mergeHeaders overlays h onto the browser's request headers, keeping the
// browser's order and dropping the marker.
func mergeHeaders(browser orderedHeaders, h http.Header) []header {
	out := make([]header, 0, len(browser)+len(h))
	for _, bh := range browser {
		if strings.EqualFold(bh.Name, markerHeader) || h.Values(bh.Name) != nil {
			continue
		}
		out = append(out, bh)
	}
	for k, vs := range h {
		switch http.CanonicalHeaderKey(k) {
		case "Host", "Content-Length", "Connection", "Transfer-Encoding":
			continue
		}
		sep := ", "
		if http.CanonicalHeaderKey(k) == "Cookie" {
			sep = "; "
		}
		out = append(out, header{k, strings.Join(vs, sep)})
	}
	return out
}

// body reads a response stream with IO.read and releases the paused request on Close.
type body struct {
	tb        *tab
	ctx       context.Context
	requestID string
	handle    string
	release   func()

	buf  []byte
	eof  bool
	once sync.Once
}

func (b *body) Read(p []byte) (int, error) {
	for len(b.buf) == 0 {
		if b.eof {
			return 0, io.EOF
		}
		var r struct {
			Base64Encoded bool   `json:"base64Encoded"`
			Data          string `json:"data"`
			EOF           bool   `json:"eof"`
		}
		if err := b.tb.s.Call(b.ctx, "IO.read", map[string]any{"handle": b.handle, "size": 1 << 20}, &r); err != nil {
			return 0, err
		}
		if r.Base64Encoded {
			d, err := base64.StdEncoding.DecodeString(r.Data)
			if err != nil {
				return 0, err
			}
			b.buf = d
		} else {
			b.buf = []byte(r.Data)
		}
		b.eof = r.EOF
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

func (b *body) Close() error {
	b.once.Do(func() {
		b.tb.call("IO.close", map[string]any{"handle": b.handle})
		b.tb.call("Fetch.failRequest", map[string]any{"requestId": b.requestID, "errorReason": "Aborted"})
		b.release()
	})
	return nil
}
