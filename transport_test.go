package cdphttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Set CDPHTTP_ENDPOINT (e.g. http://localhost:9224) to run against a browser.
func setup(t *testing.T) (*http.Client, string) {
	endpoint := os.Getenv("CDPHTTP_ENDPOINT")
	if endpoint == "" {
		t.Skip("CDPHTTP_ENDPOINT not set")
	}

	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		json.NewEncoder(w).Encode(map[string]any{"method": r.Method, "headers": r.Header, "body": b})
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusFound)
	})
	mux.HandleFunc("/setcookie", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "a", Value: "1", Path: "/"})
		http.SetCookie(w, &http.Cookie{Name: "b", Value: "2", Path: "/"})
	})
	mux.HandleFunc("/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		gz.Write([]byte("hello gzip"))
		gz.Close()
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(bigBody)))
		w.Write(bigBody)
	})
	mux.HandleFunc("/nocontent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("/cached", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		fmt.Fprint(w, hits.Add(1))
	})
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.URL)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.URL)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := NewClient(ctx, WithEndpoint(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Transport.(*Transport).Close() })
	c.Timeout = 20 * time.Second
	return c, srv.URL
}

var bigBody = bytes.Repeat([]byte("0123456789abcdef"), 1<<18)

type echo struct {
	Method  string
	Headers http.Header
	Body    []byte
}

func getEcho(t *testing.T, c *http.Client, req *http.Request) echo {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e echo
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestTransport(t *testing.T) {
	c, base := setup(t)

	t.Run("post", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", base+"/echo", strings.NewReader("\x00binary\xff"))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Custom", "yes")
		req.Header.Set("User-Agent", "custom-ua")
		e := getEcho(t, c, req)
		if e.Method != "PUT" || string(e.Body) != "\x00binary\xff" {
			t.Errorf("method=%q body=%q", e.Method, e.Body)
		}
		for k, want := range map[string]string{
			"Content-Type":   "application/octet-stream",
			"X-Custom":       "yes",
			"User-Agent":     "custom-ua",
			"Sec-Fetch-Site": "same-origin",
		} {
			if got := e.Headers.Get(k); got != want {
				t.Errorf("%s = %q, want %q", k, got, want)
			}
		}
		if e.Headers.Get(markerHeader) != "" {
			t.Error("marker header leaked")
		}
	})

	t.Run("browser headers", func(t *testing.T) {
		req, _ := http.NewRequest("GET", base+"/echo", nil)
		e := getEcho(t, c, req)
		if !strings.Contains(e.Headers.Get("User-Agent"), "Chrome/") {
			t.Errorf("User-Agent = %q", e.Headers.Get("User-Agent"))
		}
		if e.Headers.Get("Sec-Ch-Ua") == "" {
			t.Error("no Sec-Ch-Ua")
		}
	})

	t.Run("redirect", func(t *testing.T) {
		resp, err := c.Get(base + "/redirect")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.Request.URL.Path != "/echo" || resp.StatusCode != 200 {
			t.Errorf("final %s %d", resp.Request.URL, resp.StatusCode)
		}

		nc := *c
		nc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err = nc.Get(base + "/redirect")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/echo" {
			t.Errorf("got %d Location=%q", resp.StatusCode, resp.Header.Get("Location"))
		}
	})

	t.Run("cookies", func(t *testing.T) {
		resp, err := c.Get(base + "/setcookie")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		req, _ := http.NewRequest("GET", base+"/echo", nil)
		e := getEcho(t, c, req)
		if got := e.Headers.Get("Cookie"); !strings.Contains(got, "a=1") || !strings.Contains(got, "b=2") {
			t.Errorf("Cookie = %q", got)
		}
	})

	t.Run("gzip", func(t *testing.T) {
		resp, err := c.Get(base + "/gzip")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != "hello gzip" || !resp.Uncompressed || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("body=%q uncompressed=%v header=%v", b, resp.Uncompressed, resp.Header)
		}
	})

	t.Run("big", func(t *testing.T) {
		resp, err := c.Get(base + "/big")
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || !bytes.Equal(b, bigBody) || resp.ContentLength != int64(len(bigBody)) {
			t.Errorf("err=%v len=%d ContentLength=%d", err, len(b), resp.ContentLength)
		}
	})

	t.Run("head", func(t *testing.T) {
		resp, err := c.Head(base + "/big")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || len(b) != 0 {
			t.Errorf("status=%d len=%d", resp.StatusCode, len(b))
		}
	})

	t.Run("no content", func(t *testing.T) {
		resp, err := c.Get(base + "/nocontent")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("status=%d", resp.StatusCode)
		}
	})

	t.Run("http cache", func(t *testing.T) {
		var bodies []string
		for range 2 {
			resp, err := c.Get(base + "/cached")
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			bodies = append(bodies, string(b))
		}
		t.Logf("cached bodies: %q", bodies)
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", base+"/slow", nil)
		_, err := c.Do(req)
		if err == nil {
			t.Fatal("no error")
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Go(func() {
				req, _ := http.NewRequest("POST", base+"/echo", strings.NewReader(fmt.Sprint(i)))
				if e := getEcho(t, c, req); string(e.Body) != fmt.Sprint(i) {
					t.Errorf("request %d got body %q", i, e.Body)
				}
			})
		}
		wg.Wait()
	})

	t.Run("close idle", func(t *testing.T) {
		tr := c.Transport.(*Transport)
		openTabs := func() int {
			var r struct {
				TargetInfos []struct{ URL string }
			}
			if err := tr.conn.Call(context.Background(), "", "Target.getTargets", nil, &r); err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, ti := range r.TargetInfos {
				if ti.URL == base+"/" {
					n++
				}
			}
			return n
		}

		resp, err := c.Get(base + "/big")
		if err != nil {
			t.Fatal(err)
		}
		c.CloseIdleConnections() // body still open: tab must stay
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || len(b) != len(bigBody) {
			t.Fatalf("read after CloseIdleConnections: err=%v len=%d", err, len(b))
		}

		c.CloseIdleConnections()
		// Target.closeTarget returns before the tab is gone.
		n := openTabs()
		for i := 0; n != 0 && i < 20; i++ {
			time.Sleep(50 * time.Millisecond)
			n = openTabs()
		}
		if n != 0 {
			t.Errorf("%d tabs open after CloseIdleConnections", n)
		}
		req, _ := http.NewRequest("GET", base+"/echo", nil)
		getEcho(t, c, req) // reopens
		if n := openTabs(); n != 1 {
			t.Errorf("%d tabs open after reopen", n)
		}
	})

	t.Run("network error", func(t *testing.T) {
		_, err := c.Get("http://127.0.0.1:1/x")
		if err == nil {
			t.Fatal("no error")
		}
		t.Log(err)
	})
}

func TestDialTimeout(t *testing.T) {
	// Accepts connections but never answers /json/version.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()

	start := time.Now()
	_, err = NewClient(context.Background(), WithEndpoint("http://"+ln.Addr().String()))
	if err == nil {
		t.Fatal("no error")
	}
	if d := time.Since(start); d < DialTimeout || d > DialTimeout+time.Second {
		t.Errorf("failed after %v, want ~%v: %v", d, DialTimeout, err)
	}
}
