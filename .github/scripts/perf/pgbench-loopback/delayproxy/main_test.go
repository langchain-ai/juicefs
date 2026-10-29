/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"
)

func TestSampleMatchesQuantiles(t *testing.T) {
	s := newSampler(42)
	l := latency{p50: 20 * time.Millisecond, p99: 100 * time.Millisecond}
	const n = 200000
	ds := make([]time.Duration, n)
	for i := range ds {
		ds[i] = s.sample(l)
	}
	slices.Sort(ds)
	within := func(name string, got, want time.Duration) {
		t.Helper()
		if r := float64(got) / float64(want); r < 0.95 || r > 1.05 {
			t.Errorf("%s = %s, want ~%s", name, got, want)
		}
	}
	within("p50", ds[n/2], l.p50)
	within("p99", ds[n*99/100], l.p99)
	if max := ds[n-1]; max > maxTailFactor*l.p99 {
		t.Errorf("max %s exceeds cap %s", max, maxTailFactor*l.p99)
	}
}

func TestSampleDegenerate(t *testing.T) {
	s := newSampler(1)
	if d := s.sample(latency{}); d != 0 {
		t.Errorf("zero latency sampled %s", d)
	}
	if d := s.sample(latency{p50: 5 * time.Millisecond}); d != 5*time.Millisecond {
		t.Errorf("p99 unset sampled %s, want p50", d)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		method, target, want string
	}{
		{"GET", "/bucket/chunks/0/0/1_0_4194304", "get"},
		{"GET", "/bucket/?list-type=2&prefix=chunks/", "other"},
		{"GET", "/bucket", "other"},
		{"PUT", "/bucket/chunks/0/0/1_0_4194304", "put"},
		{"POST", "/bucket/key?uploads", "put"},
		{"HEAD", "/bucket/key", "other"},
		{"DELETE", "/bucket/key", "other"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.target, nil)
		if got := classify(r); got != c.want {
			t.Errorf("%s %s = %s, want %s", c.method, c.target, got, c.want)
		}
	}
}

func TestHTTPProxyPassesHostAndBodies(t *testing.T) {
	var gotHost string
	var gotBody []byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte("response-body"))
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	prof := profile{put: latency{p50: 30 * time.Millisecond}}
	front := httptest.NewServer(newHTTPProxy(u, prof, 0, 1))
	defer front.Close()

	start := time.Now()
	req, _ := http.NewRequest(http.MethodPut, front.URL+"/bucket/key", bytes.NewReader([]byte("request-body")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)

	frontHost := front.Listener.Addr().String()
	if gotHost != frontHost {
		t.Errorf("upstream saw Host %q, want the proxy address %q", gotHost, frontHost)
	}
	if string(gotBody) != "request-body" || string(body) != "response-body" {
		t.Errorf("bodies not passed through: upstream got %q, client got %q", gotBody, body)
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("PUT took %s, want at least the 30ms injected delay", elapsed)
	}
}

func TestStreamBandwidthLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1<<20)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	front := httptest.NewServer(newHTTPProxy(u, profile{streamMBps: 10}, 0, 1))
	defer front.Close()

	start := time.Now()
	resp, err := http.Get(front.URL + "/bucket/key")
	if err != nil {
		t.Fatal(err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if n != int64(len(payload)) {
		t.Fatalf("read %d bytes, want %d", n, len(payload))
	}
	// 1 MiB at 10 MB/s is ~105ms.
	if elapsed < 90*time.Millisecond || elapsed > time.Second {
		t.Errorf("1 MiB at 10 MB/s took %s", elapsed)
	}
}

// At a high rate each read's sleep is shorter than the timer's real resolution;
// the limiter must make up the oversleep rather than settle far below target.
func TestStreamBandwidthHighRate(t *testing.T) {
	const size = 32 << 20
	body := &throttledBody{
		ReadCloser: io.NopCloser(io.LimitReader(zeroReader{}, size)),
		ctx:        context.Background(),
		stream:     newLimiter(400),
	}
	start := time.Now()
	n, err := io.CopyBuffer(io.Discard, body, make([]byte, 32<<10))
	if err != nil || n != size {
		t.Fatalf("copied %d bytes, err %v", n, err)
	}
	// 32 MiB at 400 MB/s is ~84ms.
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("32 MiB at 400 MB/s took %s, want ~84ms", elapsed)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestLimiterSharesBandwidth(t *testing.T) {
	l := newLimiter(1) // 1 MB/s
	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := l.wait(context.Background(), 25_000); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Errorf("100 KB at 1 MB/s took %s, want ~100ms", elapsed)
	}
	if newLimiter(0) != nil {
		t.Error("zero bandwidth should mean unlimited")
	}
}

func TestTCPProxyDelaysRoundTrips(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &tcpProxy{upstream: echo.Addr().String(), delay: 20 * time.Millisecond}
	go func() { _ = p.serve(ctx, ln) }()
	defer ln.Close()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := make([]byte, 4)
	for i := 0; i < 3; i++ {
		start := time.Now()
		if _, err := c.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatal(err)
		}
		if rtt := time.Since(start); rtt < 40*time.Millisecond || rtt > 500*time.Millisecond {
			t.Errorf("round trip %d took %s, want ~40ms", i, rtt)
		}
		if string(buf) != "ping" {
			t.Fatalf("echo returned %q", buf)
		}
	}

	// Pipelined writes share one delay instead of queueing behind each other.
	start := time.Now()
	for i := 0; i < 10; i++ {
		if _, err := c.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := io.ReadFull(c, make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("10 pipelined pings took %s, want ~40ms", elapsed)
	}
}
