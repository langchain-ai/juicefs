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

// delayproxy makes a local object store or Redis respond like a remote one, so
// benchmarks against local backends see production-like latency.
//
// In http mode it is a reverse proxy for an S3-compatible endpoint. Each request
// waits for a delay drawn from a log-normal distribution fitted to a p50 and p99
// for its class (get, put, other), and request and response bodies are limited
// to a per-stream bandwidth and an optional aggregate one. The Host header is
// passed through unchanged, so SigV4 signatures made for the proxy address
// still verify upstream.
//
// In tcp mode it forwards a TCP connection and delivers each chunk a fixed
// one-way delay after it was read, in both directions, without serializing
// pipelined traffic. That adds 2x the delay to every round trip, which is the
// shape of a networked Redis.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type latency struct {
	p50, p99 time.Duration
}

type profile struct {
	get, put, other latency
	streamMBps      float64
}

// Starting points, not measurements: calibrate against the real bucket with
// `juicefs objbench` and override the flags (see README.md).
var profiles = map[string]profile{
	"none": {},
	"s3-standard": {
		get:        latency{25 * time.Millisecond, 120 * time.Millisecond},
		put:        latency{40 * time.Millisecond, 200 * time.Millisecond},
		other:      latency{20 * time.Millisecond, 100 * time.Millisecond},
		streamMBps: 90,
	},
	"s3-express": {
		get:        latency{4 * time.Millisecond, 15 * time.Millisecond},
		put:        latency{6 * time.Millisecond, 25 * time.Millisecond},
		other:      latency{4 * time.Millisecond, 15 * time.Millisecond},
		streamMBps: 90,
	},
}

// maxTailFactor caps a sample at this multiple of p99 so a long log-normal tail
// cannot stall one request for minutes.
const maxTailFactor = 10

// z99 is the standard normal quantile at 0.99.
const z99 = 2.3263478740408408

type sampler struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newSampler(seed uint64) *sampler {
	return &sampler{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

func (s *sampler) sample(l latency) time.Duration {
	if l.p50 <= 0 {
		return 0
	}
	if l.p99 <= l.p50 {
		return l.p50
	}
	sigma := math.Log(float64(l.p99)/float64(l.p50)) / z99
	s.mu.Lock()
	z := s.rng.NormFloat64()
	s.mu.Unlock()
	d := time.Duration(float64(l.p50) * math.Exp(sigma*z))
	if limit := maxTailFactor * l.p99; d > limit {
		d = limit
	}
	return d
}

// classify maps an S3 request to a latency class. Object reads are "get";
// uploads, including multipart parts, are "put"; listing, HEAD, DELETE and
// bucket operations are "other".
func classify(r *http.Request) string {
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		return "put"
	case http.MethodGet:
		path := strings.Trim(r.URL.EscapedPath(), "/")
		if strings.Contains(path, "/") && r.URL.Query().Get("list-type") == "" {
			return "get"
		}
	}
	return "other"
}

// limiter paces bytes against a bandwidth. A nil limiter is unlimited.
type limiter struct {
	mu          sync.Mutex
	bytesPerSec float64
	next        time.Time
}

func newLimiter(mbps float64) *limiter {
	if mbps <= 0 {
		return nil
	}
	return &limiter{bytesPerSec: mbps * 1e6}
}

// maxLag is how far the pacing schedule may fall behind the clock. Sleeps
// overshoot (by a millisecond or more on some VMs), and without credit for that
// the effective rate collapses well below the target; keeping up to maxLag of
// it lets later reads catch up. It also bounds the burst after an idle gap.
const maxLag = 20 * time.Millisecond

// wait blocks until n bytes fit within the bandwidth after everything already
// reserved.
func (l *limiter) wait(ctx context.Context, n int) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.IsZero() {
		l.next = now
	} else if floor := now.Add(-maxLag); l.next.Before(floor) {
		l.next = floor
	}
	l.next = l.next.Add(time.Duration(float64(n) / l.bytesPerSec * float64(time.Second)))
	until := l.next
	l.mu.Unlock()
	return sleepCtx(ctx, time.Until(until))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const throttleChunk = 64 << 10

// throttledBody paces reads through a per-stream limiter and the shared one.
type throttledBody struct {
	io.ReadCloser
	ctx    context.Context
	stream *limiter
	total  *limiter
}

func (b *throttledBody) Read(p []byte) (int, error) {
	if len(p) > throttleChunk {
		p = p[:throttleChunk]
	}
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		if werr := b.stream.wait(b.ctx, n); werr != nil {
			return n, werr
		}
		if werr := b.total.wait(b.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}

type classStats struct {
	requests atomic.Int64
	delayNs  atomic.Int64
}

type httpProxy struct {
	prof       profile
	total      *limiter
	sampler    *sampler
	stats      map[string]*classStats
	transport  http.RoundTripper
	reverse    *httputil.ReverseProxy
	logDelays  bool
	classOrder []string
}

func newHTTPProxy(upstream *url.URL, prof profile, totalMBps float64, seed uint64) *httpProxy {
	p := &httpProxy{
		prof:       prof,
		total:      newLimiter(totalMBps),
		sampler:    newSampler(seed),
		classOrder: []string{"get", "put", "other"},
		stats:      map[string]*classStats{"get": {}, "put": {}, "other": {}},
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 1024
	tr.MaxIdleConnsPerHost = 1024
	p.transport = tr
	p.reverse = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			// SigV4 signs the Host the client addressed; keep it.
			pr.Out.Host = pr.In.Host
			if pr.Out.Body != nil && pr.Out.Body != http.NoBody {
				pr.Out.Body = p.throttle(pr.Out.Context(), pr.Out.Body)
			}
		},
		Transport: roundTripFunc(p.roundTrip),
		ModifyResponse: func(resp *http.Response) error {
			if resp.Body != nil && resp.Body != http.NoBody {
				resp.Body = p.throttle(resp.Request.Context(), resp.Body)
			}
			return nil
		},
		ErrorLog: log.New(os.Stderr, "delayproxy: ", log.LstdFlags),
	}
	return p
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (p *httpProxy) throttle(ctx context.Context, body io.ReadCloser) io.ReadCloser {
	stream := newLimiter(p.prof.streamMBps)
	if stream == nil && p.total == nil {
		return body
	}
	return &throttledBody{ReadCloser: body, ctx: ctx, stream: stream, total: p.total}
}

func (p *httpProxy) latencyFor(class string) latency {
	switch class {
	case "get":
		return p.prof.get
	case "put":
		return p.prof.put
	}
	return p.prof.other
}

func (p *httpProxy) roundTrip(r *http.Request) (*http.Response, error) {
	class := classify(r)
	d := p.sampler.sample(p.latencyFor(class))
	st := p.stats[class]
	st.requests.Add(1)
	st.delayNs.Add(int64(d))
	if p.logDelays {
		log.Printf("%s %s %s delay=%s", class, r.Method, r.URL.Path, d)
	}
	if err := sleepCtx(r.Context(), d); err != nil {
		return nil, err
	}
	return p.transport.RoundTrip(r)
}

func (p *httpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.reverse.ServeHTTP(w, r) }

func (p *httpProxy) report() string {
	var b strings.Builder
	for _, c := range p.classOrder {
		st := p.stats[c]
		n := st.requests.Load()
		mean := time.Duration(0)
		if n > 0 {
			mean = time.Duration(st.delayNs.Load() / n)
		}
		fmt.Fprintf(&b, "%s: requests=%d mean_injected_delay=%s  ", c, n, mean.Round(time.Microsecond))
	}
	return strings.TrimSpace(b.String())
}

// tcpProxy forwards connections, delivering each chunk delay after it was read.
type tcpProxy struct {
	upstream string
	delay    time.Duration
	conns    atomic.Int64
}

type timedChunk struct {
	data []byte
	due  time.Time
}

func (t *tcpProxy) serve(ctx context.Context, ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go t.handle(ctx, c)
	}
}

func (t *tcpProxy) handle(ctx context.Context, client net.Conn) {
	defer client.Close()
	var d net.Dialer
	up, err := d.DialContext(ctx, "tcp", t.upstream)
	if err != nil {
		log.Printf("dial %s: %v", t.upstream, err)
		return
	}
	defer up.Close()
	t.conns.Add(1)
	done := make(chan struct{}, 2)
	go func() { t.pipe(up, client); done <- struct{}{} }()
	go func() { t.pipe(client, up); done <- struct{}{} }()
	<-done
}

// pipe copies src to dst. A reader stamps each chunk with its due time and a
// writer delivers it then, so a burst of pipelined commands is delayed once,
// not once per command.
func (t *tcpProxy) pipe(dst, src net.Conn) {
	q := make(chan timedChunk, 1024)
	go func() {
		defer close(q)
		for {
			buf := make([]byte, 32<<10)
			n, err := src.Read(buf)
			if n > 0 {
				q <- timedChunk{buf[:n], time.Now().Add(t.delay)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range q {
		if w := time.Until(c.due); w > 0 {
			time.Sleep(w)
		}
		if _, err := dst.Write(c.data); err != nil {
			break
		}
	}
	// Unblock the peer pipe and the reader above.
	_ = dst.Close()
	_ = src.Close()
	for range q {
	}
}

func main() {
	mode := flag.String("mode", "http", "http (S3 reverse proxy) or tcp (fixed one-way delay, e.g. for Redis)")
	listen := flag.String("listen", "127.0.0.1:9100", "listen address")
	upstream := flag.String("upstream", "", "upstream: a URL in http mode, host:port in tcp mode")
	profName := flag.String("profile", "s3-standard", "http mode latency preset: none, s3-standard, s3-express")
	getP50 := flag.Duration("get-p50", -1, "override the profile's GET p50")
	getP99 := flag.Duration("get-p99", -1, "override the profile's GET p99")
	putP50 := flag.Duration("put-p50", -1, "override the profile's PUT p50")
	putP99 := flag.Duration("put-p99", -1, "override the profile's PUT p99")
	otherP50 := flag.Duration("other-p50", -1, "override the profile's p50 for other requests")
	otherP99 := flag.Duration("other-p99", -1, "override the profile's p99 for other requests")
	streamMBps := flag.Float64("stream-mbps", -1, "override the per-request bandwidth in MB/s (0 = unlimited)")
	totalMBps := flag.Float64("total-mbps", 0, "aggregate bandwidth in MB/s across all requests (0 = unlimited)")
	delay := flag.Duration("delay", 250*time.Microsecond, "tcp mode one-way delay")
	seed := flag.Uint64("seed", 1, "random seed for latency samples")
	statsEvery := flag.Duration("stats-interval", 0, "log per-class stats this often (0 = only at exit)")
	logDelays := flag.Bool("log-requests", false, "log every request with its injected delay")
	flag.Parse()

	if *upstream == "" {
		log.Fatal("-upstream is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "tcp":
		t := &tcpProxy{upstream: *upstream, delay: *delay}
		log.Printf("tcp %s -> %s one-way delay %s", ln.Addr(), *upstream, *delay)
		go func() { <-ctx.Done(); _ = ln.Close() }()
		if err := t.serve(ctx, ln); err != nil {
			log.Fatal(err)
		}
		log.Printf("exiting after %d connections", t.conns.Load())
	case "http":
		prof, ok := profiles[*profName]
		if !ok {
			log.Fatalf("unknown profile %q", *profName)
		}
		override := func(dst *time.Duration, v time.Duration) {
			if v >= 0 {
				*dst = v
			}
		}
		override(&prof.get.p50, *getP50)
		override(&prof.get.p99, *getP99)
		override(&prof.put.p50, *putP50)
		override(&prof.put.p99, *putP99)
		override(&prof.other.p50, *otherP50)
		override(&prof.other.p99, *otherP99)
		if *streamMBps >= 0 {
			prof.streamMBps = *streamMBps
		}
		u, err := url.Parse(*upstream)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Fatalf("bad -upstream URL %q", *upstream)
		}
		p := newHTTPProxy(u, prof, *totalMBps, *seed)
		p.logDelays = *logDelays
		log.Printf("http %s -> %s profile=%s get=%s/%s put=%s/%s other=%s/%s stream=%gMB/s total=%gMB/s",
			ln.Addr(), u, *profName, prof.get.p50, prof.get.p99, prof.put.p50, prof.put.p99,
			prof.other.p50, prof.other.p99, prof.streamMBps, *totalMBps)
		srv := &http.Server{Handler: p, ReadHeaderTimeout: time.Minute}
		if *statsEvery > 0 {
			go func() {
				tk := time.NewTicker(*statsEvery)
				defer tk.Stop()
				for {
					select {
					case <-tk.C:
						log.Print(p.report())
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		go func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
		log.Print(p.report())
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
}
