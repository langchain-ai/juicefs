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

package meta

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// redisFaultProxy sits between the client and Redis and injects the failures a
// Redis restart or failover causes.
type redisFaultProxy struct {
	t      *testing.T
	addr   string
	target string

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}

	// dropExecReply makes the next EXEC reach Redis but loses its reply.
	dropExecReply atomic.Bool
}

func newRedisFaultProxy(t *testing.T, target string) *redisFaultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &redisFaultProxy{t: t, addr: ln.Addr().String(), target: target, conns: map[net.Conn]struct{}{}}
	p.serve(ln)
	t.Cleanup(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.ln != nil {
			_ = p.ln.Close()
		}
		for c := range p.conns {
			_ = c.Close()
		}
	})
	return p
}

func (p *redisFaultProxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(client)
		}
	}()
}

func (p *redisFaultProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range conns {
		p.conns[c] = struct{}{}
	}
}

func (p *redisFaultProxy) forward(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, server)
	var muted atomic.Bool
	closeBoth := func() {
		_ = client.Close()
		_ = server.Close()
	}
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := server.Read(buf)
			if n > 0 && !muted.Load() {
				if _, werr := client.Write(buf[:n]); werr != nil {
					closeBoth()
					return
				}
			}
			if err != nil {
				closeBoth()
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := client.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			drop := bytes.Contains(bytes.ToLower(chunk), []byte("\r\nexec\r\n")) && p.dropExecReply.CompareAndSwap(true, false)
			if drop {
				muted.Store(true)
			}
			if _, werr := server.Write(chunk); werr != nil {
				closeBoth()
				return
			}
			if drop {
				// Let Redis apply the EXEC, then cut the connection before its reply.
				time.Sleep(100 * time.Millisecond)
				closeBoth()
				return
			}
		}
		if err != nil {
			closeBoth()
			return
		}
	}
}

// outage drops every connection and refuses new ones for d, as a restarting
// Redis does.
func (p *redisFaultProxy) outage(d time.Duration) {
	p.mu.Lock()
	_ = p.ln.Close()
	p.ln = nil
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
	time.Sleep(d)
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Errorf("relisten on %s: %v", p.addr, err)
		return
	}
	p.serve(ln)
}

func testRedisAddr() string {
	if addr := os.Getenv("JFS_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func newOutageTestMeta(t *testing.T, tolerance time.Duration) (*redisMeta, *redisFaultProxy) {
	t.Helper()
	old := metaOutageTolerance
	metaOutageTolerance = tolerance
	t.Cleanup(func() { metaOutageTolerance = old })

	proxy := newRedisFaultProxy(t, testRedisAddr())
	// go-redis retries each command (and each dial) on its own for many seconds by
	// default; shrink that so a short outage outlasts it and reaches the
	// transaction's retries.
	conf := testConfig()
	conf.Retries = 1
	metaClient, err := newRedisMeta("redis", proxy.addr+"/13?max-retry-backoff=100ms", conf)
	if err != nil {
		t.Fatalf("create meta: %v", err)
	}
	m := metaClient.(*redisMeta)
	t.Cleanup(func() { m.Shutdown() })
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %v", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init meta: %v", err)
	}
	return m, proxy
}

func newOutageTestFile(t *testing.T, m *redisMeta) Ino {
	t.Helper()
	var inode Ino
	if st := m.Mknod(Background(), RootInode, "disk.img", TypeFile, 0644, 022, 0, "", &inode, nil); st != 0 {
		t.Fatalf("mknod: %s", st)
	}
	return inode
}

func writeTestSlice(t *testing.T, m *redisMeta, inode Ino) syscall.Errno {
	t.Helper()
	var id uint64
	if st := m.NewSlice(Background(), &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	return m.Write(Background(), inode, 0, 0, Slice{Id: id, Size: 4096, Len: 4096}, time.Now())
}

func chunkSliceCount(t *testing.T, m *redisMeta, inode Ino) int64 {
	t.Helper()
	n, err := m.rdb.LLen(Background(), m.chunkKey(inode, 0)).Result()
	if err != nil {
		t.Fatalf("llen: %v", err)
	}
	return n
}

// A slice commit issued while Redis refuses connections must wait for it to come
// back rather than fail: a failed commit poisons the file's writer for good.
func TestRedisWriteRidesOutRefusedConnections(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tolerance time.Duration
		wantErr   bool
	}{
		{name: "tolerated", tolerance: 30 * time.Second},
		{name: "disabled", tolerance: 0, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, proxy := newOutageTestMeta(t, tc.tolerance)
			inode := newOutageTestFile(t, m)

			var id uint64
			if st := m.NewSlice(Background(), &id); st != 0 {
				t.Fatalf("new slice: %s", st)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				proxy.outage(3 * time.Second)
			}()
			time.Sleep(100 * time.Millisecond)
			start := time.Now()
			st := m.Write(Background(), inode, 0, 0, Slice{Id: id, Size: 4096, Len: 4096}, time.Now())
			elapsed := time.Since(start)
			<-done

			if tc.wantErr {
				if st == 0 {
					t.Fatalf("write succeeded during the outage with retries disabled; the outage was not observed")
				}
				return
			}
			if st != 0 {
				t.Fatalf("write failed after %s: %s", elapsed, st)
			}
			if elapsed < 2*time.Second {
				t.Fatalf("write returned after %s, before the outage ended", elapsed)
			}
			if got := chunkSliceCount(t, m, inode); got != 1 {
				t.Fatalf("chunk holds %d slices, want 1", got)
			}
		})
	}
}

// When EXEC reaches Redis but its reply is lost, the commit retries and finds its
// own slice instead of failing or pushing it twice.
func TestRedisWriteReplaysAfterLostExecReply(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tolerance time.Duration
		wantErr   bool
	}{
		{name: "tolerated", tolerance: 30 * time.Second},
		{name: "disabled", tolerance: 0, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, proxy := newOutageTestMeta(t, tc.tolerance)
			inode := newOutageTestFile(t, m)

			proxy.dropExecReply.Store(true)
			st := writeTestSlice(t, m, inode)
			if proxy.dropExecReply.Load() {
				t.Fatalf("the write never sent EXEC")
			}
			if tc.wantErr {
				if st == 0 {
					t.Fatalf("write succeeded with retries disabled; the lost reply was not observed")
				}
				return
			}
			if st != 0 {
				t.Fatalf("write failed: %s", st)
			}
			if got := chunkSliceCount(t, m, inode); got != 1 {
				t.Fatalf("chunk holds %d slices, want exactly 1 (no duplicate push)", got)
			}
			var attr Attr
			if st := m.GetAttr(Background(), inode, &attr); st != 0 {
				t.Fatalf("getattr: %s", st)
			}
			if attr.Length != 4096 {
				t.Fatalf("length %d, want 4096", attr.Length)
			}
		})
	}
}

func TestRedisOutageErrorClassification(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	for _, tc := range []struct {
		err        error
		unsent     bool
		connection bool
	}{
		{err: refused, unsent: true, connection: true},
		{err: reset, connection: true},
		{err: io.EOF, connection: true},
		{err: errors.New("ERR wrong number of arguments"), unsent: false, connection: false},
		{err: syscall.ENOENT},
	} {
		if got := isUnsentRedisError(tc.err); got != tc.unsent {
			t.Errorf("isUnsentRedisError(%v) = %v, want %v", tc.err, got, tc.unsent)
		}
		if got := isRedisConnectionError(tc.err); got != tc.connection {
			t.Errorf("isRedisConnectionError(%v) = %v, want %v", tc.err, got, tc.connection)
		}
	}
}

// A block read or attribute refresh issued while Redis refuses connections must
// wait for it: an I/O error on a VM disk image sends the guest's ext4 read-only.
func TestRedisReadsRideOutRefusedConnections(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tolerance time.Duration
		wantErr   bool
	}{
		{name: "tolerated", tolerance: 30 * time.Second},
		{name: "disabled", tolerance: 0, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, proxy := newOutageTestMeta(t, tc.tolerance)
			inode := newOutageTestFile(t, m)
			if st := writeTestSlice(t, m, inode); st != 0 {
				t.Fatalf("write: %s", st)
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				proxy.outage(3 * time.Second)
			}()
			time.Sleep(100 * time.Millisecond)
			slices, readErr := m.doRead(Background(), inode, 0)
			var attr Attr
			attrErr := m.doGetAttr(Background(), inode, &attr)
			<-done

			if tc.wantErr {
				if readErr == 0 && attrErr == 0 {
					t.Fatalf("reads succeeded during the outage with retries disabled; the outage was not observed")
				}
				return
			}
			if readErr != 0 || attrErr != 0 {
				t.Fatalf("read: %s, getattr: %s", readErr, attrErr)
			}
			if len(slices) != 1 || attr.Length != 4096 {
				t.Fatalf("got %d slices and length %d, want 1 and 4096", len(slices), attr.Length)
			}
		})
	}
}
