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

package vfs

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// ioGate holds calls, keyed by chunk index or slice id, until the test lets them through.
type ioGate struct {
	mu      sync.Mutex
	holding bool
	passed  map[uint64]bool
	opened  map[uint64]chan struct{} // closed when the key is let through
	blocked map[uint64]int           // calls waiting at the gate
	done    map[uint64]int           // calls that went through and returned
}

func newIOGate() *ioGate {
	return &ioGate{
		passed:  make(map[uint64]bool),
		opened:  make(map[uint64]chan struct{}),
		blocked: make(map[uint64]int),
		done:    make(map[uint64]int),
	}
}

// hold makes every later call wait until its key is let through or the gate opens.
func (g *ioGate) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.holding = true
	g.passed = make(map[uint64]bool)
}

// let lets through the calls for key, both those waiting and those still to come.
func (g *ioGate) let(key uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.passed[key] = true
	if ch := g.opened[key]; ch != nil {
		close(ch)
		delete(g.opened, key)
	}
}

// open lets every call through.
func (g *ioGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.holding = false
	for key, ch := range g.opened {
		close(ch)
		delete(g.opened, key)
	}
}

func (g *ioGate) pass(ctx context.Context, key uint64) error {
	g.mu.Lock()
	if !g.holding || g.passed[key] {
		g.mu.Unlock()
		return nil
	}
	ch := g.opened[key]
	if ch == nil {
		ch = make(chan struct{})
		g.opened[key] = ch
	}
	g.blocked[key]++
	g.mu.Unlock()

	var err error
	select {
	case <-ch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	g.mu.Lock()
	g.blocked[key]--
	g.mu.Unlock()
	return err
}

func (g *ioGate) finish(key uint64) {
	g.mu.Lock()
	g.done[key]++
	g.mu.Unlock()
}

func (g *ioGate) waitUntil(limit time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(limit)
	for {
		g.mu.Lock()
		ok := cond()
		g.mu.Unlock()
		if ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

func (g *ioGate) waitBlocked(t *testing.T, key uint64) {
	t.Helper()
	if !g.waitUntil(10*time.Second, func() bool { return g.blocked[key] > 0 }) {
		g.open()
		t.Fatalf("no call for %d reached the gate", key)
	}
}

// gatedMeta holds the chunk lookups of a reader at its gate.
type gatedMeta struct {
	meta.Meta
	gate *ioGate
}

func (m *gatedMeta) Read(ctx meta.Context, inode Ino, indx uint32, slices *[]meta.Slice) syscall.Errno {
	if m.gate.pass(ctx, uint64(indx)) != nil {
		return syscall.EINTR
	}
	return m.Meta.Read(ctx, inode, indx, slices)
}

// gatedChunkStore holds the slice fetches of a reader at its gate.
type gatedChunkStore struct {
	chunk.ChunkStore
	gate *ioGate
}

func (s *gatedChunkStore) NewReader(id uint64, length int) chunk.Reader {
	return &gatedChunkReader{s.ChunkStore.NewReader(id, length), id, s.gate}
}

type gatedChunkReader struct {
	chunk.Reader
	id   uint64
	gate *ioGate
}

func (r *gatedChunkReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	if err := r.gate.pass(ctx, r.id); err != nil {
		return 0, err
	}
	defer r.gate.finish(r.id)
	return r.Reader.ReadAt(ctx, p, off)
}

// createGatedReaderVFS returns a test VFS whose reader looks up chunks and fetches slices through gates; writes and
// commits bypass them.
func createGatedReaderVFS() (v *VFS, lookups, fetches *ioGate) {
	v, _ = createTestVFS(nil, "")
	lookups, fetches = newIOGate(), newIOGate()
	r := NewDataReader(v.Conf, &gatedMeta{v.Meta, lookups}, &gatedChunkStore{v.Store, fetches})
	v.reader = r
	v.writer.(*dataWriter).reader = r
	return v, lookups, fetches
}

type readResult struct {
	data []byte
	err  syscall.Errno
}

func startRead(v *VFS, ino Ino, fh uint64, off uint64, size int) chan readResult {
	res := make(chan readResult, 1)
	go func() {
		buf := make([]byte, size)
		n, e := v.Read(NewLogContext(meta.Background()), ino, buf, off, fh)
		res <- readResult{buf[:n], e}
	}()
	return res
}

func waitRead(t *testing.T, res <-chan readResult) []byte {
	t.Helper()
	select {
	case r := <-res:
		if r.err != 0 {
			t.Fatalf("read: %s", r.err)
		}
		return r.data
	case <-time.After(10 * time.Second):
		t.Fatal("read did not return")
		return nil
	}
}

func writeAndSync(t *testing.T, v *VFS, ino Ino, fh uint64, off uint64, data []byte) {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	if e := v.Write(ctx, ino, data, off, fh); e != 0 {
		t.Fatalf("write %d bytes at %d: %s", len(data), off, e)
	}
	if e := v.Fsync(ctx, ino, 1, fh); e != 0 {
		t.Fatalf("fsync: %s", e)
	}
}

// sliceAt returns the id of the slice that holds byte pos of chunk 0.
func sliceAt(t *testing.T, v *VFS, ino Ino, pos uint32) uint64 {
	t.Helper()
	var slices []meta.Slice
	if st := v.Meta.Read(meta.Background(), ino, 0, &slices); st != 0 {
		t.Fatalf("read chunk: %s", st)
	}
	var off uint32
	for _, s := range slices {
		if pos < off+s.Len {
			return s.Id
		}
		off += s.Len
	}
	t.Fatalf("no slice at %d", pos)
	return 0
}

// awaitEarly gives a read up to limit to return and leaves its result in res for waitRead.
func awaitEarly(res chan readResult, limit time.Duration) {
	select {
	case r := <-res:
		res <- r
	case <-time.After(limit):
	}
}

func fill(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// runs describes b by its runs of equal bytes, such as 32768×'a' 32768×'d'.
func runs(b []byte) string {
	var parts []string
	for i := 0; i < len(b); {
		j := i
		for j < len(b) && b[j] == b[i] {
			j++
		}
		parts = append(parts, fmt.Sprintf("%d×%q", j-i, b[i]))
		i = j
	}
	return strings.Join(parts, " ")
}

// setupTwoBlockFile writes block 0 of a new file as 'a' and block 1 as 'c', each committed on its own, and opens it
// read-only with block 0 already read in.
func setupTwoBlockFile(t *testing.T, v *VFS) (ino Ino, fhw, fh uint64, bs uint64) {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	bs = uint64(v.Conf.Chunk.BlockSize)
	fe, fhw, e := v.Create(ctx, 1, "file", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	ino = fe.Inode
	writeAndSync(t, v, ino, fhw, 0, fill('a', int(bs)))
	writeAndSync(t, v, ino, fhw, bs, fill('c', int(bs)))
	if _, fh, e = v.Open(ctx, ino, syscall.O_RDONLY); e != 0 {
		t.Fatalf("open: %s", e)
	}
	if got := waitRead(t, startRead(v, ino, fh, 4096, int(bs-4096))); !bytes.Equal(got, fill('a', int(bs-4096))) {
		t.Fatalf("first read of block 0 returned %s", runs(got))
	}
	return ino, fhw, fh, bs
}

// A read issued after a write's fsync has returned must return the written bytes, including when another read on the
// same handle still holds the part of the file the commit invalidated.
func TestReadAfterCommitReturnsNewBytes(t *testing.T) {
	v, lookups, _ := createGatedReaderVFS()
	ino, fhw, fh, bs := setupTwoBlockFile(t, v)

	// The first read straddles into block 1, whose lookup is held, so it waits while holding block 0.
	lookups.hold()
	first := startRead(v, ino, fh, bs-64<<10, 128<<10)
	lookups.waitBlocked(t, 0)

	writeAndSync(t, v, ino, fhw, bs/2, fill('b', 64<<10))

	// The new bytes cannot be fetched while lookups are held, so a read that returns meanwhile served cached bytes.
	second := startRead(v, ino, fh, bs/2, 64<<10)
	awaitEarly(second, 500*time.Millisecond)
	lookups.open()
	if got := waitRead(t, second); !bytes.Equal(got, fill('b', 64<<10)) {
		t.Fatalf("read after the commit returned %s, want %d×'b'", runs(got), 64<<10)
	}
	if got := waitRead(t, first); !bytes.Equal(got, cat(fill('a', 64<<10), fill('c', 64<<10))) {
		t.Fatalf("first read returned %s", runs(got))
	}
}

// A read that runs while two writes commit one after the other may return the bytes from before either of them, after
// the first, or after both, but never the second without the first.
func TestReadDuringCommitsSeesThemInOrder(t *testing.T) {
	v, lookups, fetches := createGatedReaderVFS()
	ino, fhw, fh, bs := setupTwoBlockFile(t, v)
	blockOne := sliceAt(t, v, ino, uint32(bs))

	// The read straddles into block 1, whose lookup is held, so it waits while holding block 0.
	lookups.hold()
	fetches.hold()
	read := startRead(v, ino, fh, bs-64<<10, 128<<10)
	lookups.waitBlocked(t, 0)

	writeAndSync(t, v, ino, fhw, bs-64<<10, fill('b', 32<<10))
	writeAndSync(t, v, ino, fhw, bs-32<<10, fill('d', 32<<10))
	second := sliceAt(t, v, ino, uint32(bs-32<<10))

	// Of block 0's slices only the second write's can be fetched. Once that fetch is in, if one happens at all, block 1
	// comes in and the read resumes.
	fetches.let(second)
	lookups.open()
	fetches.waitUntil(500*time.Millisecond, func() bool { return fetches.done[second] > 0 })
	fetches.let(blockOne)
	awaitEarly(read, 500*time.Millisecond)
	fetches.open()

	got := waitRead(t, read)
	for _, head := range [][]byte{
		fill('a', 64<<10),                         // before both writes
		cat(fill('b', 32<<10), fill('a', 32<<10)), // after the first
		cat(fill('b', 32<<10), fill('d', 32<<10)), // after both
	} {
		if bytes.Equal(got, cat(head, fill('c', 64<<10))) {
			return
		}
	}
	t.Fatalf("read returned %s, a state the file was never in", runs(got))
}
