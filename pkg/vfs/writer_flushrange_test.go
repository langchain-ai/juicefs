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
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// gatedStore holds every Put while closed, so a test can keep slices uploading for as long as it needs.
type gatedStore struct {
	object.ObjectStorage
	mu      sync.Mutex
	closed  bool
	open    chan struct{}
	blocked int // Puts waiting at the gate
}

func newGatedStore(inner object.ObjectStorage) *gatedStore {
	return &gatedStore{ObjectStorage: inner, open: make(chan struct{})}
}

func (g *gatedStore) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		g.open = make(chan struct{})
	}
}

func (g *gatedStore) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		g.closed = false
		close(g.open)
	}
}

func (g *gatedStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	g.mu.Lock()
	wait := g.open
	closed := g.closed
	if closed {
		g.blocked++
	}
	g.mu.Unlock()
	if closed {
		<-wait
		g.mu.Lock()
		g.blocked--
		g.mu.Unlock()
	}
	return g.ObjectStorage.Put(ctx, key, in, getters...)
}

func (g *gatedStore) waitBlocked(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		g.mu.Lock()
		n := g.blocked
		g.mu.Unlock()
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			g.release()
			t.Fatal("no upload reached the gate")
		}
		time.Sleep(time.Millisecond)
	}
}

func createGatedTestVFS() (*VFS, *gatedStore) {
	var gated *gatedStore
	v, _ := createTestVFSWithStore(nil, "", func(inner object.ObjectStorage) object.ObjectStorage {
		gated = newGatedStore(inner)
		return gated
	})
	return v, gated
}

func newRangeTestFile(t *testing.T, v *VFS) (Ino, uint64) {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	fe, fh, e := v.Create(ctx, 1, "file", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create file: %s", e)
	}
	return fe.Inode, fh
}

func mustWrite(t *testing.T, v *VFS, ino Ino, fh uint64, off uint64, data []byte) {
	t.Helper()
	if e := v.Write(NewLogContext(meta.Background()), ino, data, off, fh); e != 0 {
		t.Fatalf("write %d bytes at %d: %s", len(data), off, e)
	}
}

func mustRead(t *testing.T, v *VFS, ino Ino, fh uint64, off uint64, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	n, e := v.Read(NewLogContext(meta.Background()), ino, buf, off, fh)
	if e != 0 {
		t.Fatalf("read %d bytes at %d: %s", size, off, e)
	}
	return buf[:n]
}

// mustReadWithin reads like mustRead and fails if the read takes longer than limit. The background flusher leaves a
// slice alone for at least a second after its last write, so a read that waits for it takes at least that long.
func mustReadWithin(t *testing.T, limit time.Duration, v *VFS, ino Ino, fh uint64, off uint64, size int) []byte {
	t.Helper()
	start := time.Now()
	got := mustRead(t, v, ino, fh, off, size)
	if took := time.Since(start); took > limit {
		t.Fatalf("read of %d bytes at %d took %s, want under %s", size, off, took, limit)
	}
	return got
}

// committedSlices counts the data slices the metadata engine holds for one chunk of the file.
func committedSlices(t *testing.T, v *VFS, ino Ino, indx uint32) int {
	t.Helper()
	var slices []meta.Slice
	if st := v.Meta.Read(meta.Background(), ino, indx, &slices); st != 0 {
		t.Fatalf("meta read chunk %d: %s", indx, st)
	}
	n := 0
	for _, s := range slices {
		if s.Id != 0 {
			n++
		}
	}
	return n
}

func TestReadSeesItsOwnPendingWrites(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	cases := []struct {
		name string
		off  uint64
		data []byte
	}{
		{"inside one chunk", 1 << 20, []byte("inside one chunk")},
		{"across a chunk boundary", meta.ChunkSize - 7, []byte("across a chunk boundary")},
		{"past the end of the file", 5 * meta.ChunkSize, []byte("past the end")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustWrite(t, v, ino, fh, c.off, c.data)
			if got := mustRead(t, v, ino, fh, c.off, len(c.data)); !bytes.Equal(got, c.data) {
				t.Fatalf("read %q, want %q", got, c.data)
			}
		})
	}
}

func TestReadLeavesPendingWritesOutsideItsRange(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	mustWrite(t, v, ino, fh, 0, []byte("read me"))
	mustWrite(t, v, ino, fh, 3*meta.ChunkSize, []byte("leave me pending"))

	if got := mustRead(t, v, ino, fh, 0, 7); string(got) != "read me" {
		t.Fatalf("read %q", got)
	}
	// The background flusher leaves a slice alone for at least a second after its last write.
	if n := committedSlices(t, v, ino, 3); n != 0 {
		t.Fatalf("a read of chunk 0 committed %d slices of chunk 3", n)
	}
	if n := committedSlices(t, v, ino, 0); n != 1 {
		t.Fatalf("chunk 0 has %d committed slices after the read, want 1", n)
	}
	if e := v.Fsync(NewLogContext(meta.Background()), ino, 1, fh); e != 0 {
		t.Fatalf("fsync: %s", e)
	}
	if n := committedSlices(t, v, ino, 3); n != 1 {
		t.Fatalf("chunk 3 has %d committed slices after fsync, want 1", n)
	}
}

func TestReadCommitsEarlierSlicesOfTheSameChunk(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	// Far enough apart that the second write cannot extend the first slice.
	mustWrite(t, v, ino, fh, 0, []byte("first"))
	mustWrite(t, v, ino, fh, 8<<20, []byte("second"))

	// The first slice commits ahead of the second, so the read must start its upload rather than wait for the flusher.
	if got := mustReadWithin(t, 500*time.Millisecond, v, ino, fh, 8<<20, 6); string(got) != "second" {
		t.Fatalf("read %q", got)
	}
	if n := committedSlices(t, v, ino, 0); n != 2 {
		t.Fatalf("chunk 0 has %d committed slices, want both", n)
	}
	if got := mustRead(t, v, ino, fh, 0, 5); string(got) != "first" {
		t.Fatalf("read %q", got)
	}
}

func TestReadOfAnAppendedChunkCommitsTheSliceItDependsOn(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	// Both writes extend the file, so the first slice of chunk 1 depends on the growing slice of chunk 0.
	mustWrite(t, v, ino, fh, 0, []byte("chunk 0 growing"))
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("chunk 1 first"))
	if got := mustReadWithin(t, 500*time.Millisecond, v, ino, fh, meta.ChunkSize, 13); string(got) != "chunk 1 first" {
		t.Fatalf("read %q", got)
	}
}

func TestReadAfterOverwriteReturnsTheNewBytes(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	mustWrite(t, v, ino, fh, 4096, []byte("aaaaaaaaaa"))
	if got := mustRead(t, v, ino, fh, 4096, 10); string(got) != "aaaaaaaaaa" {
		t.Fatalf("read %q", got)
	}
	// The first slice is committed and cached by the reader; the overwrite is a new pending slice.
	mustWrite(t, v, ino, fh, 4099, []byte("bbbb"))
	if got := mustRead(t, v, ino, fh, 4096, 10); string(got) != "aaabbbbaaa" {
		t.Fatalf("read %q, want the overwrite", got)
	}
}

func TestReadAheadOfAPendingOverwriteIsReplacedWhenItCommits(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ino, fh := newRangeTestFile(t, v)
	mustWrite(t, v, ino, fh, 0, bytes.Repeat([]byte("a"), 1<<20))
	if e := v.Fsync(NewLogContext(meta.Background()), ino, 1, fh); e != 0 {
		t.Fatalf("fsync: %s", e)
	}
	mustWrite(t, v, ino, fh, 512<<10, []byte("bbbb"))
	// Sequential reads that stop short of the overwrite fetch ahead of themselves from the committed slices, caching
	// the old bytes of the range the pending overwrite covers.
	for off := uint64(0); off < 448<<10; off += 64 << 10 {
		mustRead(t, v, ino, fh, off, 64<<10)
	}
	if got := mustRead(t, v, ino, fh, 512<<10-2, 8); string(got) != "aabbbbaa" {
		t.Fatalf("read %q, want the pending overwrite", got)
	}
}

func TestWritesProceedWhileAReadWaitsForItsRange(t *testing.T) {
	v, gated := createGatedTestVFS()
	ino, fh := newRangeTestFile(t, v)
	gated.close()
	mustWrite(t, v, ino, fh, 0, []byte("held"))

	read := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4)
		n, _ := v.Read(NewLogContext(meta.Background()), ino, buf, 0, fh)
		read <- buf[:n]
	}()
	// The read has frozen its range and is waiting for that upload.
	gated.waitBlocked(t)

	// A second handle: the read holds its own handle's reader lock, which orders writes through that handle
	// after it; the flush itself must not hold back writes to the inode.
	_, fh2, e := v.Open(NewLogContext(meta.Background()), ino, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("open a second handle: %s", e)
	}
	wrote := make(chan syscall.Errno, 1)
	go func() {
		wrote <- v.Write(NewLogContext(meta.Background()), ino, []byte("elsewhere"), 2*meta.ChunkSize, fh2)
	}()
	select {
	case e := <-wrote:
		if e != 0 {
			t.Fatalf("write while a read waits: %s", e)
		}
	case <-time.After(5 * time.Second):
		gated.release()
		t.Fatal("a write to another range waited for the read's upload")
	}
	select {
	case got := <-read:
		t.Fatalf("the read returned %q before its range was uploaded", got)
	default:
	}

	// Each commit wakes the waiting read; without the wake-up it would notice only at its 3 s poll.
	gated.release()
	select {
	case got := <-read:
		if string(got) != "held" {
			t.Fatalf("read %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the read did not finish within a second of the upload being released")
	}
}
