//go:build linux

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

package vhostfs

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// daxMapAlignment is the granularity the guest aligns SETUPMAPPING to, and
// daxMapAlignmentShift is its log2 as reported in FUSE_INIT.
const (
	daxMapAlignmentShift uint32 = 21
	daxMapAlignment      uint64 = 1 << daxMapAlignmentShift
)

// daxShmid is the VIRTIO shared memory region id virtio-fs uses for its window.
const daxShmid uint8 = 0

// chunkKey identifies a materialized range of a file.
type chunkKey struct {
	nodeID  uint64
	fOffset uint64
	length  uint64
}

// chunk is one file range materialized as a memfd.
//
// JuiceFS files are objects in a bucket, not host files, so there is no fd to
// hand a VMM for a DAX mapping. Each range is copied once into a memfd this
// process owns; the VMM then mmaps that memfd into the guest's window. Because
// the cache is per-Server rather than per-VM, a second guest asking for the same
// range maps the same pages instead of paying for its own copy.
type chunk struct {
	key  chunkKey
	file *os.File
	refs int
}

// chunkCache holds materialized ranges shared by every session on a Server.
type chunkCache struct {
	fs fuse.RawFileSystem

	mu     sync.Mutex
	chunks map[chunkKey]*chunk
}

func newChunkCache(fs fuse.RawFileSystem) *chunkCache {
	return &chunkCache{fs: fs, chunks: make(map[chunkKey]*chunk)}
}

// acquire returns the chunk for a range, materializing it on first use, with a
// reference held on behalf of the caller.
func (cc *chunkCache) acquire(cancel <-chan struct{}, hdr *fuse.InHeader, in *setupmappingIn) (*chunk, fuse.Status) {
	key := chunkKey{nodeID: hdr.NodeId, fOffset: in.Foffset, length: in.Len}

	cc.mu.Lock()
	if existing, ok := cc.chunks[key]; ok {
		existing.refs++
		cc.mu.Unlock()
		return existing, fuse.OK
	}
	cc.mu.Unlock()

	// Materialize outside the lock: a cold range is a read through the whole
	// storage stack, and holding the lock would serialize every other guest's
	// mappings behind it.
	fresh, st := cc.materialize(cancel, hdr, in, key)
	if st != fuse.OK {
		return nil, st
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()
	// Another session may have materialized the same range meanwhile; keep the one
	// already published so both guests share it.
	if existing, ok := cc.chunks[key]; ok {
		_ = fresh.file.Close()
		existing.refs++
		return existing, fuse.OK
	}
	fresh.refs = 1
	cc.chunks[key] = fresh
	return fresh, fuse.OK
}

func (cc *chunkCache) materialize(cancel <-chan struct{}, hdr *fuse.InHeader, in *setupmappingIn, key chunkKey) (*chunk, fuse.Status) {
	fd, err := unix.MemfdCreate(fmt.Sprintf("juicefs-dax-%d", hdr.NodeId), unix.MFD_CLOEXEC)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	file := os.NewFile(uintptr(fd), "juicefs-dax")
	// The window maps whole aligned chunks, so the memfd is sized to the request
	// even when the file ends inside it; the tail reads as zeroes, which is what a
	// mapping past EOF gives anyway.
	if err := file.Truncate(int64(in.Len)); err != nil {
		_ = file.Close()
		return nil, errnoToStatus(err)
	}

	// The kernel sends no file handle with SETUPMAPPING — it sets fh to -1, since a
	// mapping outlives the descriptor that triggered it — so the range is read
	// through a handle opened here against the request's inode.
	var opened fuse.OpenOut
	openIn := &fuse.OpenIn{InHeader: *hdr, Flags: syscall.O_RDONLY}
	if st := cc.fs.Open(cancel, openIn, &opened); st != fuse.OK {
		_ = file.Close()
		return nil, st
	}
	defer cc.fs.Release(cancel, &fuse.ReleaseIn{InHeader: *hdr, Fh: opened.Fh})

	buf := make([]byte, in.Len)
	read := &fuse.ReadIn{
		InHeader: *hdr,
		Fh:       opened.Fh,
		Offset:   in.Foffset,
		Size:     uint32(in.Len),
	}
	res, st := cc.fs.Read(cancel, read, buf)
	if st != fuse.OK {
		_ = file.Close()
		return nil, st
	}
	data, st := res.Bytes(buf)
	if st != fuse.OK {
		_ = file.Close()
		return nil, st
	}
	if len(data) > 0 {
		if _, err := file.WriteAt(data, 0); err != nil {
			_ = file.Close()
			return nil, errnoToStatus(err)
		}
	}
	return &chunk{key: key, file: file}, fuse.OK
}

// release drops one reference, closing the memfd when the last one goes.
func (cc *chunkCache) release(c *chunk) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	c.refs--
	if c.refs > 0 {
		return
	}
	delete(cc.chunks, c.key)
	_ = c.file.Close()
}

func (cc *chunkCache) close() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, c := range cc.chunks {
		_ = c.file.Close()
	}
	cc.chunks = make(map[chunkKey]*chunk)
}

// daxWindow is one guest's DAX window: which chunk currently occupies each offset.
type daxWindow struct {
	cache  *chunkCache
	sender *backendSender

	mu     sync.Mutex
	active map[uint64]*chunk
}

func newDaxWindow(cache *chunkCache, sender *backendSender) *daxWindow {
	return &daxWindow{cache: cache, sender: sender, active: make(map[uint64]*chunk)}
}

// setup handles FUSE_SETUPMAPPING: materialize the range, then ask the VMM to map
// it into the window at the offset the guest chose.
func (w *daxWindow) setup(cancel <-chan struct{}, hdr *fuse.InHeader, in *setupmappingIn) fuse.Status {
	if in.Len == 0 || in.Len%daxMapAlignment != 0 || in.Moffset%daxMapAlignment != 0 {
		return fuse.EINVAL
	}
	// A writable mapping would let the guest write pages we never read back, so the
	// write would be silently lost. Reads are what the window is for.
	if in.Flags&setupmappingWrite != 0 {
		return fuse.ENOSYS
	}

	logger.Debugf("vhost-user-fs: setupmapping ino=%d foffset=%d len=%d moffset=%d",
		hdr.NodeId, in.Foffset, in.Len, in.Moffset)
	c, st := w.cache.acquire(cancel, hdr, in)
	if st != fuse.OK {
		logger.Warnf("vhost-user-fs: setupmapping ino=%d could not materialize: %d", hdr.NodeId, st)
		return st
	}

	w.mu.Lock()
	prev, held := w.active[in.Moffset]
	w.active[in.Moffset] = c
	w.mu.Unlock()
	// Whatever this offset held before is no longer mapped there.
	if held && prev != c {
		w.cache.release(prev)
	}

	if err := w.sender.shmemMap(shmemMapMsg{
		Shmid:     daxShmid,
		FdOffset:  0,
		ShmOffset: in.Moffset,
		Len:       in.Len,
		Flags:     shmemMapR,
	}, int(c.file.Fd())); err != nil {
		logger.Warnf("vhost-user-fs: shmem map moffset=%d len=%d failed: %s", in.Moffset, in.Len, err)
		w.mu.Lock()
		if w.active[in.Moffset] == c {
			delete(w.active, in.Moffset)
		}
		w.mu.Unlock()
		w.cache.release(c)
		return fuse.EIO
	}
	logger.Debugf("vhost-user-fs: mapped ino=%d moffset=%d len=%d", hdr.NodeId, in.Moffset, in.Len)
	return fuse.OK
}

// remove handles FUSE_REMOVEMAPPING for one window range.
func (w *daxWindow) remove(moffset, length uint64) fuse.Status {
	if length == 0 || length%daxMapAlignment != 0 || moffset%daxMapAlignment != 0 {
		return fuse.EINVAL
	}
	w.mu.Lock()
	c, held := w.active[moffset]
	if held {
		delete(w.active, moffset)
	}
	w.mu.Unlock()
	if held {
		w.cache.release(c)
	}

	// Unmap even when the range was not tracked: the guest believes it is mapped,
	// so the window has to be restored either way.
	if err := w.sender.shmemUnmap(shmemMapMsg{
		Shmid:     daxShmid,
		ShmOffset: moffset,
		Len:       length,
	}); err != nil {
		logger.Warnf("vhost-user-fs: shmem unmap failed: %s", err)
		return fuse.EIO
	}
	return fuse.OK
}

// close releases every range this guest had mapped.
func (w *daxWindow) close() {
	w.mu.Lock()
	active := w.active
	w.active = make(map[uint64]*chunk)
	w.mu.Unlock()
	for _, c := range active {
		w.cache.release(c)
	}
}

// backendAckTimeout bounds how long a mapping request waits for the VMM.
const backendAckTimeout = 30 * time.Second

// backendSender serializes backend-initiated requests on the channel the frontend
// handed us, waiting for each ack before sending the next.
type backendSender struct {
	mu sync.Mutex
	c  *conn
}

func (b *backendSender) shmemMap(msg shmemMapMsg, fd int) error {
	return b.request(backendReqShmemMap, msg, fd)
}

func (b *backendSender) shmemUnmap(msg shmemMapMsg) error {
	return b.request(backendReqShmemUnmap, msg, -1)
}

func (b *backendSender) request(req backendReq, msg shmemMapMsg, fd int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.c.sendBackendReq(req, encodeShmemMap(msg), fd, true); err != nil {
		return err
	}
	// Bounded: the guest's read is parked on this reply, so a VMM that never answers
	// must surface as an I/O error rather than an unkillable process in the guest.
	res, err := b.c.recvAckWithin(backendAckTimeout)
	if err != nil {
		return err
	}
	// The frontend answers 0 on success and a negative errno otherwise.
	if int64(res) != 0 {
		return fmt.Errorf("frontend rejected request %d: %d", req, int64(res))
	}
	return nil
}
