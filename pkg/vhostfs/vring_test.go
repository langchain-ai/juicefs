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
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests drive a real split virtqueue laid out in a fake guest memory
// buffer, so the descriptor walk, the reply scatter, and the used-ring bookkeeping
// are exercised the way a guest kernel sees them. A guest that gets a malformed
// completion stalls without ever sending another request, which is invisible from
// the dispatch layer alone.

const (
	testRingSize  = 8
	testMemBase   = uint64(0x4000_0000)
	testDescOff   = 0x1000
	testAvailOff  = 0x2000
	testUsedOff   = 0x3000
	testBufferOff = 0x4000
)

// testQueue is a guest-side view of one virtqueue, used to publish requests and
// inspect completions.
type testQueue struct {
	mem  *guestMemory
	buf  []byte
	next uint64 // next free buffer offset
}

func newTestQueue(t *testing.T) (*testQueue, *vring) {
	t.Helper()
	buf := make([]byte, 1<<20)
	mem := &guestMemory{regions: []region{{
		guestPhysAddr: testMemBase,
		userAddr:      testMemBase,
		size:          uint64(len(buf)),
		data:          buf,
	}}}
	q := &testQueue{mem: mem, buf: buf, next: testBufferOff}
	v := &vring{
		index:     requestQueue,
		size:      testRingSize,
		descTable: buf[testDescOff : testDescOff+testRingSize*descSize],
		avail:     buf[testAvailOff : testAvailOff+6+testRingSize*2],
		used:      buf[testUsedOff : testUsedOff+6+testRingSize*8],
		enabled:   true,
	}
	return q, v
}

// alloc copies data into guest memory and returns its guest physical address.
func (q *testQueue) alloc(data []byte) (uint64, uint32) {
	off := q.next
	copy(q.buf[off:], data)
	q.next += uint64(len(data))
	// Keep buffers from sharing a descriptor's worth of space.
	q.next = (q.next + 63) & ^uint64(63)
	return testMemBase + off, uint32(len(data))
}

// allocWritable reserves a device-writable buffer and returns its address.
func (q *testQueue) allocWritable(size int) (uint64, uint32, []byte) {
	off := q.next
	q.next += uint64(size)
	q.next = (q.next + 63) & ^uint64(63)
	return testMemBase + off, uint32(size), q.buf[off : off+uint64(size)]
}

// writeDesc fills descriptor idx.
func (q *testQueue) writeDesc(idx uint16, addr uint64, length uint32, flags uint16, next uint16) {
	off := testDescOff + int(idx)*descSize
	binary.LittleEndian.PutUint64(q.buf[off:], addr)
	binary.LittleEndian.PutUint32(q.buf[off+8:], length)
	binary.LittleEndian.PutUint16(q.buf[off+12:], flags)
	binary.LittleEndian.PutUint16(q.buf[off+14:], next)
}

// publish makes head available to the device, the way a guest kick does.
func (q *testQueue) publish(slot, head uint16) {
	binary.LittleEndian.PutUint16(q.buf[testAvailOff+4+int(slot)*2:], head)
	binary.LittleEndian.PutUint16(q.buf[testAvailOff+2:], slot+1)
}

// usedEntry reads back what the device published for slot.
func (q *testQueue) usedEntry(slot uint16) (id uint32, length uint32) {
	off := testUsedOff + 4 + int(slot)*8
	return binary.LittleEndian.Uint32(q.buf[off:]), binary.LittleEndian.Uint32(q.buf[off+4:])
}

func (q *testQueue) usedIdx() uint16 {
	return binary.LittleEndian.Uint16(q.buf[testUsedOff+2:])
}

// A read reply spans two device-writable descriptors — the out header and the data
// buffer — so the scatter across them and the reported length have to be right.
func TestVringReadReplySpansWritableDescriptors(t *testing.T) {
	fs := newStubFS()
	fs.readData = []byte("mapped-through-virtiofs")
	s := newTestSession(t, fs)
	q, v := newTestQueue(t)

	in := fuse.ReadIn{Fh: 7, Offset: 0, Size: uint32(len(fs.readData))}
	request := asRequest(t, opRead, 2, &in)
	reqAddr, reqLen := q.alloc(request)
	hdrAddr, hdrLen, hdrBuf := q.allocWritable(outHeaderSize)
	dataAddr, dataLen, dataBuf := q.allocWritable(len(fs.readData))

	// desc 0 -> desc 1 -> desc 2, mirroring a guest's read chain.
	q.writeDesc(0, reqAddr, reqLen, descFNext, 1)
	q.writeDesc(1, hdrAddr, hdrLen, descFWrite|descFNext, 2)
	q.writeDesc(2, dataAddr, dataLen, descFWrite, 0)
	q.publish(0, 0)

	c, ok, err := v.nextChain(q.mem)
	require.NoError(t, err)
	require.True(t, ok, "the device must see the published chain")
	require.Len(t, c.readable, 1)
	require.Len(t, c.writable, 2, "header and data are separate descriptors")

	reply, err := s.handleChain(c)
	require.NoError(t, err)
	written := scatter(c.writable, reply)
	require.NoError(t, v.addUsed(c.head, written))

	assert.Equal(t, uint16(1), q.usedIdx(), "one completion published")
	id, length := q.usedEntry(0)
	assert.Equal(t, uint32(0), id, "completion names the chain head")
	assert.Equal(t, uint32(outHeaderSize+len(fs.readData)), length,
		"reported length covers the header and the data")

	// The guest reads its reply out of its own buffers, so that is where it must land.
	out := (*fuse.OutHeader)(unsafe.Pointer(&hdrBuf[0]))
	assert.Equal(t, int32(fuse.OK), out.Status)
	assert.Equal(t, uint32(outHeaderSize+len(fs.readData)), out.Length)
	assert.Equal(t, fs.readData, dataBuf)
}

func TestVringConsumesChainsInOrder(t *testing.T) {
	s := newTestSession(t, newStubFS())
	q, v := newTestQueue(t)

	for slot := uint16(0); slot < 3; slot++ {
		request := asRequest(t, opGetattr, uint64(slot+1), &fuse.GetAttrIn{})
		reqAddr, reqLen := q.alloc(request)
		outAddr, outLen, _ := q.allocWritable(outHeaderSize + int(unsafe.Sizeof(fuse.AttrOut{})))
		desc := slot * 2
		q.writeDesc(desc, reqAddr, reqLen, descFNext, desc+1)
		q.writeDesc(desc+1, outAddr, outLen, descFWrite, 0)
		q.publish(slot, desc)
	}

	for slot := uint16(0); slot < 3; slot++ {
		c, ok, err := v.nextChain(q.mem)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, slot*2, c.head, "chains come back in the order the guest published them")
		reply, err := s.handleChain(c)
		require.NoError(t, err)
		require.NoError(t, v.addUsed(c.head, scatter(c.writable, reply)))
	}

	_, ok, err := v.nextChain(q.mem)
	require.NoError(t, err)
	assert.False(t, ok, "no chain remains once the available ring is drained")
	assert.Equal(t, uint16(3), q.usedIdx())
}

// A chain whose descriptors loop must be rejected rather than walked forever.
func TestVringRejectsCyclicChain(t *testing.T) {
	q, v := newTestQueue(t)
	addr, length := q.alloc(make([]byte, 64))
	q.writeDesc(0, addr, length, descFNext, 1)
	q.writeDesc(1, addr, length, descFNext, 0)
	q.publish(0, 0)

	_, _, err := v.nextChain(q.mem)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too long")
}

// Indirect descriptors are not implemented, so they must fail loudly instead of
// being silently treated as direct ones.
func TestVringRejectsIndirectDescriptors(t *testing.T) {
	q, v := newTestQueue(t)
	addr, length := q.alloc(make([]byte, 64))
	q.writeDesc(0, addr, length, descFIndirect, 0)
	q.publish(0, 0)

	_, _, err := v.nextChain(q.mem)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "indirect")
}

func TestVringRejectsDescriptorOutsideGuestMemory(t *testing.T) {
	q, v := newTestQueue(t)
	q.writeDesc(0, testMemBase+(1<<30), 64, 0, 0)
	q.publish(0, 0)

	_, _, err := v.nextChain(q.mem)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not mapped")
}
