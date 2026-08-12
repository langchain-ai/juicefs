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
	"testing"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests drive the FUSE decode/dispatch/encode path with the byte layout a
// guest kernel actually puts on the request queue, with no VM involved. The
// transport's sharpest edge is that go-fuse's input structs embed InHeader, so
// they describe a request from its first byte; reading them from the body after
// the header silently shifts every field by the header's width and makes the
// kernel abort the connection.

// stubFS records what the transport asked of the filesystem and answers with
// canned data.
type stubFS struct {
	fuse.RawFileSystem

	lookupName string
	lookupNode uint64

	openNode uint64
	openFh   uint64

	readNode   uint64
	readFh     uint64
	readOffset uint64
	readSize   uint32
	readData   []byte

	forgotNode   uint64
	forgotLookup uint64
}

func newStubFS() *stubFS {
	return &stubFS{RawFileSystem: fuse.NewDefaultRawFileSystem(), openFh: 42}
}

func (f *stubFS) Lookup(_ <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	f.lookupNode, f.lookupName = header.NodeId, name
	out.NodeId = 2
	out.Attr.Ino = 2
	out.Attr.Size = uint64(len(f.readData))
	out.Attr.Mode = fuse.S_IFREG | 0o444
	return fuse.OK
}

func (f *stubFS) Open(_ <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	f.openNode = in.NodeId
	out.Fh = f.openFh
	return fuse.OK
}

func (f *stubFS) Read(_ <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	f.readNode, f.readFh, f.readOffset, f.readSize = in.NodeId, in.Fh, in.Offset, in.Size
	n := copy(buf, f.readData)
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func (f *stubFS) Forget(nodeid, nlookup uint64) {
	f.forgotNode, f.forgotLookup = nodeid, nlookup
}

func newTestSession(t *testing.T, fs fuse.RawFileSystem) *session {
	t.Helper()
	srv := &Server{fs: fs, maxWrite: 128 << 10}
	return newSession(srv, nil)
}

// asRequest serializes a typed input struct the way a guest lays it out: the
// InHeader it embeds first, then the operation's own fields.
func asRequest[T any](t *testing.T, opcode uint32, nodeID uint64, in *T, trailer ...byte) []byte {
	t.Helper()
	size := int(unsafe.Sizeof(*in))
	buf := make([]byte, size, size+len(trailer))
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(in)), size))
	hdr := (*fuse.InHeader)(unsafe.Pointer(&buf[0]))
	hdr.Opcode = opcode
	hdr.NodeId = nodeID
	hdr.Unique = 7
	buf = append(buf, trailer...)
	hdr = (*fuse.InHeader)(unsafe.Pointer(&buf[0]))
	hdr.Length = uint32(len(buf))
	return buf
}

// runOp feeds one request through the transport and returns the raw reply.
func runOp(t *testing.T, s *session, request []byte, replyCap int) []byte {
	t.Helper()
	writable := make([]byte, replyCap)
	reply, err := s.handleChain(chain{readable: [][]byte{request}, writable: [][]byte{writable}})
	require.NoError(t, err)
	return reply
}

func replyHeader(t *testing.T, reply []byte) *fuse.OutHeader {
	t.Helper()
	require.GreaterOrEqual(t, len(reply), outHeaderSize)
	return (*fuse.OutHeader)(unsafe.Pointer(&reply[0]))
}

// A rejected FUSE_INIT makes the kernel abort the connection, after which every
// operation on the mount fails with ECONNREFUSED — so this is the one reply that
// has to be right before anything else can work.
func TestDispatchInitIsAccepted(t *testing.T) {
	s := newTestSession(t, newStubFS())
	in := fuse.InitIn{Major: 7, Minor: 38, MaxReadAhead: 128 << 10, Flags: fuse.CAP_ASYNC_READ}
	request := asRequest(t, opInit, 0, &in)

	reply := runOp(t, s, request, outHeaderSize+int(unsafe.Sizeof(fuse.InitOut{})))

	out := replyHeader(t, reply)
	require.Equal(t, int32(fuse.OK), out.Status, "init must not be rejected")
	assert.Equal(t, uint64(7), out.Unique)
	assert.Equal(t, uint32(len(reply)), out.Length)

	init := (*fuse.InitOut)(unsafe.Pointer(&reply[outHeaderSize]))
	assert.Equal(t, uint32(7), init.Major)
	assert.LessOrEqual(t, init.Minor, uint32(fuseMinor))
	assert.Equal(t, uint32(128<<10), init.MaxWrite)
	// Only flags the guest offered may come back.
	assert.Zero(t, init.Flags&^in.Flags)
}

// A guest that offers a newer, larger fuse_init_in must still be understood.
func TestDispatchInitAcceptsLongerRequest(t *testing.T) {
	s := newTestSession(t, newStubFS())
	in := fuse.InitIn{Major: 7, Minor: 38}
	request := asRequest(t, opInit, 0, &in, make([]byte, 64)...)

	reply := runOp(t, s, request, outHeaderSize+int(unsafe.Sizeof(fuse.InitOut{})))
	assert.Equal(t, int32(fuse.OK), replyHeader(t, reply).Status)
}

func TestDispatchInitRejectsWrongMajor(t *testing.T) {
	s := newTestSession(t, newStubFS())
	in := fuse.InitIn{Major: 6, Minor: 38}
	request := asRequest(t, opInit, 0, &in)

	reply := runOp(t, s, request, outHeaderSize+int(unsafe.Sizeof(fuse.InitOut{})))
	assert.NotEqual(t, int32(fuse.OK), replyHeader(t, reply).Status)
}

// Field offsets: a request read from the wrong base still "works" but carries
// garbage, so the values the filesystem sees are what matters.
func TestDispatchReadPassesThroughRequestFields(t *testing.T) {
	fs := newStubFS()
	fs.readData = []byte("juicefs-over-virtiofs")
	s := newTestSession(t, fs)

	in := fuse.ReadIn{Fh: 99, Offset: 4096, Size: 512}
	request := asRequest(t, opRead, 2, &in)

	reply := runOp(t, s, request, outHeaderSize+512)

	out := replyHeader(t, reply)
	require.Equal(t, int32(fuse.OK), out.Status)
	assert.Equal(t, uint64(2), fs.readNode, "nodeid reached the filesystem")
	assert.Equal(t, uint64(99), fs.readFh, "file handle reached the filesystem")
	assert.Equal(t, uint64(4096), fs.readOffset, "offset reached the filesystem")
	assert.Equal(t, uint32(512), fs.readSize)
	assert.Equal(t, fs.readData, reply[outHeaderSize:], "payload is the data read")
	assert.Equal(t, uint32(len(reply)), out.Length)
}

func TestDispatchOpenPassesThroughNodeID(t *testing.T) {
	fs := newStubFS()
	s := newTestSession(t, fs)

	in := fuse.OpenIn{Flags: 0}
	reply := runOp(t, s, asRequest(t, opOpen, 5, &in), outHeaderSize+int(unsafe.Sizeof(fuse.OpenOut{})))

	require.Equal(t, int32(fuse.OK), replyHeader(t, reply).Status)
	assert.Equal(t, uint64(5), fs.openNode)
	assert.Equal(t, fs.openFh, (*fuse.OpenOut)(unsafe.Pointer(&reply[outHeaderSize])).Fh)
}

func TestDispatchLookupReadsNameFromBody(t *testing.T) {
	fs := newStubFS()
	fs.readData = []byte("twenty-three bytes here")
	s := newTestSession(t, fs)

	var hdr fuse.InHeader
	request := asRequest(t, opLookup, 1, &hdr, append([]byte("greeting.txt"), 0)...)
	reply := runOp(t, s, request, outHeaderSize+int(unsafe.Sizeof(fuse.EntryOut{})))

	require.Equal(t, int32(fuse.OK), replyHeader(t, reply).Status)
	assert.Equal(t, uint64(1), fs.lookupNode)
	assert.Equal(t, "greeting.txt", fs.lookupName)

	// The size has to survive to the wire: a guest that reads zero treats every
	// offset as past the end of the file and returns a hole without ever asking
	// the backend for data.
	entry := (*fuse.EntryOut)(unsafe.Pointer(&reply[outHeaderSize]))
	assert.Equal(t, uint64(2), entry.NodeId)
	assert.Equal(t, uint64(len(fs.readData)), entry.Attr.Size, "file size reached the guest")
	assert.Equal(t, uint32(fuse.S_IFREG|0o444), entry.Attr.Mode)
}

// FUSE_FORGET carries no writable descriptor, so requiring a reply buffer would
// drop it and leak the guest's lookup count.
func TestDispatchForgetNeedsNoReplyBuffer(t *testing.T) {
	fs := newStubFS()
	s := newTestSession(t, fs)

	var hdr fuse.InHeader
	nlookup := make([]byte, 8)
	nlookup[0] = 3
	request := asRequest(t, opForget, 9, &hdr, nlookup...)

	reply, err := s.handleChain(chain{readable: [][]byte{request}})
	require.NoError(t, err, "a chain with no reply buffer is normal for forget")
	assert.Nil(t, reply)
	assert.Equal(t, uint64(9), fs.forgotNode)
	assert.Equal(t, uint64(3), fs.forgotLookup)
}

// Without a DAX window the guest must be told mapping is unsupported rather than
// left waiting.
func TestDispatchSetupMappingWithoutWindowIsRefused(t *testing.T) {
	s := newTestSession(t, newStubFS())

	var hdr fuse.InHeader
	body := make([]byte, int(unsafe.Sizeof(setupmappingIn{})))
	request := asRequest(t, opSetupmapping, 2, &hdr, body...)

	reply := runOp(t, s, request, outHeaderSize)
	assert.Equal(t, int32(fuse.ENOSYS), replyHeader(t, reply).Status)
}

// The guest sizes its reply buffer from its own struct definitions, so a reply
// larger than the kernel's would be truncated on the way out — silently zeroing
// trailing fields such as a file's size, which the guest then reads as a hole.
func TestReplyStructsMatchKernelSizes(t *testing.T) {
	assert.Equal(t, uintptr(16), unsafe.Sizeof(fuse.OutHeader{}), "fuse_out_header")
	assert.Equal(t, uintptr(128), unsafe.Sizeof(fuse.EntryOut{}), "fuse_entry_out")
	assert.Equal(t, uintptr(104), unsafe.Sizeof(fuse.AttrOut{}), "fuse_attr_out")
	assert.Equal(t, uintptr(16), unsafe.Sizeof(fuse.OpenOut{}), "fuse_open_out")
	assert.Equal(t, uintptr(64), unsafe.Sizeof(fuse.InitOut{}), "fuse_init_out")
	assert.Equal(t, uintptr(40), unsafe.Sizeof(fuse.InHeader{}), "fuse_in_header")
	assert.Equal(t, uintptr(40), unsafe.Sizeof(setupmappingIn{}), "fuse_setupmapping_in")
}

func TestDispatchUnknownOpcodeIsRefused(t *testing.T) {
	s := newTestSession(t, newStubFS())

	var hdr fuse.InHeader
	reply := runOp(t, s, asRequest(t, 4242, 1, &hdr), outHeaderSize)
	assert.Equal(t, int32(fuse.ENOSYS), replyHeader(t, reply).Status)
}
