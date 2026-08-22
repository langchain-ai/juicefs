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

func TestHeaderRoundTrip(t *testing.T) {
	in := header{Request: reqSetMemTable, Flags: hdrVersion1 | hdrFlagNeed, Size: 1234}
	out := decodeHeader(encodeHeader(in))
	assert.Equal(t, in, out)
	assert.Equal(t, hdrVersion1, out.version())
	assert.True(t, out.needsReply())

	noReply := decodeHeader(encodeHeader(header{Request: reqGetFeatures, Flags: hdrVersion1}))
	assert.False(t, noReply.needsReply())
}

// The frontend decodes this payload as a packed C struct, so every field has to
// land at the offset it expects.
func TestEncodeShmemMapFieldOffsets(t *testing.T) {
	buf := encodeShmemMap(shmemMapMsg{
		Shmid:     1,
		FdOffset:  0x1122,
		ShmOffset: 0x3344,
		Len:       0x5566,
		Flags:     shmemMapR,
	})
	require.Len(t, buf, shmemMapMsgSize)
	assert.Equal(t, byte(1), buf[0])
	assert.Equal(t, uint64(0x1122), binary.LittleEndian.Uint64(buf[8:]))
	assert.Equal(t, uint64(0x3344), binary.LittleEndian.Uint64(buf[16:]))
	assert.Equal(t, uint64(0x5566), binary.LittleEndian.Uint64(buf[24:]))
	assert.Equal(t, shmemMapR, binary.LittleEndian.Uint64(buf[32:]))
	// Bytes 1..7 are padding and must stay zero.
	assert.Equal(t, make([]byte, 7), buf[1:8])
}

func TestDecodeMemoryRegionAndVringPayloads(t *testing.T) {
	buf := make([]byte, memoryRegionSize)
	binary.LittleEndian.PutUint64(buf[0:], 0xa000)
	binary.LittleEndian.PutUint64(buf[8:], 0x1000)
	binary.LittleEndian.PutUint64(buf[16:], 0x7f00)
	binary.LittleEndian.PutUint64(buf[24:], 0x40)
	assert.Equal(t, memoryRegion{
		GuestPhysAddr: 0xa000,
		MemorySize:    0x1000,
		UserAddr:      0x7f00,
		MmapOffset:    0x40,
	}, decodeMemoryRegion(buf))

	addr := make([]byte, vringAddrSize)
	binary.LittleEndian.PutUint32(addr[0:], 1)
	binary.LittleEndian.PutUint64(addr[8:], 0xd000)
	binary.LittleEndian.PutUint64(addr[16:], 0xe000)
	assert.Equal(t, uint32(1), decodeVringAddr(addr).Index)
	assert.Equal(t, uint64(0xd000), decodeVringAddr(addr).Descriptor)
	assert.Equal(t, uint64(0xe000), decodeVringAddr(addr).Used)

	st := decodeVringState(encodeVringState(vringState{Index: 1, Num: 256}))
	assert.Equal(t, vringState{Index: 1, Num: 256}, st)
}

// dirEntryListBytes reads a field go-fuse does not export. This fails loudly if
// that struct is ever reordered.
func TestDirEntryListBytes(t *testing.T) {
	buf := make([]byte, 4096)
	list := fuse.NewDirEntryList(buf, 0)
	require.True(t, list.AddDirEntry(fuse.DirEntry{Name: "hello", Ino: 42, Mode: fuse.S_IFREG}))

	got := dirEntryListBytes(list)
	require.NotEmpty(t, got, "recovered no bytes; go-fuse's DirEntryList layout changed")
	// A serialized dirent is {ino, off, namelen, type} then the name.
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(got[0:]))
	assert.Equal(t, uint32(len("hello")), binary.LittleEndian.Uint32(got[16:]))
	assert.Contains(t, string(got), "hello")
}

func TestEncodeReply(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	buf := encodeReply(7, fuse.OK, payload)
	require.Len(t, buf, outHeaderSize+len(payload))
	out := (*fuse.OutHeader)(unsafe.Pointer(&buf[0]))
	assert.Equal(t, uint64(7), out.Unique)
	assert.Equal(t, int32(0), out.Status)
	assert.Equal(t, uint32(len(buf)), out.Length)
	assert.Equal(t, payload, buf[outHeaderSize:])

	// An error reply drops the payload but keeps a consistent length.
	errBuf := encodeReply(9, fuse.ENOENT, payload)
	require.Len(t, errBuf, outHeaderSize)
	errOut := (*fuse.OutHeader)(unsafe.Pointer(&errBuf[0]))
	assert.Equal(t, int32(fuse.ENOENT), errOut.Status)
	assert.Equal(t, uint32(outHeaderSize), errOut.Length)

	// FUSE_FORGET and friends expect no reply at all.
	assert.Nil(t, encodeReply(11, statusNoReply, nil))
}

func TestScatterAcrossDescriptors(t *testing.T) {
	a := make([]byte, 3)
	b := make([]byte, 5)
	src := []byte("abcdefg")
	assert.Equal(t, uint32(7), scatter([][]byte{a, b}, src))
	assert.Equal(t, "abc", string(a))
	assert.Equal(t, "defg", string(b[:4]))

	// A reply larger than the guest's buffers is truncated, not written past them.
	small := make([]byte, 2)
	assert.Equal(t, uint32(2), scatter([][]byte{small}, src))
	assert.Equal(t, "ab", string(small))
}

func TestFlattenReadableDescriptors(t *testing.T) {
	single := []byte("one")
	assert.Equal(t, single, flatten([][]byte{single}))
	assert.Equal(t, []byte("onetwo"), flatten([][]byte{[]byte("one"), []byte("two")}))
}

func TestCstring(t *testing.T) {
	assert.Equal(t, "name", cstring([]byte("name\x00trailing")))
	assert.Equal(t, "", cstring([]byte("no-terminator")))
}
