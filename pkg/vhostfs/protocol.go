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
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

// Requests the frontend (the VMM) sends to us.
type frontendReq uint32

const (
	reqGetFeatures         frontendReq = 1
	reqSetFeatures         frontendReq = 2
	reqSetOwner            frontendReq = 3
	reqResetOwner          frontendReq = 4
	reqSetMemTable         frontendReq = 5
	reqSetVringNum         frontendReq = 8
	reqSetVringAddr        frontendReq = 9
	reqSetVringBase        frontendReq = 10
	reqGetVringBase        frontendReq = 11
	reqSetVringKick        frontendReq = 12
	reqSetVringCall        frontendReq = 13
	reqSetVringErr         frontendReq = 14
	reqGetProtocolFeatures frontendReq = 15
	reqSetProtocolFeatures frontendReq = 16
	reqGetQueueNum         frontendReq = 17
	reqSetVringEnable      frontendReq = 18
	reqSetBackendReqFd     frontendReq = 21
	reqGetConfig           frontendReq = 24
	reqSetConfig           frontendReq = 25
	reqGetMaxMemSlots      frontendReq = 36
	reqSetStatus           frontendReq = 39
	reqGetStatus           frontendReq = 40
)

// Requests we send to the frontend over the backend-request channel.
type backendReq uint32

const (
	backendReqShmemMap   backendReq = 9
	backendReqShmemUnmap backendReq = 10
)

// Header flag bits.
const (
	hdrVersion1  uint32 = 0x1
	hdrFlagReply uint32 = 0x4
	hdrFlagNeed  uint32 = 0x8
	hdrVersMask  uint32 = 0x3
)

// Protocol feature bits we care about.
const (
	protoFeatureMQ         uint64 = 0x1
	protoFeatureReplyAck   uint64 = 0x8
	protoFeatureBackendReq uint64 = 0x20
	protoFeatureConfig     uint64 = 0x200
)

// virtio feature bits. VERSION_1 is required; PROTOCOL_FEATURES is the
// vhost-user bit that lets the frontend negotiate the protocol features above.
const (
	virtioFVersion1        uint64 = 1 << 32
	vhostUserFProtoFeature uint64 = 1 << 30
)

const headerSize = 12

// maxPayload bounds a single message body so a malformed size cannot make us
// allocate unbounded memory.
const maxPayload = 1 << 20

type header struct {
	Request frontendReq
	Flags   uint32
	Size    uint32
}

func (h header) version() uint32 { return h.Flags & hdrVersMask }

func (h header) needsReply() bool { return h.Flags&hdrFlagNeed != 0 }

func encodeHeader(h header) []byte {
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(buf[0:], uint32(h.Request))
	binary.LittleEndian.PutUint32(buf[4:], h.Flags)
	binary.LittleEndian.PutUint32(buf[8:], h.Size)
	return buf
}

func decodeHeader(buf []byte) header {
	return header{
		Request: frontendReq(binary.LittleEndian.Uint32(buf[0:])),
		Flags:   binary.LittleEndian.Uint32(buf[4:]),
		Size:    binary.LittleEndian.Uint32(buf[8:]),
	}
}

// memoryRegion mirrors the frontend's per-region descriptor in SET_MEM_TABLE.
type memoryRegion struct {
	GuestPhysAddr uint64
	MemorySize    uint64
	UserAddr      uint64
	MmapOffset    uint64
}

const memoryRegionSize = 32

func decodeMemoryRegion(buf []byte) memoryRegion {
	return memoryRegion{
		GuestPhysAddr: binary.LittleEndian.Uint64(buf[0:]),
		MemorySize:    binary.LittleEndian.Uint64(buf[8:]),
		UserAddr:      binary.LittleEndian.Uint64(buf[16:]),
		MmapOffset:    binary.LittleEndian.Uint64(buf[24:]),
	}
}

// vringAddr mirrors the SET_VRING_ADDR payload. The ring addresses are
// frontend-process virtual addresses, translated through the memory table.
type vringAddr struct {
	Index      uint32
	Flags      uint32
	Descriptor uint64
	Used       uint64
	Available  uint64
	LogGuest   uint64
}

const vringAddrSize = 40

func decodeVringAddr(buf []byte) vringAddr {
	return vringAddr{
		Index:      binary.LittleEndian.Uint32(buf[0:]),
		Flags:      binary.LittleEndian.Uint32(buf[4:]),
		Descriptor: binary.LittleEndian.Uint64(buf[8:]),
		Used:       binary.LittleEndian.Uint64(buf[16:]),
		Available:  binary.LittleEndian.Uint64(buf[24:]),
		LogGuest:   binary.LittleEndian.Uint64(buf[32:]),
	}
}

// vringState is the {index, num} payload shared by several requests.
type vringState struct {
	Index uint32
	Num   uint32
}

func decodeVringState(buf []byte) vringState {
	return vringState{
		Index: binary.LittleEndian.Uint32(buf[0:]),
		Num:   binary.LittleEndian.Uint32(buf[4:]),
	}
}

func encodeVringState(s vringState) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:], s.Index)
	binary.LittleEndian.PutUint32(buf[4:], s.Num)
	return buf
}

// shmemMapMsg is the SHMEM_MAP/SHMEM_UNMAP payload. An unmap uses only Shmid,
// ShmOffset and Len.
type shmemMapMsg struct {
	Shmid     uint8
	FdOffset  uint64
	ShmOffset uint64
	Len       uint64
	Flags     uint64
}

// Protection bits in shmemMapMsg.Flags.
const (
	shmemMapR uint64 = 0x1
	shmemMapW uint64 = 0x2
)

const shmemMapMsgSize = 40

func encodeShmemMap(m shmemMapMsg) []byte {
	buf := make([]byte, shmemMapMsgSize)
	buf[0] = m.Shmid
	// bytes 1..7 are padding
	binary.LittleEndian.PutUint64(buf[8:], m.FdOffset)
	binary.LittleEndian.PutUint64(buf[16:], m.ShmOffset)
	binary.LittleEndian.PutUint64(buf[24:], m.Len)
	binary.LittleEndian.PutUint64(buf[32:], m.Flags)
	return buf
}

// conn is a vhost-user connection: length-prefixed messages over a Unix socket,
// with file descriptors passed as SCM_RIGHTS ancillary data.
type conn struct {
	sock *net.UnixConn
}

// recv reads one message plus any attached fds. The fds are returned as files so
// their lifetime is tied to the garbage collector rather than a bare int.
func (c *conn) recv() (header, []byte, []*os.File, error) {
	hdrBuf := make([]byte, headerSize)
	oob := make([]byte, syscall.CmsgSpace(8*4))
	n, oobn, _, _, err := c.sock.ReadMsgUnix(hdrBuf, oob)
	if err != nil {
		return header{}, nil, nil, err
	}
	if n == 0 && oobn == 0 {
		return header{}, nil, nil, fmt.Errorf("connection closed")
	}
	if n != headerSize {
		return header{}, nil, nil, fmt.Errorf("short header: %d bytes", n)
	}
	files, err := parseFds(oob[:oobn])
	if err != nil {
		return header{}, nil, nil, err
	}
	hdr := decodeHeader(hdrBuf)
	if hdr.version() != hdrVersion1 {
		closeFiles(files)
		return header{}, nil, nil, fmt.Errorf("unsupported message version %d", hdr.version())
	}
	if hdr.Size > maxPayload {
		closeFiles(files)
		return header{}, nil, nil, fmt.Errorf("message body too large: %d", hdr.Size)
	}

	var body []byte
	if hdr.Size > 0 {
		body = make([]byte, hdr.Size)
		read := 0
		for read < len(body) {
			// The body carries no fds of its own, so a plain read is enough here.
			m, err := c.sock.Read(body[read:])
			if err != nil {
				closeFiles(files)
				return header{}, nil, nil, err
			}
			if m == 0 {
				closeFiles(files)
				return header{}, nil, nil, fmt.Errorf("connection closed mid-body")
			}
			read += m
		}
	}
	return hdr, body, files, nil
}

// reply answers a request, echoing its type with the REPLY flag set.
func (c *conn) reply(req frontendReq, body []byte) error {
	hdr := header{Request: req, Flags: hdrVersion1 | hdrFlagReply, Size: uint32(len(body))}
	return c.send(encodeHeader(hdr), body, nil)
}

func (c *conn) replyU64(req frontendReq, value uint64) error {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, value)
	return c.reply(req, body)
}

// sendBackendReq sends a backend-initiated request, optionally with one fd.
func (c *conn) sendBackendReq(req backendReq, body []byte, fd int, needReply bool) error {
	flags := hdrVersion1
	if needReply {
		flags |= hdrFlagNeed
	}
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(buf[0:], uint32(req))
	binary.LittleEndian.PutUint32(buf[4:], flags)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(body)))
	var fds []int
	if fd >= 0 {
		fds = []int{fd}
	}
	return c.send(buf, body, fds)
}

// recvAckWithin reads a reply to a backend-initiated request, giving up after
// timeout so a caller is never parked on an unresponsive frontend.
func (c *conn) recvAckWithin(timeout time.Duration) (uint64, error) {
	if err := c.sock.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return 0, err
	}
	// Clearing the deadline keeps it from leaking onto the next request.
	defer func() { _ = c.sock.SetReadDeadline(time.Time{}) }()
	return c.recvAck()
}

// recvAck reads a reply to a backend-initiated request and reports the payload.
func (c *conn) recvAck() (uint64, error) {
	hdr, body, files, err := c.recv()
	if err != nil {
		return 0, err
	}
	closeFiles(files)
	if hdr.Flags&hdrFlagReply == 0 {
		return 0, fmt.Errorf("expected a reply, got flags %#x", hdr.Flags)
	}
	if len(body) < 8 {
		return 0, fmt.Errorf("short reply body: %d bytes", len(body))
	}
	return binary.LittleEndian.Uint64(body), nil
}

func (c *conn) send(hdr, body []byte, fds []int) error {
	var oob []byte
	if len(fds) > 0 {
		oob = syscall.UnixRights(fds...)
	}
	// Header and body go in one message so a concurrent sender cannot interleave
	// between them.
	msg := append(append([]byte(nil), hdr...), body...)
	_, _, err := c.sock.WriteMsgUnix(msg, oob, nil)
	return err
}

func (c *conn) close() error { return c.sock.Close() }

func parseFds(oob []byte) ([]*os.File, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	var files []*os.File
	for _, m := range msgs {
		fds, err := syscall.ParseUnixRights(&m)
		if err != nil {
			closeFiles(files)
			return nil, err
		}
		for _, fd := range fds {
			syscall.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "vhost-user-fd"))
		}
	}
	return files, nil
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}
