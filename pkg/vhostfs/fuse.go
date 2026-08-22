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
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// FUSE opcodes. go-fuse keeps its own copies unexported, so the ones this
// transport dispatches are named here.
const (
	opLookup        uint32 = 1
	opForget        uint32 = 2
	opGetattr       uint32 = 3
	opReadlink      uint32 = 5
	opOpen          uint32 = 14
	opRead          uint32 = 15
	opStatfs        uint32 = 17
	opRelease       uint32 = 18
	opInit          uint32 = 26
	opOpendir       uint32 = 27
	opReaddir       uint32 = 28
	opReleasedir    uint32 = 29
	opDestroy       uint32 = 38
	opReaddirplus   uint32 = 44
	opSetupmapping  uint32 = 48
	opRemovemapping uint32 = 49
)

// Kernel-facing protocol version. 7.31 is the minimum that carries
// SETUPMAPPING, which is what DAX rides on.
const (
	fuseMajor = 7
	fuseMinor = 31
)

// fuseMapAlignment is FUSE_MAP_ALIGNMENT: the guest reads init_out.map_alignment
// only when this flag comes back set.
const fuseMapAlignment uint32 = 1 << 26

// setupmappingIn is the FUSE_SETUPMAPPING request body.
type setupmappingIn struct {
	Fh      uint64
	Foffset uint64
	Len     uint64
	Flags   uint64
	Moffset uint64
}

// Flag bits in setupmappingIn.Flags.
const (
	setupmappingWrite uint64 = 1 << 0
	setupmappingRead  uint64 = 1 << 1
)

// removemappingIn is the FUSE_REMOVEMAPPING request body: a count followed by
// that many removemappingOne entries.
type removemappingIn struct {
	Count uint32
}

type removemappingOne struct {
	Moffset uint64
	Len     uint64
}

const (
	outHeaderSize = int(unsafe.Sizeof(fuse.OutHeader{}))
	inHeaderSize  = int(unsafe.Sizeof(fuse.InHeader{}))
)

// request is one decoded FUSE request taken off a descriptor chain.
type request struct {
	hdr *fuse.InHeader
	// in is the request bytes after the header.
	in []byte
	// out is the reply buffer, starting at its fuse_out_header.
	out []byte
}

// flatten copies a chain's readable descriptors into one buffer. virtio-fs
// usually puts the whole request in a single descriptor, but the spec permits a
// split, and a copy keeps every opcode decoder free of that concern.
func flatten(bufs [][]byte) []byte {
	if len(bufs) == 1 {
		return bufs[0]
	}
	var out []byte
	for _, b := range bufs {
		out = append(out, b...)
	}
	return out
}

// scatter writes src across a chain's writable descriptors, returning the byte
// count written.
func scatter(dst [][]byte, src []byte) uint32 {
	written := 0
	for _, d := range dst {
		if written >= len(src) {
			break
		}
		n := copy(d, src[written:])
		written += n
	}
	return uint32(written)
}

// writableLen is the total space the guest offered for the reply.
func writableLen(bufs [][]byte) int {
	total := 0
	for _, b := range bufs {
		total += len(b)
	}
	return total
}

// handleChain decodes one chain, dispatches it, and returns the reply bytes. A
// panic in the filesystem is turned into an EIO reply: one backend serves every VM
// on the host, so one bad request must not take the others down with it.
func (s *session) handleChain(c chain) (reply []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("vhost-user-fs: panic serving request: %v\n%s", r, debug.Stack())
			reply, err = nil, fmt.Errorf("panic serving request: %v", r)
		}
	}()
	return s.handleChainLocked(c)
}

func (s *session) handleChainLocked(c chain) ([]byte, error) {
	in := flatten(c.readable)
	if len(in) < inHeaderSize {
		return nil, fmt.Errorf("request is shorter than a fuse header: %d bytes", len(in))
	}
	hdr := (*fuse.InHeader)(unsafe.Pointer(&in[0]))
	body := in[inHeaderSize:]
	// FUSE_FORGET and the hiprio queue's requests carry no writable descriptor at
	// all, so an absent reply buffer is normal rather than malformed.
	replyCap := writableLen(c.writable)
	payloadCap := 0
	if replyCap > outHeaderSize {
		payloadCap = replyCap - outHeaderSize
	}

	payload, status := s.dispatch(in, hdr, body, payloadCap)
	if replyCap < outHeaderSize {
		if status != statusNoReply {
			return nil, fmt.Errorf("opcode %d needs a reply but the guest offered %d bytes", hdr.Opcode, replyCap)
		}
		return nil, nil
	}
	return encodeReply(hdr.Unique, status, payload), nil
}

// dispatch runs one opcode, returning the reply payload (excluding the out
// header) and its status.
func (s *session) dispatch(in []byte, hdr *fuse.InHeader, body []byte, replyCap int) ([]byte, fuse.Status) {
	cancel := make(chan struct{})
	switch hdr.Opcode {
	case opInit:
		return s.doInit(in)

	case opDestroy:
		return nil, fuse.OK

	case opForget:
		if len(body) < 8 {
			return nil, fuse.EINVAL
		}
		nlookup := binary.LittleEndian.Uint64(body)
		s.srv.fs.Forget(hdr.NodeId, nlookup)
		// FUSE_FORGET carries no reply.
		return nil, statusNoReply

	case opLookup:
		name := cstring(body)
		if name == "" {
			return nil, fuse.EINVAL
		}
		var out fuse.EntryOut
		if st := s.srv.fs.Lookup(cancel, hdr, name, &out); st != fuse.OK {
			return nil, st
		}
		return asBytes(&out), fuse.OK

	case opGetattr:
		input, ok := castIn[fuse.GetAttrIn](in)
		if !ok {
			return nil, fuse.EINVAL
		}
		var out fuse.AttrOut
		if st := s.srv.fs.GetAttr(cancel, input, &out); st != fuse.OK {
			return nil, st
		}
		return asBytes(&out), fuse.OK

	case opReadlink:
		target, st := s.srv.fs.Readlink(cancel, hdr)
		if st != fuse.OK {
			return nil, st
		}
		return target, fuse.OK

	case opStatfs:
		var out fuse.StatfsOut
		if st := s.srv.fs.StatFs(cancel, hdr, &out); st != fuse.OK {
			return nil, st
		}
		return asBytes(&out), fuse.OK

	case opOpen, opOpendir:
		input, ok := castIn[fuse.OpenIn](in)
		if !ok {
			return nil, fuse.EINVAL
		}
		var out fuse.OpenOut
		var st fuse.Status
		if hdr.Opcode == opOpen {
			st = s.srv.fs.Open(cancel, input, &out)
		} else {
			st = s.srv.fs.OpenDir(cancel, input, &out)
		}
		if st != fuse.OK {
			return nil, st
		}
		return asBytes(&out), fuse.OK

	case opRelease, opReleasedir:
		input, ok := castIn[fuse.ReleaseIn](in)
		if !ok {
			return nil, fuse.EINVAL
		}
		if hdr.Opcode == opRelease {
			s.srv.fs.Release(cancel, input)
		} else {
			s.srv.fs.ReleaseDir(input)
		}
		return nil, fuse.OK

	case opRead:
		input, ok := castIn[fuse.ReadIn](in)
		if !ok {
			return nil, fuse.EINVAL
		}
		size := int(input.Size)
		if size > replyCap {
			size = replyCap
		}
		buf := make([]byte, size)
		res, st := s.srv.fs.Read(cancel, input, buf)
		if st != fuse.OK {
			return nil, st
		}
		data, st := res.Bytes(buf)
		if st != fuse.OK {
			return nil, st
		}
		return data, fuse.OK

	case opReaddir, opReaddirplus:
		input, ok := castIn[fuse.ReadIn](in)
		if !ok {
			return nil, fuse.EINVAL
		}
		size := int(input.Size)
		if size > replyCap {
			size = replyCap
		}
		buf := make([]byte, size)
		list := fuse.NewDirEntryList(buf, uint64(input.Offset))
		var st fuse.Status
		if hdr.Opcode == opReaddir {
			st = s.srv.fs.ReadDir(cancel, input, list)
		} else {
			st = s.srv.fs.ReadDirPlus(cancel, input, list)
		}
		if st != fuse.OK {
			return nil, st
		}
		return dirEntryListBytes(list), fuse.OK

	case opSetupmapping:
		return s.doSetupMapping(cancel, hdr, body)

	case opRemovemapping:
		return s.doRemoveMapping(body)

	default:
		return nil, fuse.ENOSYS
	}
}

// statusNoReply marks an opcode the kernel expects no answer to. It is not a FUSE
// status; the caller checks for it before writing anything back.
const statusNoReply = fuse.Status(-1)

func (s *session) doInit(request []byte) ([]byte, fuse.Status) {
	in, ok := castIn[fuse.InitIn](request)
	if !ok {
		return nil, fuse.EINVAL
	}
	if in.Major != fuseMajor {
		return nil, fuse.Status(syscall.EPROTO)
	}
	minor := in.Minor
	if minor > fuseMinor {
		minor = fuseMinor
	}
	// Only flags the guest offered may be enabled.
	flags := in.Flags & (fuse.CAP_BIG_WRITES | fuse.CAP_ASYNC_READ | fuse.CAP_READDIRPLUS)
	out := fuse.InitOut{
		Major:               fuseMajor,
		Minor:               minor,
		MaxReadAhead:        in.MaxReadAhead,
		MaxWrite:            uint32(s.srv.maxWrite),
		MaxBackground:       64,
		CongestionThreshold: 48,
	}
	if s.window != nil && in.Flags&fuseMapAlignment != 0 {
		// The guest only reads map_alignment when this flag is set, and it aligns
		// SETUPMAPPING to 1<<map_alignment. go-fuse calls the field Padding, but the
		// kernel's fuse_init_out has map_alignment at exactly that offset, right
		// after max_pages.
		flags |= fuseMapAlignment
		out.Padding = uint16(daxMapAlignmentShift)
	}
	out.Flags = flags
	logger.Debugf("vhost-user-fs: init in major=%d minor=%d flags=%#x -> out minor=%d flags=%#x max_write=%d align=%d",
		in.Major, in.Minor, in.Flags, out.Minor, out.Flags, out.MaxWrite, out.Padding)
	return asBytes(&out), fuse.OK
}

func (s *session) doSetupMapping(cancel <-chan struct{}, hdr *fuse.InHeader, body []byte) ([]byte, fuse.Status) {
	if s.window == nil {
		return nil, fuse.ENOSYS
	}
	if len(body) < int(unsafe.Sizeof(setupmappingIn{})) {
		return nil, fuse.EINVAL
	}
	in := (*setupmappingIn)(unsafe.Pointer(&body[0]))
	st := s.window.setup(cancel, hdr, in)
	if st != fuse.OK {
		return nil, st
	}
	return nil, fuse.OK
}

func (s *session) doRemoveMapping(body []byte) ([]byte, fuse.Status) {
	if s.window == nil {
		return nil, fuse.ENOSYS
	}
	if len(body) < int(unsafe.Sizeof(removemappingIn{})) {
		return nil, fuse.EINVAL
	}
	count := int((*removemappingIn)(unsafe.Pointer(&body[0])).Count)
	entries := body[unsafe.Sizeof(removemappingIn{}):]
	one := int(unsafe.Sizeof(removemappingOne{}))
	if len(entries) < count*one {
		return nil, fuse.EINVAL
	}
	for i := 0; i < count; i++ {
		e := (*removemappingOne)(unsafe.Pointer(&entries[i*one]))
		if st := s.window.remove(e.Moffset, e.Len); st != fuse.OK {
			return nil, st
		}
	}
	return nil, fuse.OK
}

// encodeReply frames a reply: out header followed by its payload.
func encodeReply(unique uint64, status fuse.Status, payload []byte) []byte {
	if status == statusNoReply {
		return nil
	}
	buf := make([]byte, outHeaderSize+len(payload))
	out := (*fuse.OutHeader)(unsafe.Pointer(&buf[0]))
	out.Unique = unique
	out.Status = int32(status)
	if status != fuse.OK {
		// An error reply carries no payload, whatever the handler produced.
		buf = buf[:outHeaderSize]
	} else {
		copy(buf[outHeaderSize:], payload)
	}
	out.Length = uint32(len(buf))
	return buf
}

// dirEntryListBytes recovers the serialized directory entries from a filled
// DirEntryList. go-fuse keeps the buffer unexported and only its own /dev/fuse
// server can read it, so the leading slice field is read directly.
// TestDirEntryListBytes fails if that layout ever changes.
func dirEntryListBytes(l *fuse.DirEntryList) []byte {
	return *(*[]byte)(unsafe.Pointer(l))
}

// castIn reinterprets a whole request as its fixed-size input struct. go-fuse's
// input types embed InHeader, so they describe the request from its first byte,
// not the body after the header. It reports false when the guest sent fewer bytes
// than the struct needs.
func castIn[T any](in []byte) (*T, bool) {
	var zero T
	if len(in) < int(unsafe.Sizeof(zero)) {
		return nil, false
	}
	return (*T)(unsafe.Pointer(&in[0])), true
}

// asBytes exposes a fixed-size reply struct as bytes without copying it.
func asBytes[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

// cstring reads the NUL-terminated name at the start of a request body.
func cstring(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		return string(b[:i])
	}
	return ""
}

// errnoToStatus keeps syscall errors on the FUSE status path.
func errnoToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	if errno, ok := err.(syscall.Errno); ok {
		return fuse.Status(errno)
	}
	return fuse.EIO
}
