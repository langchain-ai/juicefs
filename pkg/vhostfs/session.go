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
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// session is one VMM connection: its guest memory, its virtqueues, and its view
// of the DAX window. The filesystem and the DAX chunk cache are shared with every
// other session on the same Server, which is what lets two VMs reading the same
// file share one copy of its pages.
type session struct {
	srv *Server
	c   *conn

	mu     sync.Mutex
	mem    *guestMemory
	vrings [numQueues]vring
	sender *backendSender
	window *daxWindow

	features uint64
	protocol uint64

	closed atomic.Bool
	// kickLoops guards against starting a second drain loop for a queue the
	// frontend enables more than once.
	kickLoops [numQueues]sync.Once
}

func newSession(srv *Server, c *conn) *session {
	s := &session{srv: srv, c: c}
	for i := range s.vrings {
		s.vrings[i].index = uint16(i)
	}
	return s
}

// serve runs the control channel until the frontend disconnects.
func (s *session) serve() error {
	defer s.close()
	for {
		hdr, body, files, err := s.c.recv()
		if err != nil {
			closeFiles(files)
			return err
		}
		if err := s.handleRequest(hdr, body, files); err != nil {
			closeFiles(files)
			return err
		}
	}
}

func (s *session) close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	_ = s.c.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.window != nil {
		s.window.close()
		s.window = nil
	}
	if s.mem != nil {
		s.mem.unmap()
		s.mem = nil
	}
	for i := range s.vrings {
		if s.vrings[i].kick != nil {
			_ = s.vrings[i].kick.Close()
		}
		if s.vrings[i].call != nil {
			_ = s.vrings[i].call.Close()
		}
		s.vrings[i] = vring{index: uint16(i)}
	}
	s.sender = nil
}

func (s *session) handleRequest(hdr header, body []byte, files []*os.File) error {
	// Every branch either consumes the files or leaves them to be closed here.
	consumed := false
	defer func() {
		if !consumed {
			closeFiles(files)
		}
	}()

	switch hdr.Request {
	case reqGetFeatures:
		return s.c.replyU64(hdr.Request, virtioFVersion1|vhostUserFProtoFeature)

	case reqSetFeatures:
		value, err := payloadU64(hdr, body)
		if err != nil {
			return err
		}
		s.features = value
		return s.ackIfNeeded(hdr, 0)

	case reqGetProtocolFeatures:
		features := protoFeatureMQ | protoFeatureReplyAck
		if s.srv.cfg.EnableDax {
			features |= protoFeatureBackendReq
		}
		return s.c.replyU64(hdr.Request, features)

	case reqSetProtocolFeatures:
		value, err := payloadU64(hdr, body)
		if err != nil {
			return err
		}
		s.protocol = value
		return s.ackIfNeeded(hdr, 0)

	case reqGetQueueNum:
		return s.c.replyU64(hdr.Request, numQueues)

	case reqSetOwner, reqResetOwner, reqSetStatus:
		return s.ackIfNeeded(hdr, 0)

	case reqGetMaxMemSlots:
		return s.c.replyU64(hdr.Request, maxMemRegions)

	case reqGetStatus:
		return s.c.replyU64(hdr.Request, 0)

	case reqSetMemTable:
		consumed = true
		if err := s.setMemTable(body, files); err != nil {
			return err
		}
		logger.Debugf("vhost-user-fs: mem table set (%d fds)", len(files))
		return s.ackIfNeeded(hdr, 0)

	case reqSetVringNum:
		st, err := payloadVringState(hdr, body)
		if err != nil {
			return err
		}
		v, err := s.vring(st.Index)
		if err != nil {
			return err
		}
		s.mu.Lock()
		v.size = uint16(st.Num)
		s.mu.Unlock()
		return s.ackIfNeeded(hdr, 0)

	case reqSetVringAddr:
		if len(body) < vringAddrSize {
			return fmt.Errorf("SET_VRING_ADDR body is %d bytes", len(body))
		}
		if err := s.setVringAddr(decodeVringAddr(body)); err != nil {
			return err
		}
		return s.ackIfNeeded(hdr, 0)

	case reqSetVringBase:
		st, err := payloadVringState(hdr, body)
		if err != nil {
			return err
		}
		v, err := s.vring(st.Index)
		if err != nil {
			return err
		}
		s.mu.Lock()
		v.lastAvail = uint16(st.Num)
		s.mu.Unlock()
		return s.ackIfNeeded(hdr, 0)

	case reqGetVringBase:
		st, err := payloadVringState(hdr, body)
		if err != nil {
			return err
		}
		v, err := s.vring(st.Index)
		if err != nil {
			return err
		}
		s.mu.Lock()
		v.enabled = false
		last := v.lastAvail
		s.mu.Unlock()
		return s.c.reply(hdr.Request, encodeVringState(vringState{Index: st.Index, Num: uint32(last)}))

	case reqSetVringKick, reqSetVringCall, reqSetVringErr:
		consumed = true
		if err := s.setVringFd(hdr.Request, body, files); err != nil {
			return err
		}
		return s.ackIfNeeded(hdr, 0)

	case reqSetVringEnable:
		st, err := payloadVringState(hdr, body)
		if err != nil {
			return err
		}
		v, err := s.vring(st.Index)
		if err != nil {
			return err
		}
		s.mu.Lock()
		v.enabled = st.Num == 1
		enabled := v.enabled
		s.mu.Unlock()
		// Both queues need draining: the hiprio queue carries forgets and
		// interrupts, and a queue nobody drains eventually stalls the guest.
		logger.Debugf("vhost-user-fs: vring %d enabled=%t", st.Index, enabled)
		if enabled {
			s.startKickLoop(uint16(st.Index))
		}
		return s.ackIfNeeded(hdr, 0)

	case reqSetBackendReqFd:
		if len(files) != 1 {
			return fmt.Errorf("SET_BACKEND_REQ_FD carried %d fds", len(files))
		}
		consumed = true
		if err := s.setBackendReqFd(files[0]); err != nil {
			return err
		}
		return s.ackIfNeeded(hdr, 0)

	default:
		// An unknown request must not be dropped silently: the frontend may be
		// waiting on a reply, and guessing at the payload is worse than failing.
		logger.Warnf("vhost-user-fs: unsupported request %d", hdr.Request)
		return s.ackIfNeeded(hdr, ^uint64(0))
	}
}

// ackIfNeeded answers a request when REPLY_ACK is negotiated and the frontend set
// NEED_REPLY.
func (s *session) ackIfNeeded(hdr header, value uint64) error {
	if s.protocol&protoFeatureReplyAck == 0 || !hdr.needsReply() {
		return nil
	}
	return s.c.replyU64(hdr.Request, value)
}

func (s *session) vring(index uint32) (*vring, error) {
	if index >= numQueues {
		return nil, fmt.Errorf("vring index %d out of range", index)
	}
	return &s.vrings[index], nil
}

func (s *session) setMemTable(body []byte, files []*os.File) error {
	defer closeFiles(files)
	if len(body) < 8 {
		return fmt.Errorf("SET_MEM_TABLE body is %d bytes", len(body))
	}
	count := int(binary.LittleEndian.Uint32(body))
	if count == 0 || count > maxMemRegions {
		return fmt.Errorf("SET_MEM_TABLE declares %d regions", count)
	}
	// 8 bytes of {num_regions, padding} precede the region array.
	regionBytes := body[8:]
	if len(regionBytes) < count*memoryRegionSize {
		return fmt.Errorf("SET_MEM_TABLE holds %d bytes for %d regions", len(regionBytes), count)
	}
	regions := make([]memoryRegion, count)
	for i := range regions {
		regions[i] = decodeMemoryRegion(regionBytes[i*memoryRegionSize:])
	}
	mem, err := mapGuestMemory(regions, files)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem != nil {
		s.mem.unmap()
	}
	s.mem = mem
	return nil
}

func (s *session) setVringAddr(addr vringAddr) error {
	v, err := s.vring(addr.Index)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		return errors.New("SET_VRING_ADDR before SET_MEM_TABLE")
	}
	if v.size == 0 {
		return errors.New("SET_VRING_ADDR before SET_VRING_NUM")
	}
	size := uint64(v.size)
	// Split-ring layout: 16 bytes per descriptor; the available and used rings each
	// carry a 4-byte header, their entries, and a 2-byte event field.
	desc, err := s.mem.sliceUserAddr(addr.Descriptor, size*descSize)
	if err != nil {
		return fmt.Errorf("descriptor table: %w", err)
	}
	avail, err := s.mem.sliceUserAddr(addr.Available, 6+size*2)
	if err != nil {
		return fmt.Errorf("available ring: %w", err)
	}
	used, err := s.mem.sliceUserAddr(addr.Used, 6+size*8)
	if err != nil {
		return fmt.Errorf("used ring: %w", err)
	}
	v.descTable, v.avail, v.used = desc, avail, used
	return nil
}

func (s *session) setVringFd(req frontendReq, body []byte, files []*os.File) error {
	if len(body) < 8 {
		closeFiles(files)
		return fmt.Errorf("vring fd request body is %d bytes", len(body))
	}
	// The index is the low byte; bit 8 set means no fd is attached.
	value := binary.LittleEndian.Uint64(body)
	v, err := s.vring(uint32(value & 0xff))
	if err != nil {
		closeFiles(files)
		return err
	}
	var file *os.File
	if value&0x100 == 0 && len(files) == 1 {
		file = files[0]
	} else {
		closeFiles(files)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch req {
	case reqSetVringKick:
		if v.kick != nil {
			_ = v.kick.Close()
		}
		v.kick = file
	case reqSetVringCall:
		if v.call != nil {
			_ = v.call.Close()
		}
		v.call = file
	default:
		// SET_VRING_ERR is accepted so the frontend's setup completes, but errors go
		// back through the FUSE reply status rather than this fd.
		if file != nil {
			_ = file.Close()
		}
	}
	return nil
}

func (s *session) setBackendReqFd(file *os.File) error {
	sock, err := net.FileConn(file)
	// FileConn dups the fd, so ours is redundant either way.
	_ = file.Close()
	if err != nil {
		return fmt.Errorf("backend request channel: %w", err)
	}
	unixSock, ok := sock.(*net.UnixConn)
	if !ok {
		_ = sock.Close()
		return fmt.Errorf("backend request channel is a %T, not a unix socket", sock)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sender = &backendSender{c: &conn{sock: unixSock}}
	if s.srv.cfg.EnableDax {
		s.window = newDaxWindow(s.srv.chunks, s.sender)
	}
	return nil
}

func (s *session) startKickLoop(index uint16) {
	if index >= numQueues {
		return
	}
	s.kickLoops[index].Do(func() { go s.kickLoop(index) })
}

// kickLoop drains one queue whenever the guest kicks it.
//
// The frontend hands over an eventfd it created non-blocking, so a read finds
// EAGAIN whenever the loop comes back around before the guest kicks again. Fd()
// detaches it from the runtime poller and puts it back in blocking mode, which is
// what makes the wait below actually wait; treating EAGAIN as fatal would retire
// the loop after the first burst of requests and leave every later kick — the
// guest's first read among them — unanswered forever.
func (s *session) kickLoop(index uint16) {
	s.mu.Lock()
	kick := s.vrings[index].kick
	s.mu.Unlock()
	if kick == nil {
		return
	}
	fd := int(kick.Fd())

	var buf [8]byte
	for {
		if s.closed.Load() {
			return
		}
		switch _, err := unix.Read(fd, buf[:]); err {
		case nil:
			s.drainQueue(index)
		case unix.EINTR, unix.EAGAIN:
			// Not a kick: wait for the fd to become readable and look again.
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			if _, err := unix.Poll(fds, kickPollTimeoutMs); err != nil && err != unix.EINTR {
				if !s.closed.Load() {
					logger.Warnf("vhost-user-fs: poll failed on queue %d: %s", index, err)
				}
				return
			}
		default:
			if !s.closed.Load() && !errors.Is(err, os.ErrClosed) {
				logger.Warnf("vhost-user-fs: kick read failed on queue %d: %s", index, err)
			}
			return
		}
	}
}

// kickPollTimeoutMs bounds the wait so a closed session is noticed even if the
// guest never kicks again.
const kickPollTimeoutMs = 1000

// drainQueue processes every chain the guest has made available.
func (s *session) drainQueue(index uint16) {
	s.mu.Lock()
	v := &s.vrings[index]
	mem := s.mem
	ready := v.ready() && mem != nil
	s.mu.Unlock()
	if !ready {
		return
	}

	notified := false
	for {
		s.mu.Lock()
		c, ok, err := v.nextChain(mem)
		s.mu.Unlock()
		if err != nil {
			logger.Warnf("vhost-user-fs: bad descriptor chain: %s", err)
			return
		}
		if !ok {
			break
		}

		reply, err := s.handleChain(c)
		if err != nil {
			// Complete the chain even so, or the guest waits on it forever.
			logger.Warnf("vhost-user-fs: malformed request: %s", err)
			reply = nil
		}
		written := scatter(c.writable, reply)

		s.mu.Lock()
		err = v.addUsed(c.head, written)
		s.mu.Unlock()
		if err != nil {
			logger.Warnf("vhost-user-fs: cannot publish completion: %s", err)
			return
		}
		notified = true
	}
	if notified {
		s.mu.Lock()
		err := v.notify()
		s.mu.Unlock()
		if err != nil {
			logger.Warnf("vhost-user-fs: notify failed: %s", err)
		}
	}
}

func payloadU64(hdr header, body []byte) (uint64, error) {
	if len(body) < 8 {
		return 0, fmt.Errorf("request %d body is %d bytes, want 8", hdr.Request, len(body))
	}
	return binary.LittleEndian.Uint64(body), nil
}

func payloadVringState(hdr header, body []byte) (vringState, error) {
	if len(body) < 8 {
		return vringState{}, fmt.Errorf("request %d body is %d bytes, want a vring state", hdr.Request, len(body))
	}
	return decodeVringState(body), nil
}
