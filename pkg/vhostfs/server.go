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

// Package vhostfs serves a JuiceFS filesystem to virtual machines over
// vhost-user virtio-fs, so a guest mounts it with `mount -t virtiofs` instead of
// going through a FUSE mount on the host.
package vhostfs

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/utils"
)

var logger = utils.GetLogger("juicefs")

// numQueues is the queue count virtio-fs uses: one hiprio plus one request queue.
const numQueues = 2

// requestQueue is the index of the queue carrying ordinary FUSE traffic; queue 0
// is hiprio (interrupts and forgets).
const requestQueue = 1

// maxMemRegions caps the guest memory table, bounding what one SET_MEM_TABLE can
// make us map.
const maxMemRegions = 32

// Config configures a Server.
type Config struct {
	// Socket is the Unix socket path to listen on. Every VMM on the host connects
	// to it, and each connection is served independently.
	Socket string
	// MaxWrite is the largest write the guest may send in one request.
	MaxWrite int
	// EnableDax serves reads by mapping into the guest's DAX window rather than
	// copying them through the request queue. It requires the VMM to offer the
	// backend-request channel.
	EnableDax bool
	// SocketMode is applied to the socket after it is created. Connecting to a unix
	// socket needs write permission on it, so a VMM running as another user (a
	// jailed hypervisor) cannot reach a socket left at the default mode. Zero
	// leaves whatever the process umask produced.
	SocketMode os.FileMode
}

// defaultSocketMode lets a hypervisor running as another user connect. The socket
// carries no authentication, so it must sit in a directory only trusted users can
// reach.
const defaultSocketMode os.FileMode = 0o666

// Server is a vhost-user virtio-fs backend. One Server serves any number of
// concurrent VMs from a single filesystem client, and its DAX chunk cache is
// shared across them: two guests reading the same file map the same host pages.
type Server struct {
	cfg      Config
	fs       fuse.RawFileSystem
	maxWrite int
	chunks   *chunkCache

	listener *net.UnixListener
	stopped  atomic.Bool

	mu       sync.Mutex
	sessions map[*session]struct{}
}

// NewServer creates a backend serving fs, listening on cfg.Socket.
func NewServer(fs fuse.RawFileSystem, cfg Config) (*Server, error) {
	if cfg.Socket == "" {
		return nil, errors.New("socket path is required")
	}
	if cfg.MaxWrite <= 0 {
		cfg.MaxWrite = 128 << 10
	}
	// A stale socket from a previous run would make Listen fail; we own the path,
	// since the VMMs connect to us.
	if err := os.Remove(cfg.Socket); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("clear stale socket: %w", err)
	}
	addr, err := net.ResolveUnixAddr("unix", cfg.Socket)
	if err != nil {
		return nil, err
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}
	if cfg.SocketMode != 0 {
		if err := os.Chmod(cfg.Socket, cfg.SocketMode); err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("chmod socket: %w", err)
		}
	}
	return &Server{
		cfg:      cfg,
		fs:       fs,
		maxWrite: cfg.MaxWrite,
		chunks:   newChunkCache(fs),
		listener: l,
		sessions: make(map[*session]struct{}),
	}, nil
}

// Serve accepts connections until Close, running each in its own goroutine. It
// returns nil on a clean shutdown.
func (s *Server) Serve() error {
	for {
		sock, err := s.listener.AcceptUnix()
		if err != nil {
			if s.stopped.Load() {
				return nil
			}
			return err
		}
		sess := newSession(s, &conn{sock: sock})
		s.mu.Lock()
		s.sessions[sess] = struct{}{}
		s.mu.Unlock()
		logger.Infof("vhost-user-fs: frontend connected on %s", s.cfg.Socket)

		go func() {
			err := sess.serve()
			s.mu.Lock()
			delete(s.sessions, sess)
			s.mu.Unlock()
			if err != nil && !s.stopped.Load() {
				logger.Infof("vhost-user-fs: connection ended: %s", err)
			}
		}()
	}
}

// Close stops accepting, ends every session, and releases the socket.
func (s *Server) Close() error {
	if !s.stopped.CompareAndSwap(false, true) {
		return nil
	}
	err := s.listener.Close()

	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.close()
	}
	s.chunks.close()
	return err
}
