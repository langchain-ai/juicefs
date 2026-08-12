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
	jfsfuse "github.com/juicedata/juicefs/pkg/fuse"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// Serve exposes v to a VMM over vhost-user virtio-fs and blocks until the socket
// is closed. It is the vhost-user counterpart of pkg/fuse.Serve.
func Serve(v *vfs.VFS, cfg Config) error {
	if cfg.SocketMode == 0 {
		cfg.SocketMode = defaultSocketMode
	}
	if cfg.MaxWrite <= 0 {
		cfg.MaxWrite = v.Conf.FuseOpts.MaxWrite
	}
	srv, err := NewServer(jfsfuse.NewRawFileSystem(v), cfg)
	if err != nil {
		return err
	}
	defer srv.Close()
	logger.Infof("vhost-user-fs: serving volume %s on %s (dax=%t)", v.Conf.Format.Name, cfg.Socket, cfg.EnableDax)
	return srv.Serve()
}
