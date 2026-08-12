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

package cmd

import (
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/juicedata/juicefs/pkg/vhostfs"
	"github.com/urfave/cli/v2"
)

func serveVhostUser(v *vfs.VFS, socket string, dax bool) error {
	return vhostfs.Serve(v, vhostfs.Config{Socket: socket, EnableDax: dax})
}

// vhostUserMode reports whether this mount serves a VM over vhost-user instead of
// mounting locally. Several steps of the mount flow act on the local mount point,
// which in this mode is never mounted at all.
func vhostUserMode(c *cli.Context) bool {
	return c.String("vhost-user-socket") != ""
}
