/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package fs

import (
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// An invalid commit config makes NewFileSystem fail, with an error that names the variable, rather than end the
// process (the Java SDK runs in the JVM's).
func TestNewFileSystemRejectsAnInvalidCommitConfig(t *testing.T) {
	for name, value := range map[string]string{"JFS_COMMIT_MODE": "Epoch", "JFS_EPOCH_MAX_AGE_MS": "abc"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			conf := vfs.Config{Meta: meta.DefaultConf(), Chunk: &chunk.Config{BlockSize: 4 << 20, MaxUpload: 1, BufferSize: 100 << 20}}
			objStore, _ := object.CreateStorage("mem", "", "", "", "")
			jfs, err := NewFileSystem(&conf, meta.NewClient("memkv://", nil), chunk.NewCachedStore(objStore, *conf.Chunk, nil), nil)
			if err == nil || jfs != nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("NewFileSystem with %s=%q: %v, %v; want an error naming the variable", name, value, jfs, err)
			}
		})
	}
}
