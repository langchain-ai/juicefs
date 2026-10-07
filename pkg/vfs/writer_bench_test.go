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

package vfs

import (
	"fmt"
	"math/rand"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

// BenchmarkWrite times the write path, VFS.Write through the writer to the in-memory object store and memkv
// metadata, then Flush, in both commit modes: an 8 MiB file per iteration in writes of the given size. It compares
// builds of the writer (for example before and after an upstream sync); run it with a fixed -benchtime=Nx, since
// every iteration keeps its file, and compare with benchstat.
func BenchmarkWrite(b *testing.B) {
	const fileSize = 8 << 20
	for _, mode := range []string{"chunk", "epoch"} {
		for _, size := range []int{4 << 10, 128 << 10} {
			b.Run(fmt.Sprintf("mode=%s/write=%dKiB", mode, size>>10), func(b *testing.B) {
				b.Setenv("JFS_COMMIT_MODE", mode) // read by NewDataWriter
				v, _ := createTestVFS(nil, "")
				ctx := NewLogContext(meta.Background())
				buf := make([]byte, size)
				rand.New(rand.NewSource(1)).Read(buf)
				b.SetBytes(fileSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fe, fh, e := v.Create(ctx, 1, fmt.Sprintf("f%d", i), 0644, 0, syscall.O_RDWR)
					if e != 0 {
						b.Fatalf("create: %s", e)
					}
					for off := 0; off < fileSize; off += size {
						if e := v.Write(ctx, fe.Inode, buf, uint64(off), fh); e != 0 {
							b.Fatalf("write at %d: %s", off, e)
						}
					}
					if e := v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
						b.Fatalf("flush: %s", e)
					}
					v.Release(ctx, fe.Inode, fh)
				}
			})
		}
	}
}
