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

package vfs

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// discardStore reads the data of every Put and keeps none of it, so a benchmark's memory stays flat however many files
// it writes, and the cases that run first do not slow down the ones after them. The store it wraps stays empty: a Get
// finds nothing, and is counted, since the write path should not read anything back.
type discardStore struct {
	object.ObjectStorage
	gets atomic.Int64
}

func (s *discardStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	_, err := io.Copy(io.Discard, in)
	return err
}

func (s *discardStore) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	s.gets.Add(1)
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

// BenchmarkWrite times the write path, VFS.Write through the writer to an object store that discards the data
// (discardStore) and memkv metadata, then Flush, in both commit modes: an 8 MiB file per iteration in writes of the
// given size, consecutive slices of one random 8 MiB buffer, so every case uploads the same incompressible bytes. It
// compares builds of the writer (for example before and after an upstream sync); run it with a fixed -benchtime=Nx, so
// the runs compared do the same work, and compare with benchstat.
func BenchmarkWrite(b *testing.B) {
	const fileSize = 8 << 20
	data := make([]byte, fileSize)
	rand.New(rand.NewSource(1)).Read(data)
	for _, mode := range []string{"chunk", "epoch"} {
		for _, size := range []int{4 << 10, 128 << 10} {
			b.Run(fmt.Sprintf("mode=%s/write=%dKiB", mode, size>>10), func(b *testing.B) {
				b.Setenv("JFS_COMMIT_MODE", mode) // read by NewDataWriter
				store := new(discardStore)
				v, _ := createTestVFSWithStore(nil, "", func(inner object.ObjectStorage) object.ObjectStorage {
					store.ObjectStorage = inner
					return store
				})
				ctx := NewLogContext(meta.Background())
				b.SetBytes(fileSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fe, fh, e := v.Create(ctx, 1, fmt.Sprintf("f%d", i), 0644, 0, syscall.O_RDWR)
					if e != 0 {
						b.Fatalf("create: %s", e)
					}
					for off := 0; off < fileSize; off += size {
						if e := v.Write(ctx, fe.Inode, data[off:off+size], uint64(off), fh); e != 0 {
							b.Fatalf("write at %d: %s", off, e)
						}
					}
					if e := v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
						b.Fatalf("flush: %s", e)
					}
					v.Release(ctx, fe.Inode, fh)
				}
				b.StopTimer()
				if n := store.gets.Load(); n != 0 {
					b.Fatalf("the write path read %d objects back, which the store did not keep", n)
				}
			})
		}
	}
}
