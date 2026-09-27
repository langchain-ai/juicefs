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
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// faultyMeta fails chunk lookups with lookupErr while it is set.
type faultyMeta struct {
	meta.Meta
	lookupErr atomic.Uint32
}

func (m *faultyMeta) Read(ctx meta.Context, inode Ino, indx uint32, slices *[]meta.Slice) syscall.Errno {
	if e := syscall.Errno(m.lookupErr.Load()); e != 0 {
		return e
	}
	return m.Meta.Read(ctx, inode, indx, slices)
}

// faultyChunkStore fails slice fetches while failing is set.
type faultyChunkStore struct {
	chunk.ChunkStore
	failing *atomic.Bool
}

func (s *faultyChunkStore) NewReader(id uint64, length int) chunk.Reader {
	return &faultyChunkReader{s.ChunkStore.NewReader(id, length), s.failing}
}

type faultyChunkReader struct {
	chunk.Reader
	failing *atomic.Bool
}

func (r *faultyChunkReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	if r.failing.Load() {
		return 0, errors.New("injected fetch failure")
	}
	return r.Reader.ReadAt(ctx, p, off)
}

// A read that runs out of retries fails, but a later read on the same handle tries again once the backend recovers,
// unless the file is gone.
func TestReadAfterFailedReadRetries(t *testing.T) {
	cases := []struct {
		name      string
		lookupErr syscall.Errno
		fetchFail bool
		wantErr   syscall.Errno
		recovers  bool
	}{
		{name: "chunk lookup fails", lookupErr: syscall.EIO, wantErr: syscall.EIO, recovers: true},
		{name: "slice fetch fails", fetchFail: true, wantErr: syscall.EIO, recovers: true},
		{name: "file is missing", lookupErr: syscall.ENOENT, wantErr: syscall.EBADF, recovers: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := createTestVFS(func(c *meta.Config) { c.Retries = 1 }, "")
			m := &faultyMeta{Meta: v.Meta}
			var fetchFail atomic.Bool
			r := NewDataReader(v.Conf, m, &faultyChunkStore{v.Store, &fetchFail})
			v.reader = r
			v.writer.(*dataWriter).reader = r

			ctx := NewLogContext(meta.Background())
			fe, fhw, e := v.Create(ctx, 1, "file", 0644, 0, syscall.O_RDWR)
			require.Zero(t, e)
			want := bytes.Repeat([]byte{'a'}, 64<<10)
			require.Zero(t, v.Write(ctx, fe.Inode, want, 0, fhw))
			require.Zero(t, v.Fsync(ctx, fe.Inode, 1, fhw))
			_, fh, e := v.Open(ctx, fe.Inode, syscall.O_RDONLY)
			require.Zero(t, e)

			m.lookupErr.Store(uint32(tc.lookupErr))
			fetchFail.Store(tc.fetchFail)
			buf := make([]byte, len(want))
			_, e = v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Equal(t, tc.wantErr, e)

			m.lookupErr.Store(0)
			fetchFail.Store(false)
			n, e := v.Read(ctx, fe.Inode, buf, 0, fh)
			if !tc.recovers {
				require.Equal(t, tc.wantErr, e)
				return
			}
			require.Zero(t, e)
			require.Equal(t, want, buf[:n])
		})
	}
}
