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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// noProgressChunkReader reports success without consuming any of the page, the one combination
// readSlice's loop cannot make progress on.
type noProgressChunkReader struct {
	calls atomic.Int64
}

func (r *noProgressChunkReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	r.calls.Add(1)
	return 0, nil
}

type stubChunkStore struct {
	reader chunk.Reader
}

func (s *stubChunkStore) NewReader(id uint64, length int) chunk.Reader                   { return s.reader }
func (s *stubChunkStore) NewWriter(id uint64, tierID uint8) chunk.Writer                 { return nil }
func (s *stubChunkStore) Remove(id uint64, length int) error                             { return nil }
func (s *stubChunkStore) FillCache(id uint64, length uint32, parts []chunk.Range) error  { return nil }
func (s *stubChunkStore) EvictCache(id uint64, length uint32, parts []chunk.Range) error { return nil }
func (s *stubChunkStore) CheckCache(id uint64, length uint32, parts []chunk.Range, handler func(bool, string, int)) error {
	return nil
}
func (s *stubChunkStore) UsedMemory() int64                  { return 0 }
func (s *stubChunkStore) UpdateLimit(upload, download int64) {}
func (s *stubChunkStore) BlobStorage() object.ObjectStorage  { return nil }

// A reader that returns (0, nil) leaves readSlice's loop unable to advance. Without a guard the
// loop re-reads the same range forever, wedging the goroutine and its page.
func TestReadSliceRejectsNoProgress(t *testing.T) {
	reader := &noProgressChunkReader{}
	dr, _ := createCancellationTestReader(t, &stubChunkStore{reader: reader})

	page := chunk.NewOffPage(4)
	defer page.Release()

	done := make(chan error, 1)
	go func() {
		done <- dr.readSlice(context.Background(), &meta.Slice{Id: 1, Size: 4, Len: 4}, page, 0)
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, io.ErrNoProgress)
		require.EqualValues(t, 1, reader.calls.Load(), "should give up after one fruitless read")
	case <-time.After(3 * time.Second):
		t.Fatalf("readSlice never returned: spun %d times on a reader making no progress", reader.calls.Load())
	}
}

// A canceled read is a dropped read-ahead slice, not a failure of the file, so readSlice must hand
// the cancellation back rather than reporting it as an I/O error.
func TestReadSliceReturnsCancellation(t *testing.T) {
	reader := &blockingChunkReader{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	dr, _ := createCancellationTestReader(t, &blockingChunkStore{reader: reader})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	page := chunk.NewOffPage(4)
	defer page.Release()

	err := dr.readSlice(ctx, &meta.Slice{Id: 1, Size: 4, Len: 4}, page, 0)
	require.ErrorIs(t, err, context.Canceled)
}
