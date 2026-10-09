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

package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// gatedGetStorage holds the first Get until release is closed (or the Get's context ends), so a
// test can keep a shared block load in flight while other readers join it.
type gatedGetStorage struct {
	object.ObjectStorage
	gets    atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (s *gatedGetStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if s.gets.Add(1) == 1 {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

// waitForJoin returns once a reader has joined the load of key in flight.
func waitForJoin(t *testing.T, store *cachedStore, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		store.group.Lock()
		defer store.group.Unlock()
		c, ok := store.group.rs[key]
		return ok && c.dups > 0
	}, 5*time.Second, time.Millisecond)
}

// A reader that joins a block load owned by another reader used to get that owner's
// context.Canceled when the owner's context was canceled, even though its own context was live.
// That is how a compaction (context.Background) failed when it joined the download of a read-ahead
// the vfs reader then dropped. The joiner must load the block again and get its bytes, and must
// not keep a reference to the page of the load it gave up on.
func TestSharedLoadSurvivesCanceledOwner(t *testing.T) {
	const blockSize = 1 << 20
	const id = 7
	key := "chunks/0/0/7_0_1048576"
	data := bytes.Repeat([]byte("epoch"), blockSize/5+1)[:blockSize]

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	require.NoError(t, mem.Put(ctx, key, bytes.NewReader(data)))
	gated := &gatedGetStorage{ObjectStorage: mem, started: make(chan struct{}), release: make(chan struct{})}
	defer close(gated.release)

	conf := defaultConf
	// The memory cache keeps the disk cache's background scanner out of this test.
	conf.CacheDir = "memory"
	// The owner's Get is held open while the test coordinates; only cancelOwner may end it.
	conf.GetTimeout = time.Minute
	store := NewCachedStore(gated, conf, nil).(*cachedStore)

	// The owner reads the whole block into its own page, so the shared load fills that page.
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()
	ownerPage := NewOffPage(blockSize)
	ownerErr := make(chan error, 1)
	go func() {
		_, err := store.NewReader(id, blockSize).ReadAt(ownerCtx, ownerPage, 0)
		ownerErr <- err
	}()
	<-gated.started

	// The joiner reads part of the block with a context nothing cancels, as a compaction does.
	joinerPage := NewOffPage(4096)
	defer joinerPage.Release()
	type result struct {
		n   int
		err error
	}
	joined := make(chan result, 1)
	go func() {
		n, err := store.NewReader(id, blockSize).ReadAt(context.Background(), joinerPage, 0)
		joined <- result{n, err}
	}()
	waitForJoin(t, store, key)

	cancelOwner()
	require.ErrorIs(t, <-ownerErr, context.Canceled)

	got := <-joined
	require.NoError(t, got.err)
	require.Equal(t, len(joinerPage.Data), got.n)
	require.Equal(t, data[:len(joinerPage.Data)], joinerPage.Data)
	require.EqualValues(t, 2, gated.gets.Load(), "the joiner loads the block again after the owner is canceled")

	// Only this test still holds the owner's page once the abandoned Get has returned.
	require.Eventually(t, func() bool { return atomic.LoadInt32(&ownerPage.refs) == 1 }, 5*time.Second, time.Millisecond)
	ownerPage.Release()
}

// executeShared loads again only when the load it got failed with context.Canceled while the caller's
// own context is live, at most sharedLoadRetries times, and releases the page of every load it gives
// up on.
func TestExecuteSharedRetries(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.New("get failed")
	cases := []struct {
		name    string
		ctx     context.Context
		results []error // what each load returns; the last one repeats
		loads   int
		err     error
	}{
		{"success", context.Background(), []error{nil}, 1, nil},
		{"other error", context.Background(), []error{failure}, 1, failure},
		{"own context canceled", canceled, []error{context.Canceled}, 1, context.Canceled},
		{"canceled once", context.Background(), []error{context.Canceled, nil}, 2, nil},
		{"canceled every time", context.Background(), []error{context.Canceled}, 1 + sharedLoadRetries, context.Canceled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &cachedStore{group: NewController()}
			var pages []*Page
			block, err := store.executeShared(c.ctx, "key", func() (*Page, error) {
				p := NewOffPage(4096)
				pages = append(pages, p)
				return p, c.results[min(len(pages), len(c.results))-1]
			})
			require.Equal(t, c.err, err)
			require.Len(t, pages, c.loads)
			require.Same(t, pages[len(pages)-1], block)
			for _, p := range pages[:len(pages)-1] {
				require.Zero(t, atomic.LoadInt32(&p.refs), "the page of a load given up on is released")
			}
			require.EqualValues(t, 1, atomic.LoadInt32(&block.refs), "the caller holds the returned page")
			block.Release()
		})
	}
}
