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
	"context"
	"io"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/stretchr/testify/require"
)

const raceTestKey = "chunks/0/0/1_0_1048576"

// slowGetStorage delays Get past GetTimeout so WithTimeout abandons the caller's closure while it
// is still running.
type slowGetStorage struct {
	object.ObjectStorage
	delay time.Duration
}

func (s *slowGetStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

func newTimingOutStore(t *testing.T) *cachedStore {
	t.Helper()
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	require.NoError(t, mem.Put(ctx, raceTestKey, io.LimitReader(fillReader{}, 1<<20)))

	conf := defaultConf
	// The memory cache keeps the disk cache's background scanner out of this test.
	conf.CacheDir = "memory"
	conf.GetTimeout = 20 * time.Millisecond
	return NewCachedStore(&slowGetStorage{mem, 120 * time.Millisecond}, conf, nil).(*cachedStore)
}

// A GET that outlives GetTimeout leaves the closure running after load has returned. The closure
// must not share err, n, reqID or sc with load: writing the error interface while errors.Is reads
// it tears the interface, and the runtime faults on the torn type word rather than raising a
// recoverable panic.
func TestLoadDoesNotRaceWithTimedOutGet(t *testing.T) {
	store := newTimingOutStore(t)
	page := NewOffPage(1 << 20)
	defer page.Release()

	err := store.load(ctx, raceTestKey, page, false, false)
	require.ErrorContains(t, err, utils.ErrFuncTimeout.Error())

	// Let the abandoned closure publish its result; it must not reach load's caller.
	time.Sleep(300 * time.Millisecond)
}

func TestLoadRangeDoesNotRaceWithTimedOutGet(t *testing.T) {
	store := newTimingOutStore(t)
	page := NewOffPage(64 << 10)
	defer page.Release()

	n, err := store.loadRange(ctx, raceTestKey, page, 0)
	require.ErrorIs(t, err, errTryFullRead)
	require.Zero(t, n)

	time.Sleep(300 * time.Millisecond)
}

type fillReader struct{}

func (fillReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
