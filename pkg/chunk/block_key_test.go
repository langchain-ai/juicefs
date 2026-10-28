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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// cacheTestDir returns a cache directory that is removed after the test. Unlike t.TempDir, a failed removal is not an
// error: the store keeps flushing cached pages in the background after the test is done with it.
func cacheTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "jfs-hash-prefix-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestBlockKey(t *testing.T) {
	const id = 1922164123 // id%256 = 0x9B
	cases := []struct {
		name       string
		hashPrefix bool
		from       uint64
		key        string
	}{
		{"sequential volume", false, 0, "chunks/1922/1922164/1922164123_3_4096"},
		{"hash-prefix volume", true, 0, "chunks/9B/1922/1922164123_3_4096"},
		{"below the threshold", false, id + 1, "chunks/1922/1922164/1922164123_3_4096"},
		{"at the threshold", false, id, "chunks/9B/1922/1922164123_3_4096"},
		{"above the threshold", false, id - 1, "chunks/9B/1922/1922164123_3_4096"},
		{"hash-prefix volume ignores the threshold", true, id + 1, "chunks/9B/1922/1922164123_3_4096"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.key, BlockKey(id, 3, 4096, c.hashPrefix, c.from))
			require.Equal(t, c.key, SliceKeyPrefix(id, c.hashPrefix, c.from)+"_3_4096")
		})
	}
	// Slice ids below 1e6 hash too: the threshold is a plain id comparison.
	require.Equal(t, "chunks/D0/0/2000_0_1", BlockKey(2000, 0, 1, false, 2000))
	require.Equal(t, "chunks/0/1/1999_0_1", BlockKey(1999, 0, 1, false, 2000))
}

func TestKeyLayoutSetHashFrom(t *testing.T) {
	l := newKeyLayout(false, 0)
	l.setHashFrom(0)
	require.Equal(t, "chunks/0/2/2500_0_1", l.key(2500, 0, 1))

	l.setHashFrom(2000)
	require.Equal(t, "chunks/C4/0/2500_0_1", l.key(2500, 0, 1))

	// The first threshold sticks: a later one, or none, would move the keys of slices already written.
	l.setHashFrom(3000)
	require.Equal(t, "chunks/C4/0/2500_0_1", l.key(2500, 0, 1))
	l.setHashFrom(0)
	require.Equal(t, "chunks/C4/0/2500_0_1", l.key(2500, 0, 1))
	require.Equal(t, uint64(2000), l.hashFrom.Load())

	require.Equal(t, uint64(5), newKeyLayout(false, 5).hashFrom.Load())
}

// TestStoreHashPrefixFromSlice checks that the store and its disk cache name blocks by the same threshold, and that a
// threshold learned after the store started applies to new slices only.
func TestStoreHashPrefixFromSlice(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheDir = cacheTestDir(t)
	conf.FreeSpace = 0.01
	store := NewCachedStore(mem, conf, nil)
	cs := store.(*cachedStore)

	require.NoError(t, forgetSlice(store, 1000, 4096))
	require.NoError(t, forgetSlice(store, 2000, 4096))
	store.SetHashPrefixFromSlice(2000)
	require.NoError(t, forgetSlice(store, 1999, 4096))
	require.NoError(t, forgetSlice(store, 2001, 4096))
	store.SetHashPrefixFromSlice(3000) // ignored
	require.NoError(t, forgetSlice(store, 2500, 4096))

	for _, key := range []string{
		"chunks/0/1/1000_0_4096",
		"chunks/0/2/2000_0_4096", // written before the store learned it, which config's margin rules out in practice
		"chunks/0/1/1999_0_4096",
		"chunks/D1/0/2001_0_4096",
		"chunks/C4/0/2500_0_4096",
	} {
		_, err := mem.Head(ctx, key)
		require.NoError(t, err, key)
	}

	dc := cs.bcache.(*cacheManager).stores[0]
	require.Equal(t, "chunks/D1/0/2001_0_4096", dc.getPathFromKey(cacheKey{id: 2001, indx: 0, size: 4096}))
	require.Equal(t, "chunks/0/1/1999_0_4096", dc.getPathFromKey(cacheKey{id: 1999, indx: 0, size: 4096}))

	for _, id := range []uint64{1999, 2001, 2500} {
		p := NewPage(make([]byte, 4096))
		n, err := store.NewReader(id, 4096).ReadAt(ctx, p, 0)
		require.NoError(t, err)
		require.Equal(t, 4096, n)
		p.Release()
	}

	require.NoError(t, store.Remove(2001, 4096))
	require.NoError(t, store.Remove(1999, 4096))
	for _, key := range []string{"chunks/D1/0/2001_0_4096", "chunks/0/1/1999_0_4096"} {
		_, err := mem.Head(ctx, key)
		require.Error(t, err, key)
	}
}

// TestStagingHashPrefixFromSlice writes back blocks on both sides of the threshold, then starts a second store on the
// same cache directory, as a restarted client would, and checks that it uploads each staged block under the key it was
// staged with.
func TestStagingHashPrefixFromSlice(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheDir = cacheTestDir(t)
	conf.FreeSpace = 0.01
	conf.Writeback = true
	conf.WritebackThresholdSize = conf.BlockSize + 1
	conf.UploadDelay = time.Hour
	conf.HashPrefixFromSlice = 2000
	store := NewCachedStore(mem, conf, nil)
	require.NoError(t, forgetSlice(store, 1999, 4096))
	require.NoError(t, forgetSlice(store, 2001, 4096))
	staged := []string{"chunks/0/1/1999_0_4096", "chunks/D1/0/2001_0_4096"}
	for _, key := range staged {
		_, err := os.Stat(filepath.Join(conf.CacheDir, stagingDir, key))
		require.NoError(t, err, key)
		_, err = mem.Head(ctx, key)
		require.Error(t, err, key) // not uploaded yet
	}

	conf.UploadDelay = 0
	_ = NewCachedStore(mem, conf, nil)
	require.Eventually(t, func() bool {
		for _, key := range staged {
			if _, err := mem.Head(ctx, key); err != nil {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}
