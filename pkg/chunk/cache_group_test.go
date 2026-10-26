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
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	dto "github.com/prometheus/client_model/go"
)

// owner returns key's primary owner.
func (g *cacheGroup) owner(key string) string {
	return g.owners(key)[0]
}

// countingStorage counts GETs and adds a fixed latency to each, standing in for a remote object store.
type countingStorage struct {
	object.ObjectStorage
	gets  atomic.Int64
	delay time.Duration
}

func (c *countingStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	c.gets.Add(1)
	time.Sleep(c.delay)
	return c.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	var addrs []string
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, ln.Addr().String())
		_ = ln.Close()
	}
	return addrs
}

// writeSlice stores a slice of nBlocks full blocks of random data and returns its contents.
func writeSlice(t *testing.T, storage object.ObjectStorage, id uint64, nBlocks int) []byte {
	t.Helper()
	conf := defaultConf
	conf.CacheDir = groupCacheDir(t)
	w := NewCachedStore(storage, conf, nil).NewWriter(id, 0)
	data := make([]byte, nBlocks*conf.BlockSize)
	_, _ = rand.Read(data)
	if _, err := w.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(len(data)); err != nil {
		t.Fatal(err)
	}
	return data
}

// groupStores starts one store per address, all in one cache group over storage.
func groupStores(t *testing.T, storage object.ObjectStorage, peers string, addrs []string) []ChunkStore {
	t.Helper()
	var stores []ChunkStore
	for _, a := range addrs {
		conf := defaultConf
		conf.CacheDir = groupCacheDir(t)
		conf.CacheSize = 100 << 20
		conf.CacheFullBlock = true // as mounts default to (cache-partial-only off)
		conf.CacheGroupPeers = peers
		conf.CacheGroupListen = a
		conf.CacheGroupSelf = a
		conf.CacheGroupTimeout = 2 * time.Second
		stores = append(stores, NewCachedStore(storage, conf, nil))
	}
	return stores
}

func readBlock(t *testing.T, store ChunkStore, id uint64, length, indx int) []byte {
	t.Helper()
	p := NewOffPage(defaultConf.BlockSize)
	defer p.Release()
	n, err := store.NewReader(id, length).ReadAt(context.Background(), p, indx*defaultConf.BlockSize)
	if err != nil || n != defaultConf.BlockSize {
		t.Fatalf("read block %d: %d %v", indx, n, err)
	}
	return append([]byte(nil), p.Data...)
}

func TestCacheGroupOwnerIsStable(t *testing.T) {
	g := &cacheGroup{self: "a:1"}
	members := []string{"a:1", "b:1", "c:1", "d:1"}
	g.members.Store(&members)
	before := map[string]string{}
	counts := map[string]int{}
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("chunks/0/0/%d_0_4194304", i)
		before[key] = g.owner(key)
		counts[before[key]]++
	}
	for _, m := range members {
		if counts[m] < 800 || counts[m] > 1200 {
			t.Fatalf("uneven spread: %v", counts)
		}
	}
	fewer := []string{"a:1", "b:1", "d:1"}
	g.members.Store(&fewer)
	for key, was := range before {
		if now := g.owner(key); was != "c:1" && now != was {
			t.Fatalf("%s moved from %s to %s though its owner stayed", key, was, now)
		}
	}
}

func TestCacheGroupFetchesEachBlockOnce(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 7, 24
	data := writeSlice(t, storage, id, nBlocks)
	storage.gets.Store(0)
	storage.delay = 20 * time.Millisecond

	addrs := freeAddrs(t, 3)
	stores := groupStores(t, storage, strings.Join(addrs, ","), addrs)

	// Every member reads every block at once, as a burst of VMs on three cold hosts would.
	var wg sync.WaitGroup
	errs := make(chan error, len(stores)*nBlocks)
	for _, s := range stores {
		for i := 0; i < nBlocks; i++ {
			wg.Add(1)
			go func(s ChunkStore, i int) {
				defer wg.Done()
				if got := readBlock(t, s, id, len(data), i); !bytes.Equal(got, data[i*defaultConf.BlockSize:(i+1)*defaultConf.BlockSize]) {
					errs <- fmt.Errorf("block %d: wrong data", i)
				}
			}(s, i)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := storage.gets.Load(); got != nBlocks {
		for i, st := range stores {
			g := st.(*cachedStore).cacheGroup
			t.Logf("member %d (%s): hit=%v loaded=%v connect=%v timeout=%v down=%v", i, g.self, testutilCount(g, "hit"), testutilCount(g, "loaded"), testutilCount(g, "connect"), testutilCount(g, "timeout"), testutilCount(g, "down"))
		}
		t.Fatalf("object storage GETs = %d, want %d (one per block across the group)", got, nBlocks)
	}

	// A small read inside a block another member owns goes to that member too, not to an object-storage range GET.
	fresh := groupStores(t, storage, strings.Join(addrs, ","), freeAddrs(t, 1))[0]
	before := storage.gets.Load()
	p := NewOffPage(100)
	defer p.Release()
	if n, err := fresh.NewReader(id, len(data)).ReadAt(context.Background(), p, 3*defaultConf.BlockSize+17); err != nil || n != 100 {
		t.Fatalf("small read: %d %v", n, err)
	}
	if !bytes.Equal(p.Data, data[3*defaultConf.BlockSize+17:3*defaultConf.BlockSize+117]) {
		t.Fatal("small read: wrong data")
	}
	if got := storage.gets.Load() - before; got != 0 {
		t.Fatalf("small read made %d object storage GETs, want 0", got)
	}
}

func TestCacheGroupFallsBackWhenOwnerIsDown(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 9, 8
	data := writeSlice(t, storage, id, nBlocks)

	addrs := freeAddrs(t, 2) // the second member never starts
	store := groupStores(t, storage, strings.Join(addrs, ","), addrs[:1])[0]
	for i := 0; i < nBlocks; i++ {
		if got := readBlock(t, store, id, len(data), i); !bytes.Equal(got, data[i*defaultConf.BlockSize:(i+1)*defaultConf.BlockSize]) {
			t.Fatalf("block %d: wrong data", i)
		}
	}
	g := store.(*cachedStore).cacheGroup
	if !g.isDown(addrs[1]) {
		t.Fatal("unreachable member was not marked down")
	}
}

func TestCacheGroupRejectsNonBlockKeys(t *testing.T) {
	for _, key := range []string{"", "meta/dump", "chunks/../secret_0_10", "chunks/0/0/1_0_0", fmt.Sprintf("chunks/0/0/1_0_%d", defaultConf.BlockSize+1)} {
		if _, ok := validCacheGroupKey(key, defaultConf.BlockSize); ok {
			t.Fatalf("%q accepted", key)
		}
	}
	if size, ok := validCacheGroupKey("chunks/0/0/1_0_4096", defaultConf.BlockSize); !ok || size != 4096 {
		t.Fatalf("valid key rejected: %d %v", size, ok)
	}
}

func TestCacheGroupResolvesDNSMembers(t *testing.T) {
	g := &cacheGroup{peers: "dns:sandbox-host-peers.sandbox.svc:9568", resolve: func(host string) ([]string, error) {
		if host != "sandbox-host-peers.sandbox.svc" {
			return nil, fmt.Errorf("unexpected host %s", host)
		}
		return []string{"10.0.0.2", "10.0.0.1"}, nil
	}}
	if err := g.refresh(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(*g.members.Load(), ","); got != "10.0.0.1:9568,10.0.0.2:9568" {
		t.Fatalf("members = %s", got)
	}
}

func testutilCount(g *cacheGroup, result string) float64 {
	m := &dto.Metric{}
	_ = g.requests.WithLabelValues(result).Write(m)
	return m.GetCounter().GetValue()
}

// groupCacheDir is a temporary cache directory removed best-effort: the disk cache writes blocks in the background, so a
// test can end while a write is still landing, which t.TempDir's strict cleanup reports as a failure.
func groupCacheDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "cache-group-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestCacheGroupColdMemberLatency compares how long cold members take to read a working set one block at a time (as
// a restoring guest's page faults do) when another member has already read it, with and without a cache group.
// It is a timing comparison, so it only runs with JFS_CACHE_GROUP_TIMING set.
func TestCacheGroupColdMemberLatency(t *testing.T) {
	if os.Getenv("JFS_CACHE_GROUP_TIMING") == "" {
		t.Skip("set JFS_CACHE_GROUP_TIMING to run")
	}
	const id, nBlocks, members = 11, 40, 9
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	data := writeSlice(t, storage, id, nBlocks)
	storage.delay = 50 * time.Millisecond

	run := func(t *testing.T, stores []ChunkStore) {
		storage.gets.Store(0)
		for i := 0; i < nBlocks; i++ { // the warm member
			readBlock(t, stores[0], id, len(data), i)
		}
		warmGets := storage.gets.Load()
		var wg sync.WaitGroup
		times := make([]time.Duration, len(stores)-1)
		for m, s := range stores[1:] {
			wg.Add(1)
			go func(m int, s ChunkStore) {
				defer wg.Done()
				start := time.Now()
				for i := 0; i < nBlocks; i++ {
					readBlock(t, s, id, len(data), i)
				}
				times[m] = time.Since(start)
			}(m, s)
		}
		wg.Wait()
		var max time.Duration
		for _, d := range times {
			max = maxDuration(max, d)
		}
		t.Logf("%d cold members, %d blocks each: slowest %s, object GETs while warming %d, during the burst %d",
			len(stores)-1, nBlocks, max.Round(time.Millisecond), warmGets, storage.gets.Load()-warmGets)
	}
	t.Run("without", func(t *testing.T) {
		var stores []ChunkStore
		for i := 0; i < members; i++ {
			conf := defaultConf
			conf.CacheDir = groupCacheDir(t)
			conf.CacheSize = 100 << 20
			conf.CacheFullBlock = true
			stores = append(stores, NewCachedStore(storage, conf, nil))
		}
		run(t, stores)
	})
	t.Run("with", func(t *testing.T) {
		addrs := freeAddrs(t, members)
		run(t, groupStores(t, storage, strings.Join(addrs, ","), addrs))
	})
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func TestCacheGroupCountsCanceledReadsWithoutMarkingTheOwnerDown(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 13, 1
	writeSlice(t, storage, id, nBlocks)
	storage.delay = 500 * time.Millisecond // the owner's object-storage load outlives the caller

	addrs := freeAddrs(t, 2)
	stores := groupStores(t, storage, strings.Join(addrs, ","), addrs)
	key := fmt.Sprintf("chunks/0/0/%d_0_%d", id, defaultConf.BlockSize)
	var from, owner *cacheGroup
	for _, s := range stores {
		g := s.(*cachedStore).cacheGroup
		if g.owner(key) == g.self {
			owner = g
		} else {
			from = g
		}
	}
	if from == nil || owner == nil {
		t.Fatal("expected one owner and one other member")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := from.fetch(ctx, key, make([]byte, defaultConf.BlockSize)); err == nil {
		t.Fatal("fetch succeeded although the caller gave up")
	}
	if got := testutilCount(from, "canceled"); got != 1 {
		t.Fatalf("canceled = %v, want 1", got)
	}
	if from.isDown(owner.self) {
		t.Fatal("a canceled read marked the owner down")
	}
}

func TestCacheGroupOwnerCachesABlockItsAskerAbandoned(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 17, 1
	data := writeSlice(t, storage, id, nBlocks)
	storage.gets.Store(0)
	storage.delay = 300 * time.Millisecond

	addrs := freeAddrs(t, 2)
	stores := groupStores(t, storage, strings.Join(addrs, ","), addrs)
	key := fmt.Sprintf("chunks/0/0/%d_0_%d", id, defaultConf.BlockSize)
	var asker *cacheGroup
	for _, s := range stores {
		if g := s.(*cachedStore).cacheGroup; g.owner(key) != g.self {
			asker = g
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = asker.fetch(ctx, key, make([]byte, defaultConf.BlockSize)) // gives up long before the owner's load ends

	time.Sleep(500 * time.Millisecond) // the owner's load finishes and caches the block
	buf := make([]byte, defaultConf.BlockSize)
	if err := asker.fetch(context.Background(), key, buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("wrong data")
	}
	if got := testutilCount(asker, "hit"); got != 1 {
		t.Fatalf("second ask: hit = %v, want 1 (the owner should have kept the abandoned load)", got)
	}
	if got := storage.gets.Load(); got != 1 {
		t.Fatalf("object storage GETs = %d, want 1", got)
	}
}

func TestCacheGroupLocalCopy(t *testing.T) {
	for _, noLocalCopy := range []bool{false, true} {
		t.Run(fmt.Sprintf("noLocalCopy=%v", noLocalCopy), func(t *testing.T) {
			mem, _ := object.CreateStorage("mem", "", "", "", "")
			storage := &countingStorage{ObjectStorage: mem}
			const id, nBlocks = 19, 6
			data := writeSlice(t, storage, id, nBlocks)
			addrs := freeAddrs(t, 2)
			var stores []ChunkStore
			for _, a := range addrs {
				conf := defaultConf
				conf.CacheDir = groupCacheDir(t)
				conf.CacheSize = 100 << 20
				conf.CacheFullBlock = true
				conf.CacheGroupPeers = strings.Join(addrs, ",")
				conf.CacheGroupListen, conf.CacheGroupSelf = a, a
				conf.CacheGroupNoLocalCopy = noLocalCopy
				stores = append(stores, NewCachedStore(storage, conf, nil))
			}
			reader := stores[1].(*cachedStore)
			var remote []int // blocks the reader does not own
			for i := 0; i < nBlocks; i++ {
				if reader.cacheGroup.owner(fmt.Sprintf("chunks/0/0/%d_%d_%d", id, i, defaultConf.BlockSize)) != reader.cacheGroup.self {
					remote = append(remote, i)
				}
			}
			if len(remote) == 0 {
				t.Skip("the reader owns every block")
			}
			for round := 0; round < 2; round++ {
				for _, i := range remote {
					if got := readBlock(t, reader, id, len(data), i); !bytes.Equal(got, data[i*defaultConf.BlockSize:(i+1)*defaultConf.BlockSize]) {
						t.Fatalf("block %d: wrong data", i)
					}
				}
			}
			asks := testutilCount(reader.cacheGroup, "hit") + testutilCount(reader.cacheGroup, "loaded")
			want := float64(len(remote)) // first round only: the second reads the local copy
			if noLocalCopy {
				want = float64(2 * len(remote)) // every read asks the owner
			}
			if asks != want {
				t.Fatalf("asks of the owner = %v, want %v", asks, want)
			}
			for _, i := range remote {
				key := fmt.Sprintf("chunks/0/0/%d_%d_%d", id, i, defaultConf.BlockSize)
				if cached := reader.loadCached(key, make([]byte, defaultConf.BlockSize)); cached == noLocalCopy {
					t.Fatalf("block %d cached on the reader = %v with noLocalCopy=%v", i, cached, noLocalCopy)
				}
			}
		})
	}
}

// replicaStores starts one store per address in a cache group whose blocks each have replicas owners.
func replicaStores(t *testing.T, storage object.ObjectStorage, peers string, addrs []string, replicas int) []ChunkStore {
	t.Helper()
	var stores []ChunkStore
	for _, a := range addrs {
		conf := defaultConf
		conf.CacheDir = groupCacheDir(t)
		conf.CacheSize = 100 << 20
		conf.CacheFullBlock = true
		conf.CacheGroupPeers = peers
		conf.CacheGroupListen, conf.CacheGroupSelf = a, a
		conf.CacheGroupTimeout = 2 * time.Second
		conf.CacheGroupReplicas = replicas
		stores = append(stores, NewCachedStore(storage, conf, nil))
	}
	return stores
}

func servedCount(g *cacheGroup) float64 {
	total := 0.0
	for _, src := range []string{"cache", "object"} {
		m := &dto.Metric{}
		_ = g.served.WithLabelValues(src).Write(m)
		total += m.GetCounter().GetValue()
	}
	return total
}

func TestCacheGroupReplicasFetchOnceAndShareReads(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 23, 16
	data := writeSlice(t, storage, id, nBlocks)
	storage.gets.Store(0)
	storage.delay = 20 * time.Millisecond

	addrs := freeAddrs(t, 4)
	stores := replicaStores(t, storage, strings.Join(addrs, ","), addrs, 2)
	var wg sync.WaitGroup
	for _, s := range stores {
		for i := 0; i < nBlocks; i++ {
			wg.Add(1)
			go func(s ChunkStore, i int) {
				defer wg.Done()
				if got := readBlock(t, s, id, len(data), i); !bytes.Equal(got, data[i*defaultConf.BlockSize:(i+1)*defaultConf.BlockSize]) {
					t.Errorf("block %d: wrong data", i)
				}
			}(s, i)
		}
	}
	wg.Wait()
	if got := storage.gets.Load(); got != nBlocks {
		t.Fatalf("object storage GETs = %d, want %d (one per block across the group)", got, nBlocks)
	}
	// Both owners of a block serve readers: every member that owns some blocks as secondary served some of them.
	for _, s := range stores {
		g := s.(*cachedStore).cacheGroup
		secondary := 0
		for i := 0; i < nBlocks; i++ {
			if o := g.owners(fmt.Sprintf("chunks/0/0/%d_%d_%d", id, i, defaultConf.BlockSize)); len(o) == 2 && o[1] == g.self {
				secondary++
			}
		}
		if secondary >= 3 && servedCount(g) == 0 {
			t.Fatalf("%s is secondary owner of %d blocks but served none", g.self, secondary)
		}
	}
}

func TestCacheGroupReplicasSurviveADownOwner(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	storage := &countingStorage{ObjectStorage: mem}
	const id, nBlocks = 29, 12
	data := writeSlice(t, storage, id, nBlocks)
	storage.gets.Store(0)

	addrs := freeAddrs(t, 3) // the last member never starts, as if it were restarting
	stores := replicaStores(t, storage, strings.Join(addrs, ","), addrs[:2], 2)
	for round := 0; round < 2; round++ {
		for _, s := range stores {
			for i := 0; i < nBlocks; i++ {
				if got := readBlock(t, s, id, len(data), i); !bytes.Equal(got, data[i*defaultConf.BlockSize:(i+1)*defaultConf.BlockSize]) {
					t.Fatalf("block %d: wrong data", i)
				}
			}
		}
	}
	// Every block has a live owner, so the group still reads each block from object storage once.
	if got := storage.gets.Load(); got != nBlocks {
		t.Fatalf("object storage GETs = %d, want %d", got, nBlocks)
	}
}
