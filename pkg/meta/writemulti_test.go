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

package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

func TestWriteMultiRedis(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	if err != nil || m.Name() != "redis" {
		t.Fatalf("create meta: %s", err)
	}
	wmFlaky(m)
	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Errorf("shutdown: %s", err)
		}
	})
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	testWriteMulti(t, m)
}

func TestCompactionAfterWrite(t *testing.T) {
	for _, interval := range []int{100, 200, 250, 350} {
		t.Run(strconv.Itoa(interval), func(t *testing.T) {
			t.Setenv("JFS_WRITE_COMPACTION_INTERVAL", strconv.Itoa(interval))
			m := newBaseMeta("", testConfig())
			// One slice at a time the rule is Write's shouldStartWriteCompaction, blocking from maxSlices on.
			for n := 0; n <= 3000; n++ {
				background, blocking := m.compactionAfterWrite(n-1, n)
				want := m.shouldStartWriteCompaction(n)
				if background != (want && n < maxSlices) || blocking != (want && n >= maxSlices) {
					t.Fatalf("one slice to %d: background %v, blocking %v", n, background, blocking)
				}
			}
			b := interval - 1 // the first count at which Write compacts
			for _, c := range []struct {
				before, after        int
				background, blocking bool
			}{
				{b - 1, b + 2, true, false}, // steps over interval-1, which shouldStartWriteCompaction misses
				{b - 9, b - 6, false, false},
				{b, min(b+6, 350), false, false},           // interval-1 was reached by an earlier write
				{b - 5, min(b+interval, 350), true, false}, // steps over interval-1, and maybe 2*interval-1
				{300, 350, interval == 350, false},         // 350 is not past 350; only 349 is a boundary in (300, 350]
				{340, 351, true, false},
				{2497, 2499, true, false},
				{2498, 2503, false, true}, // reaches maxSlices
				{-1, 0, false, false},     // nothing counted
			} {
				background, blocking := m.compactionAfterWrite(c.before, c.after)
				if background != c.background || blocking != c.blocking {
					t.Fatalf("%d -> %d slices: background %v, blocking %v; want %v, %v",
						c.before, c.after, background, blocking, c.background, c.blocking)
				}
			}
		})
	}
}

func TestSortSliceWrites(t *testing.T) {
	s := func(id uint64) Slice { return Slice{Id: id, Size: 4 << 10, Len: 4 << 10} }
	in := []SliceWrite{{2, 0, s(1)}, {0, 0, s(2)}, {2, 4 << 10, s(3)}, {1, 0, s(4)}, {0, 4 << 10, s(5)}}
	sorted, added, st := sortSliceWrites(in)
	if st != 0 {
		t.Fatalf("sort: %s", st)
	}
	var ids []uint64
	for _, w := range sorted {
		ids = append(ids, w.Slice.Id)
	}
	if !reflect.DeepEqual(ids, []uint64{2, 5, 4, 1, 3}) { // by chunk, in the given order inside a chunk
		t.Fatalf("sorted ids %v", ids)
	}
	if !reflect.DeepEqual(added, map[uint32]int{0: 2, 1: 1, 2: 2}) {
		t.Fatalf("slices per chunk %v", added)
	}
	if in[0].Slice.Id != 1 {
		t.Fatalf("the caller's batch was reordered: %v", in)
	}
	if g := groupSliceWrites(sorted); !reflect.DeepEqual(g, []sliceWriteGroup{{0, 0, 2}, {1, 2, 3}, {2, 3, 5}}) {
		t.Fatalf("groups %v", g)
	}
	if end := sliceWritesEnd(in); end != 2*ChunkSize+8<<10 {
		t.Fatalf("end %d", end)
	}
	if _, _, st := sortSliceWrites([]SliceWrite{{0, ChunkSize - 4<<10, s(1)}}); st != 0 {
		t.Fatalf("a slice that ends at the end of the chunk: %s", st)
	}
	for name, w := range map[string][]SliceWrite{
		"zero id":         {{0, 0, s(1)}, {0, 4 << 10, s(0)}},
		"empty":           {{0, 0, Slice{Id: 1, Size: 4 << 10}}},
		"past the chunk":  {{0, ChunkSize - 4<<10, Slice{Id: 1, Size: 8 << 10, Len: 8 << 10}}},
		"past the object": {{0, 0, Slice{Id: 1, Size: 4 << 10, Off: 4 << 10, Len: 4 << 10}}},
		"same id twice":   {{0, 0, s(1)}, {1, 0, s(1)}},
	} {
		if _, _, st := sortSliceWrites(w); st != syscall.EINVAL {
			t.Fatalf("%s: %s, want EINVAL", name, st)
		}
	}
}

// limitsEngine is an engine whose writeMultiLimits are l (the zero value: no WriteMulti).
type limitsEngine struct {
	engine
	l WriteMultiLimits
}

func (e limitsEngine) writeMultiLimits() WriteMultiLimits { return e.l }

func TestWriteMultiLimits(t *testing.T) {
	// One call adds at most what one compaction absorbs to a chunk, whatever the engine allows.
	m := &baseMeta{en: limitsEngine{l: defaultWriteMultiLimits}}
	want := WriteMultiLimits{Slices: writeMultiMaxSlices, Chunks: writeMultiMaxChunks, ChunkSlices: maxCompactSlices - 1}
	if l := m.WriteMultiLimits(); l != want {
		t.Fatalf("limits %+v, want %+v", l, want)
	}
	m.en = limitsEngine{l: WriteMultiLimits{Slices: 1024, Chunks: 16, ChunkSlices: 230}}
	if l := m.WriteMultiLimits(); l.ChunkSlices != 230 {
		t.Fatalf("limits %+v, want the engine's 230 slices per chunk", l)
	}
	m.en = limitsEngine{l: WriteMultiLimits{Slices: 1024, Chunks: 16}}
	if l := m.WriteMultiLimits(); l != (WriteMultiLimits{}) {
		t.Fatalf("limits %+v for an engine with no slices per chunk", l)
	}
	// MySQL: a chunk of maxSlices (Write stops there) plus one call must fit in the BLOB column.
	chunkSlices := mysqlChunkValueMax/sliceBytes - maxSlices
	if chunkSlices <= 0 || (maxSlices+chunkSlices)*sliceBytes > mysqlChunkValueMax || (maxSlices+chunkSlices+1)*sliceBytes <= mysqlChunkValueMax {
		t.Fatalf("MySQL: %d slices per chunk per call", chunkSlices)
	}
}

func TestWriteMultiUnsupported(t *testing.T) {
	m := &baseMeta{en: limitsEngine{}}
	if l := m.WriteMultiLimits(); l != (WriteMultiLimits{}) {
		t.Fatalf("limits %+v", l)
	}
	w := []SliceWrite{{0, 0, Slice{Id: 1, Size: 4 << 10, Len: 4 << 10}}, {1, 0, Slice{Id: 2, Size: 4 << 10, Len: 4 << 10}}}
	if st := m.WriteMulti(Background(), 2, w, time.Now()); st != syscall.ENOTSUP {
		t.Fatalf("WriteMulti: %s, want ENOTSUP", st)
	}
}

// testWriteMulti covers WriteMulti on one engine. It runs from testMeta and from TestWriteMulti<engine>.
func testWriteMulti(t *testing.T, m Meta) {
	// A commit that fails with EIO in a test (a corrupt chunk, a lost reply) is resent without the backoff.
	backoff, maxBackoff := writeMultiResendBackoff, writeMultiResendMaxBackoff
	writeMultiResendBackoff, writeMultiResendMaxBackoff = time.Millisecond, time.Millisecond
	defer func() { writeMultiResendBackoff, writeMultiResendMaxBackoff = backoff, maxBackoff }()
	wmWaitBackground(t, m) // Init replaces the format that background tasks of the tests before read
	if err := m.Init(testFormat(), false); err != nil {
		t.Fatalf("init: %s", err)
	}
	if err := m.NewSession(false); err != nil {
		t.Fatalf("new session: %s", err)
	}
	defer func() {
		// Leave no background work to the tests after this one: closing the session can start deletions
		// of the files that were still open when they were unlinked, and the next NewSession replaces
		// the channel those deletions use.
		wmWaitBackground(t, m)
		if err := m.CloseSession(); err != nil {
			t.Errorf("close session: %s", err)
		}
		wmWaitBackground(t, m)
	}()
	rec := &compactionRecorder{made: make(map[uint64][]Slice), inputs: make(map[uint64]bool)}
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	m.OnMsg(CompactChunk, rec.handle)

	t.Run("order", func(t *testing.T) { testWriteMultiOrder(t, m) })
	t.Run("single", func(t *testing.T) { testWriteMultiSingle(t, m) })
	t.Run("invalid", func(t *testing.T) { testWriteMultiInvalid(t, m) })
	t.Run("accounting", func(t *testing.T) { testWriteMultiAccounting(t, m) })
	t.Run("quota", func(t *testing.T) { testWriteMultiQuota(t, m) })
	t.Run("userquota", func(t *testing.T) { testWriteMultiUserQuota(t, m) })
	t.Run("capacity", func(t *testing.T) { testWriteMultiCapacity(t, m) })
	t.Run("filelock", func(t *testing.T) { testWriteMultiFileLock(t, m) })
	t.Run("compaction", func(t *testing.T) { testWriteMultiCompaction(t, m, rec) })
	t.Run("maxslices", func(t *testing.T) { testWriteMultiMaxSlices(t, m, rec) })
	switch m.(type) {
	case *redisMeta, *kvMeta: // the engines whose transactions fail on a conflict at commit
		t.Run("conflict", func(t *testing.T) { testWriteMultiConflict(t, m) })
	}
	switch m.(type) {
	case *dbMeta, *kvMeta: // the engines that refuse to append to a corrupt chunk value
		t.Run("rollback", func(t *testing.T) { testWriteMultiRollback(t, m) })
	}
	t.Run("interleave", func(t *testing.T) { testWriteMultiInterleave(t, m, rec) })
	t.Run("race", func(t *testing.T) {
		// A second client compacts, as another mount would: a client runs the transactions of one inode one
		// at a time on SQL and TKV (txBatchLock), so its own compactions would never race with its writes.
		twin := wmOpenTwin(t, m, nil)
		twin.OnMsg(CompactChunk, rec.handle)
		c, ok := twin.(compactor)
		if !ok {
			t.Fatalf("%T has no compactChunk", twin)
		}
		for _, seed := range []int64{1, 2, time.Now().UnixNano()} {
			testWriteMultiRace(t, m, c, rec, seed)
		}
	})
	if km, ok := m.(*kvMeta); ok {
		t.Run("idempotent", func(t *testing.T) { testWriteMultiIdempotent(t, km) })
	}
	if e, ok := m.getBase().en.(*wmFlakyEngine); ok {
		t.Run("resend", func(t *testing.T) { testWriteMultiResend(t, m, e) })
	}
}

// wmFlakyEngine is an engine whose doWriteMulti and doWrite fail with EIO, as a lost connection makes them fail, when
// the test says so: plan holds what each of the next calls does, "landed" (it commits, and its reply is lost) or
// "lost" (its request is: nothing commits). The other calls go to the engine. The TestWriteMulti<engine> tests put it
// in front of the engine when they create the client, before any of its background work reads baseMeta.en.
type wmFlakyEngine struct {
	engine
	mu     sync.Mutex
	plan   []string
	sinces []time.Time // the since of each doWriteMulti call (zero for a first send), and zero for each doWrite
	onLost func()      // runs as a "lost" call fails
}

func wmFlaky(m Meta) {
	b := m.getBase()
	b.en = &wmFlakyEngine{engine: b.en}
}

func (e *wmFlakyEngine) next(since time.Time) (string, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.plan) == 0 {
		return "", nil
	}
	act := e.plan[0]
	e.plan = e.plan[1:]
	e.sinces = append(e.sinces, since)
	return act, e.onLost
}

func (e *wmFlakyEngine) fail(act string, onLost func(), commit func() syscall.Errno) syscall.Errno {
	switch act {
	case "lost":
		if onLost != nil {
			onLost()
		}
		return syscall.EIO
	case "landed":
		if st := commit(); st != 0 {
			return st
		}
		return syscall.EIO
	}
	return commit()
}

func (e *wmFlakyEngine) doWriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime, since time.Time, counts map[uint32]int, delta *dirStat, attr *Attr) syscall.Errno {
	act, onLost := e.next(since)
	return e.fail(act, onLost, func() syscall.Errno {
		return e.engine.doWriteMulti(ctx, inode, writes, mtime, since, counts, delta, attr)
	})
}

func (e *wmFlakyEngine) doWrite(ctx Context, inode Ino, indx uint32, off uint32, slice Slice, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
	act, onLost := e.next(time.Time{})
	return e.fail(act, onLost, func() syscall.Errno {
		return e.engine.doWrite(ctx, inode, indx, off, slice, mtime, numSlices, delta, attr)
	})
}

// set queues the plan of the next calls, and what runs as a "lost" one fails.
func (e *wmFlakyEngine) set(t *testing.T, onLost func(), plan ...string) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.plan) != 0 {
		t.Fatalf("%d planned calls were not made: %v", len(e.plan), e.plan)
	}
	e.plan, e.sinces, e.onLost = plan, nil, onLost
}

// calls returns the since of each call made since set; the plan must have been used up.
func (e *wmFlakyEngine) calls(t *testing.T) []time.Time {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.plan) != 0 {
		t.Fatalf("%d planned calls were not made: %v", len(e.plan), e.plan)
	}
	return e.sinces
}

// testWriteMultiResend covers resendWriteMulti: a commit that fails with an error that can follow a commit that landed
// is sent again, which finds the batch if it landed, writes it if it did not, and gives up if it cannot tell.
func testWriteMultiResend(t *testing.T, m Meta, e *wmFlakyEngine) {
	ctx := Background()
	tries := writeMultiResendTries
	t.Cleanup(func() { writeMultiResendTries = tries })
	for _, tc := range []struct {
		name   string
		plan   []string
		single bool // a batch of one slice: its first send is a Write
	}{
		{name: "landed", plan: []string{"landed"}},
		{name: "lost", plan: []string{"lost"}},
		{name: "lost then landed", plan: []string{"lost", "landed", "lost"}},
		{name: "single landed", plan: []string{"landed"}, single: true},
		{name: "single lost", plan: []string{"lost"}, single: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := wmMkdir(t, m, RootInode, "wm_resend")
			inode := wmCreate(t, m, dir, "f")
			batch := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 8 << 10, wmSlice(t, m, 4<<10)}}
			if tc.single {
				batch = batch[1:]
			}
			e.set(t, nil, tc.plan...)
			start := time.Now()
			if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
				t.Fatalf("WriteMulti: %s", st)
			}
			sinces := e.calls(t)
			for i, since := range sinces {
				if first := i == 0; first != since.IsZero() || !first && since.Before(start) {
					t.Fatalf("call %d had since %s (the first send at %s), want a first send and then resends", i, since, start)
				}
			}
			if !tc.single {
				wmCheckChunk(t, m, inode, 0, batch[0])
			}
			wmCheckChunk(t, m, inode, 1, batch[len(batch)-1])
			length := uint64(ChunkSize + 12<<10)
			if l := wmLength(t, m, inode); l != length {
				t.Fatalf("length %d, want %d", l, length)
			}
			if tc.single && tc.plan[0] == "landed" {
				// The Write that landed lost its count with its reply: the batch's growth is missing from the dir
				// stats and quotas (and on SQL and TKV from the used space).
				return
			}
			if err := waitCheckResult(m, dirStat{length: int64(length), space: align4K(length), inodes: 1}, func() (*dirStat, syscall.Errno) {
				return m.GetDirStat(ctx, dir)
			}); err != nil {
				t.Fatalf("dir stat: %s", err)
			}
		})
	}
	t.Run("never answers", func(t *testing.T) {
		writeMultiResendTries = 3
		inode := wmCreate(t, m, RootInode, "wm_resend_down")
		batch := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}
		e.set(t, nil, "lost", "lost", "lost", "lost")
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != syscall.EIO {
			t.Fatalf("WriteMulti: %s, want EIO", st)
		}
		if n := len(e.calls(t)); n != 4 {
			t.Fatalf("%d sends, want the first and 3 more", n)
		}
		wmCheckChunk(t, m, inode, 0)
		wmCheckChunk(t, m, inode, 1)
		writeMultiResendTries = tries
	})
	t.Run("inode changed", func(t *testing.T) {
		// The batch did not land, but the inode changed after it was sent: it could have landed and been compacted
		// away since, so it is not sent again (nothing would find it), and the commit fails.
		inode := wmCreate(t, m, RootInode, "wm_resend_changed")
		batch := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}
		e.set(t, func() {
			if st := m.SetAttr(ctx, inode, SetAttrMode, 0, &Attr{Mode: 0600}); st != 0 {
				t.Errorf("chmod: %s", st)
			}
		}, "lost")
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != syscall.EIO {
			t.Fatalf("WriteMulti: %s, want EIO", st)
		}
		if n := len(e.calls(t)); n != 1 {
			t.Fatalf("%d sends, want 1 (the resend found the inode changed and did not write)", n)
		}
		wmCheckChunk(t, m, inode, 0)
		wmCheckChunk(t, m, inode, 1)
	})
	t.Run("refused", func(t *testing.T) {
		// An error from a check before the commit is final: the batch did not land.
		dir := wmMkdir(t, m, RootInode, "wm_resend_refused")
		e.set(t, nil, "", "a resend")
		if st := m.WriteMulti(ctx, dir, []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}, time.Now()); st != syscall.EPERM {
			t.Fatalf("WriteMulti of a directory: %s, want EPERM", st)
		}
		e.mu.Lock()
		left := len(e.plan)
		e.plan = nil
		e.mu.Unlock()
		if left != 1 {
			t.Fatalf("a refused commit was sent again")
		}
	})
}

var errWMCompactionFails = errors.New("the test fails this compaction")

// compactionRecorder is a CompactChunk handler that writes no data but records what each compacted slice
// is made of, so a test can map the slices of a compacted chunk back to the slices it wrote. A test can
// make it fail (as a compaction whose data could not be written) or take some time.
type compactionRecorder struct {
	sync.Mutex
	made   map[uint64][]Slice // compacted slice id -> the data it holds, in order (id 0: a hole)
	inputs map[uint64]bool    // slice ids that went into a compaction
	fail   bool
	delay  time.Duration
}

func (r *compactionRecorder) handle(args ...interface{}) error {
	ss, ok1 := args[0].([]Slice)
	id, ok2 := args[1].(uint64)
	if !ok1 || !ok2 {
		return fmt.Errorf("unexpected CompactChunk arguments %v", args)
	}
	r.Lock()
	fail, delay := r.fail, r.delay
	r.Unlock()
	time.Sleep(delay)
	if fail {
		return errWMCompactionFails
	}
	r.Lock()
	defer r.Unlock()
	r.made[id] = append([]Slice(nil), ss...)
	for _, s := range ss {
		r.inputs[s.Id] = true
	}
	return nil
}

// set makes the compactions fail or take delay from now on, until the test ends.
func (r *compactionRecorder) set(t *testing.T, fail bool, delay time.Duration) {
	r.Lock()
	defer r.Unlock()
	r.fail, r.delay = fail, delay
	t.Cleanup(func() {
		r.Lock()
		defer r.Unlock()
		r.fail, r.delay = false, 0
	})
}

func (r *compactionRecorder) compacted(id uint64) bool {
	r.Lock()
	defer r.Unlock()
	return r.inputs[id]
}

func (r *compactionRecorder) waitCompacted(id uint64, timeout time.Duration) bool {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if r.compacted(id) {
			return true
		}
	}
	return r.compacted(id)
}

func (r *compactionRecorder) count() int {
	r.Lock()
	defer r.Unlock()
	return len(r.made)
}

// wmBlock is what a 4 KiB block of a file holds: the data of slice id from byte off (id 0: a hole).
type wmBlock struct {
	id  uint64
	off uint32
}

// resolve maps byte off of slice id to the written (not compacted) slice that holds it.
func (r *compactionRecorder) resolve(id uint64, off uint32) (wmBlock, bool) {
	r.Lock()
	defer r.Unlock()
	for depth := 0; depth < 10000; depth++ {
		parts, ok := r.made[id]
		if !ok {
			return wmBlock{id, off}, true
		}
		var pos uint32
		found := false
		for _, p := range parts {
			if off < pos+p.Len {
				if p.Id == 0 {
					return wmBlock{}, true
				}
				id, off, found = p.Id, p.Off+off-pos, true
				break
			}
			pos += p.Len
		}
		if !found {
			return wmBlock{}, false
		}
	}
	return wmBlock{}, false
}

// wmWaitBackground waits for the background compactions and file data deletions of m to finish.
func wmWaitBackground(t *testing.T, m Meta) {
	base := m.getBase()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		base.Lock()
		compacting := len(base.compacting)
		base.Unlock()
		deleting := len(base.maxDeleting)
		if compacting == 0 && deleting == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("%d compactions and %d file deletions still running", compacting, deleting)
			return
		}
	}
}

// wmCounters returns the values of a counter by the value of its first label.
func wmCounters(t *testing.T, c prometheus.Collector) map[string]float64 {
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	values := make(map[string]float64)
	for mt := range ch {
		var d dto.Metric
		if err := mt.Write(&d); err != nil {
			t.Errorf("read metric: %s", err)
			continue
		}
		var label string
		if len(d.GetLabel()) > 0 {
			label = d.GetLabel()[0].GetValue()
		}
		values[label] += d.GetCounter().GetValue()
	}
	return values
}

func wmNewID(t *testing.T, m Meta) uint64 {
	var id uint64
	if st := m.NewSlice(Background(), &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	return id
}

func wmSlice(t *testing.T, m Meta, size uint32) Slice {
	return Slice{Id: wmNewID(t, m), Size: size, Len: size}
}

// wmMkdir creates an empty directory that is removed when the test ends.
func wmMkdir(t *testing.T, m Meta, parent Ino, name string) Ino {
	ctx := Background()
	if st := m.Remove(ctx, parent, name, true, 10, nil); st != 0 && st != syscall.ENOENT {
		t.Fatalf("remove %s: %s", name, st)
	}
	var inode Ino
	var attr Attr
	if st := m.Mkdir(ctx, parent, name, 0755, 0, 0, &inode, &attr); st != 0 {
		t.Fatalf("mkdir %s: %s", name, st)
	}
	t.Cleanup(func() {
		if st := m.Rmdir(ctx, parent, name); st != 0 {
			t.Errorf("rmdir %s: %s", name, st)
		}
	})
	return inode
}

// wmCreate creates an empty file that is removed when the test ends.
func wmCreate(t *testing.T, m Meta, parent Ino, name string) Ino {
	ctx := Background()
	if st := m.Unlink(ctx, parent, name); st != 0 && st != syscall.ENOENT {
		t.Fatalf("unlink %s: %s", name, st)
	}
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, parent, name, 0644, 022, 0, &inode, &attr); st != 0 {
		t.Fatalf("create %s: %s", name, st)
	}
	t.Cleanup(func() {
		if st := m.Unlink(ctx, parent, name); st != 0 {
			t.Errorf("unlink %s: %s", name, st)
		}
	})
	return inode
}

// wmCheckChunk checks that the stored slice list of a chunk is exactly want, in order.
func wmCheckChunk(t *testing.T, m Meta, inode Ino, indx uint32, want ...SliceWrite) {
	t.Helper()
	ss, st := m.getBase().en.doRead(Background(), inode, indx)
	if st != 0 {
		t.Fatalf("read chunk %d: %s", indx, st)
	}
	var got, exp []string
	for _, s := range ss {
		got = append(got, fmt.Sprintf("%d:%d:%d:%d:%d", s.pos, s.id, s.size, s.off, s.len))
	}
	for _, w := range want {
		exp = append(exp, fmt.Sprintf("%d:%d:%d:%d:%d", w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len))
	}
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("chunk %d holds %v, want %v", indx, got, exp)
	}
}

func wmLength(t *testing.T, m Meta, inode Ino) uint64 {
	t.Helper()
	var attr Attr
	if st := m.GetAttr(Background(), inode, &attr); st != 0 {
		t.Fatalf("getattr %d: %s", inode, st)
	}
	return attr.Length
}

// wmUsedSpace returns the used space of the volume as the store counts it, after m flushed what it counted
// in memory (SQL and TKV; Redis counts it in the write transactions).
func wmUsedSpace(t *testing.T, m Meta) int64 {
	t.Helper()
	b := m.getBase()
	b.fsStatsLock.Lock()
	defer b.fsStatsLock.Unlock()
	b.en.doFlushStats()
	v, err := b.en.getCounter(usedSpace)
	if err != nil {
		t.Fatalf("read the used space: %s", err)
	}
	return v + atomic.LoadInt64(&b.newSpace)
}

// wmSliceRef returns the chunk_ref row of a slice on SQL, whether there is one.
func wmSliceRef(t *testing.T, m *dbMeta, id uint64) (sliceRef, bool) {
	t.Helper()
	ref := sliceRef{Id: id}
	ok, err := m.db.Get(&ref)
	if err != nil {
		t.Fatalf("read the chunk_ref of slice %d: %s", id, err)
	}
	return ref, ok
}

// wmOpenTwin opens a second client of the store m uses, as another mount would. On TKV it shares m's client
// object, whose transactions are the store's, and on SQL m's connection pool (but SQLite, which gets its
// own connections); on Redis it has its own connections. prepare, if not nil, gets the twin before its
// first use. The twin has no session; when the test ends it flushes the usage it counted in memory and
// is closed.
func wmOpenTwin(t *testing.T, m Meta, prepare func(Meta)) Meta {
	t.Helper()
	var twin Meta
	own := true // whether the twin has connections of its own, to close
	switch mm := m.(type) {
	case *kvMeta:
		conf := *mm.conf
		k := &kvMeta{baseMeta: newBaseMeta(mm.addr, &conf), client: mm.client}
		k.en = k
		twin, own = k, false
	case *redisMeta:
		conf := *mm.conf
		r, err := newRedisMeta("redis", mm.addr, &conf)
		if err != nil {
			t.Fatalf("open a second client: %s", err)
		}
		twin = r
	case *dbMeta:
		conf := *mm.conf
		if mm.Name() == "sqlite3" { // the data source name is the path with its options
			d, err := newSQLMeta("sqlite3", mm.addr, &conf)
			if err != nil {
				t.Fatalf("open a second client: %s", err)
			}
			twin = d
		} else { // the data source name may have lost the password
			d := &dbMeta{baseMeta: newBaseMeta(mm.addr, &conf), db: mm.db, spool: mm.spool, statement: mm.statement, tablePrefix: mm.tablePrefix}
			d.en = d
			twin, own = d, false
		}
	default:
		t.Fatalf("no second client for %T", m)
	}
	if prepare != nil {
		prepare(twin)
	}
	t.Cleanup(func() {
		wmWaitBackground(t, twin)
		b := twin.getBase()
		b.doFlushStats()
		b.doFlushDirStat()
		if !own {
			b.of.close()
		} else if err := twin.Shutdown(); err != nil {
			t.Errorf("shut down the second client: %s", err)
		}
	})
	if _, err := twin.Load(false); err != nil {
		t.Fatalf("load the format in a second client: %s", err)
	}
	twin.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	return twin
}

// wmReinit replaces the format of m. Init replaces the format that background tasks read without a lock,
// so it runs between sessions, once the compactions and file deletions are done (closing the session can
// start deletions of the files that were still open when they were unlinked).
func wmReinit(t *testing.T, m Meta, format *Format) error {
	wmWaitBackground(t, m)
	if err := m.CloseSession(); err != nil {
		return fmt.Errorf("close session: %w", err)
	}
	wmWaitBackground(t, m)
	if err := m.Init(format, false); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	return m.NewSession(false)
}

func testWriteMultiOrder(t *testing.T, m Meta) {
	ctx := Background()
	dir := wmMkdir(t, m, RootInode, "wm_order")
	inode := wmCreate(t, m, dir, "f")
	pre := SliceWrite{1, 0, wmSlice(t, m, 4<<10)}
	if st := m.Write(ctx, inode, pre.Indx, pre.Off, pre.Slice, time.Now()); st != 0 {
		t.Fatalf("write: %s", st)
	}
	a := SliceWrite{0, 0, wmSlice(t, m, 64<<10)}
	b := SliceWrite{3, 1 << 20, wmSlice(t, m, 8<<10)}
	c := SliceWrite{0, 32 << 10, wmSlice(t, m, 64<<10)} // overlaps a and comes after it
	d := SliceWrite{1, 4 << 10, Slice{Id: wmNewID(t, m), Size: 16 << 10, Off: 4 << 10, Len: 4 << 10}}
	mtime := time.Unix(1700000000, 123456789)
	if st := m.WriteMulti(ctx, inode, []SliceWrite{a, b, c, d}, mtime); st != 0 {
		t.Fatalf("WriteMulti: %s", st)
	}
	wmCheckChunk(t, m, inode, 0, a, c)
	wmCheckChunk(t, m, inode, 1, pre, d)
	wmCheckChunk(t, m, inode, 2)
	wmCheckChunk(t, m, inode, 3, b)
	var ss []Slice
	if st := m.Read(ctx, inode, 0, &ss); st != 0 {
		t.Fatalf("read: %s", st)
	}
	want := []Slice{{Id: a.Slice.Id, Size: 64 << 10, Len: 32 << 10}, {Id: c.Slice.Id, Size: 64 << 10, Len: 64 << 10}}
	if !reflect.DeepEqual(ss, want) {
		t.Fatalf("chunk 0 reads %+v, want %+v", ss, want)
	}
	var attr Attr
	if st := m.GetAttr(ctx, inode, &attr); st != 0 {
		t.Fatalf("getattr: %s", st)
	}
	length := uint64(3*ChunkSize + 1<<20 + 8<<10)
	if attr.Length != length || attr.Mtime != mtime.Unix() || attr.Mtimensec != uint32(mtime.Nanosecond()) {
		t.Fatalf("length %d mtime %d.%09d, want %d %d.%09d", attr.Length, attr.Mtime, attr.Mtimensec, length, mtime.Unix(), mtime.Nanosecond())
	}
	// The parent's usage is what the same slices written one by one would give.
	if err := waitCheckResult(m, dirStat{length: int64(length), space: align4K(length), inodes: 1}, func() (*dirStat, syscall.Errno) {
		return m.GetDirStat(ctx, dir)
	}); err != nil {
		t.Fatalf("dir stat: %s", err)
	}

	// The engine reports each chunk's slice count after the batch.
	more, _, st := sortSliceWrites([]SliceWrite{{0, 128 << 10, wmSlice(t, m, 4<<10)}, {1, 8 << 10, wmSlice(t, m, 4<<10)}, {0, 192 << 10, wmSlice(t, m, 4<<10)}})
	if st != 0 {
		t.Fatalf("sort: %s", st)
	}
	counts := make(map[uint32]int)
	var delta dirStat
	if st := m.getBase().en.doWriteMulti(ctx, inode, more, time.Now(), time.Time{}, counts, &delta, &attr); st != 0 {
		t.Fatalf("doWriteMulti: %s", st)
	}
	if !reflect.DeepEqual(counts, map[uint32]int{0: 4, 1: 3}) || delta != (dirStat{}) {
		t.Fatalf("counts %v delta %+v", counts, delta)
	}
	wmCheckChunk(t, m, inode, 0, a, c, more[0], more[1])
	wmCheckChunk(t, m, inode, 1, pre, d, more[2])

	// A batch long enough that sorting it by chunk with an unstable sort would reorder a chunk's slices
	// (sort.Slice sorts up to 12 elements by insertion, which is stable), and that SQL inserts its
	// chunk_ref rows with more than one statement.
	big := make([]SliceWrite, 250)
	per := make(map[uint32][]SliceWrite)
	for i := range big {
		indx := uint32(4 + i%3)
		big[i] = SliceWrite{indx, uint32(i) << 12, wmSlice(t, m, 4<<10)}
		per[indx] = append(per[indx], big[i])
	}
	if st := m.WriteMulti(ctx, inode, big, time.Now()); st != 0 {
		t.Fatalf("WriteMulti of %d slices: %s", len(big), st)
	}
	for indx, want := range per {
		wmCheckChunk(t, m, inode, indx, want...)
	}

	// SQL keeps a reference count per slice: one row each, as Write inserts.
	if dm, ok := m.(*dbMeta); ok {
		for _, w := range append([]SliceWrite{a, b, c, d}, append(more, big...)...) {
			if ref, ok := wmSliceRef(t, dm, w.Slice.Id); !ok || ref.Size != w.Slice.Size || ref.Refs != 1 {
				t.Fatalf("chunk_ref of slice %d: %+v (found: %v), want size %d and 1 reference", w.Slice.Id, ref, ok, w.Slice.Size)
			}
		}
	}
}

func testWriteMultiSingle(t *testing.T, m Meta) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, "wm_single")
	if st := m.WriteMulti(ctx, inode, nil, time.Now()); st != 0 {
		t.Fatalf("empty WriteMulti: %s", st)
	}
	w := SliceWrite{2, 4 << 10, wmSlice(t, m, 8<<10)}
	if st := m.WriteMulti(ctx, inode, []SliceWrite{w}, time.Now()); st != 0 {
		t.Fatalf("WriteMulti: %s", st)
	}
	wmCheckChunk(t, m, inode, 2, w)
	if l := wmLength(t, m, inode); l != 2*ChunkSize+12<<10 {
		t.Fatalf("length %d", l)
	}
}

func testWriteMultiInvalid(t *testing.T, m Meta) {
	ctx := Background()
	dir := wmMkdir(t, m, RootInode, "wm_invalid")
	inode := wmCreate(t, m, dir, "f")
	ok := func(indx uint32) SliceWrite { return SliceWrite{indx, 0, wmSlice(t, m, 4<<10)} }
	dup := ok(0)
	for name, w := range map[string][]SliceWrite{
		"zero id":         {ok(0), {1, 0, Slice{Size: 4 << 10, Len: 4 << 10}}},
		"empty":           {ok(0), {1, 0, Slice{Id: wmNewID(t, m), Size: 4 << 10}}},
		"past the chunk":  {ok(0), {1, ChunkSize - 4<<10, wmSlice(t, m, 8<<10)}},
		"past the object": {ok(0), {1, 0, Slice{Id: wmNewID(t, m), Size: 4 << 10, Off: 4 << 10, Len: 4 << 10}}},
		"same id twice":   {dup, {1, 0, dup.Slice}},
	} {
		if st := m.WriteMulti(ctx, inode, w, time.Now()); st != syscall.EINVAL {
			t.Fatalf("%s: %s, want EINVAL", name, st)
		}
	}
	limits := m.WriteMultiLimits()
	if limits.Slices <= 0 || limits.Chunks <= 0 || limits.ChunkSlices <= 0 || limits.ChunkSlices >= maxCompactSlices {
		t.Fatalf("limits %+v", limits)
	}
	used := wmUsedSpace(t, m)
	// The limits are checked before anything is written, so these ids need not come from NewSlice.
	fake := func(i int) Slice { return Slice{Id: 1<<50 + uint64(i), Size: 4 << 10, Len: 4 << 10} }
	over := make(map[string][]SliceWrite)
	// One slice more than a call takes, in as few chunks as the other two limits allow (not on etcd,
	// where 16 chunks of 99 slices are fewer than 4097).
	if n, k := limits.Slices+1, (limits.Slices+limits.ChunkSlices)/limits.ChunkSlices; k <= limits.Chunks {
		for i := 0; i < n; i++ {
			over["slices"] = append(over["slices"], SliceWrite{uint32(i % k), uint32((i/k)%1024) << 12, fake(i)})
		}
	}
	for i := 0; i <= limits.Chunks; i++ {
		over["chunks"] = append(over["chunks"], SliceWrite{uint32(i), 0, fake(i)})
	}
	for i := 0; i <= limits.ChunkSlices; i++ {
		over["slices in one chunk"] = append(over["slices in one chunk"], SliceWrite{0, uint32(i%1024) << 12, fake(i)})
	}
	over["slices in one chunk"] = append(over["slices in one chunk"], SliceWrite{1, 0, fake(limits.ChunkSlices + 1)})
	for name, w := range over {
		if st := m.WriteMulti(ctx, inode, w, time.Now()); st != syscall.E2BIG {
			t.Fatalf("too many %s (%d slices): %s, want E2BIG", name, len(w), st)
		}
	}
	for indx := uint32(0); indx <= uint32(limits.Chunks); indx++ {
		wmCheckChunk(t, m, inode, indx)
	}
	if l := wmLength(t, m, inode); l != 0 {
		t.Fatalf("length %d after failed batches", l)
	}
	if u := wmUsedSpace(t, m); u != used {
		t.Fatalf("used space %d after failed batches, %d before", u, used)
	}
	if st := m.WriteMulti(ctx, dir, []SliceWrite{ok(0), ok(1)}, time.Now()); st != syscall.EPERM {
		t.Fatalf("WriteMulti on a directory: %s, want EPERM", st)
	}
	if st := m.WriteMulti(ctx, Ino(1<<50), []SliceWrite{ok(0), ok(1)}, time.Now()); st != syscall.ENOENT {
		t.Fatalf("WriteMulti on a missing inode: %s, want ENOENT", st)
	}
}

func testWriteMultiQuota(t *testing.T, m Meta) {
	ctx := Background()
	dir := wmMkdir(t, m, RootInode, "wm_quota")
	p := "/wm_quota"
	if err := m.HandleQuota(ctx, QuotaSet, p, DirQuotaType, map[string]*Quota{p: {MaxSpace: 1 << 20, MaxInodes: 10}}, false, false, false); err != nil {
		t.Fatalf("set quota: %s", err)
	}
	t.Cleanup(func() {
		if err := m.HandleQuota(ctx, QuotaDel, p, DirQuotaType, nil, false, false, false); err != nil {
			t.Errorf("delete quota: %s", err)
		}
		m.getBase().loadQuotas()
	})
	m.getBase().loadQuotas()
	inode := wmCreate(t, m, dir, "f")
	fits := []SliceWrite{{0, 0, wmSlice(t, m, 64<<10)}, {0, 64 << 10, wmSlice(t, m, 64<<10)}}
	if st := m.WriteMulti(ctx, inode, fits, time.Now()); st != 0 {
		t.Fatalf("WriteMulti within the quota: %s", st)
	}
	// Only the second slice goes past the quota (the file becomes 64 MiB); the first must not land either.
	over := []SliceWrite{{0, 128 << 10, wmSlice(t, m, 64<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}
	if st := m.WriteMulti(ctx, inode, over, time.Now()); st != syscall.EDQUOT {
		t.Fatalf("WriteMulti over the quota: %s, want EDQUOT", st)
	}
	wmCheckChunk(t, m, inode, 0, fits...)
	wmCheckChunk(t, m, inode, 1)
	if l := wmLength(t, m, inode); l != 128<<10 {
		t.Fatalf("length %d after a batch over the quota", l)
	}
}

func testWriteMultiCapacity(t *testing.T, m Meta) {
	ctx := Background()
	base := m.getBase()
	used := atomic.LoadInt64(&base.usedSpace) + atomic.LoadInt64(&base.newSpace)
	capacity := uint64(16 << 20)
	if used > 0 {
		capacity += uint64(used)
	}
	format := testFormat()
	format.Capacity = capacity
	if err := wmReinit(t, m, format); err != nil {
		t.Fatalf("set capacity: %s", err)
	}
	t.Cleanup(func() {
		if err := wmReinit(t, m, testFormat()); err != nil {
			t.Errorf("reset capacity: %s", err)
		}
	})
	inode := wmCreate(t, m, RootInode, "wm_capacity")
	fits := []SliceWrite{{0, 0, wmSlice(t, m, 64<<10)}, {0, 64 << 10, wmSlice(t, m, 64<<10)}}
	if st := m.WriteMulti(ctx, inode, fits, time.Now()); st != 0 {
		t.Fatalf("WriteMulti within the capacity: %s", st)
	}
	// The file would become 64 GiB, far past the capacity even if the usage of the tests before this one
	// has drifted below zero.
	over := []SliceWrite{{0, 128 << 10, wmSlice(t, m, 64<<10)}, {1024, 0, wmSlice(t, m, 4<<10)}}
	if st := m.WriteMulti(ctx, inode, over, time.Now()); st != syscall.ENOSPC {
		t.Fatalf("WriteMulti over the capacity of %d bytes (%d used): %s, want ENOSPC", format.Capacity, used, st)
	}
	wmCheckChunk(t, m, inode, 0, fits...)
	wmCheckChunk(t, m, inode, 1024)
	if l := wmLength(t, m, inode); l != 128<<10 {
		t.Fatalf("length %d after a batch over the capacity", l)
	}
}

func testWriteMultiCompaction(t *testing.T, m Meta, rec *compactionRecorder) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, "wm_compaction")
	fill := func(indx uint32, n int) []uint64 {
		var ids []uint64
		for i := 0; i < n; i++ {
			s := wmSlice(t, m, 4<<10)
			if st := m.Write(ctx, inode, indx, uint32(i)<<12, s, time.Now()); st != 0 {
				t.Fatalf("write: %s", st)
			}
			ids = append(ids, s.Id)
		}
		return ids
	}
	// Write compacts a chunk when its count reaches interval-1 (JFS_WRITE_COMPACTION_INTERVAL): not yet.
	b := m.getBase().writeCompactionInterval - 1
	ids0 := fill(0, b-1)
	ids1 := fill(1, b-9)
	if rec.compacted(ids0[0]) || rec.compacted(ids1[0]) {
		t.Fatalf("compacted before the batch")
	}
	var batch []SliceWrite
	for i := 0; i < 3; i++ {
		batch = append(batch, SliceWrite{0, uint32(b-1+i) << 12, wmSlice(t, m, 4<<10)}, SliceWrite{1, uint32(b-9+i) << 12, wmSlice(t, m, 4<<10)})
	}
	if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
		t.Fatalf("WriteMulti: %s", st)
	}
	// Chunk 0 went from b-1 to b+2 slices, over b: it is compacted. Chunk 1 went from b-9 to b-6: it is not.
	if !rec.waitCompacted(ids0[0], 10*time.Second) {
		t.Fatalf("chunk 0 was not compacted after its count went from %d to %d", b-1, b+2)
	}
	time.Sleep(200 * time.Millisecond) // a compaction of chunk 1 would have started with the one of chunk 0
	if rec.compacted(ids1[0]) {
		t.Fatalf("chunk 1 was compacted at %d slices", b-6)
	}
}

// testWriteMultiRace runs WriteMulti batches on 3 chunks while c (another client) compacts them, and checks
// that the file always holds what the batches wrote, in order.
func testWriteMultiRace(t *testing.T, m Meta, c compactor, rec *compactionRecorder, seed int64) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, fmt.Sprintf("wm_race_%d", seed))
	const chunks, blocks = 3, 256 // the first MiB of each chunk, in 4 KiB blocks
	model := make([][]wmBlock, chunks)
	for i := range model {
		model[i] = make([]wmBlock, blocks)
	}
	check := func() {
		t.Helper()
		for indx, want := range model {
			var ss []Slice
			if st := m.Read(ctx, inode, uint32(indx), &ss); st != 0 {
				t.Fatalf("read chunk %d: %s", indx, st)
			}
			got := make([]wmBlock, blocks)
			var pos uint32
			for _, s := range ss {
				for o := uint32(0); s.Id != 0 && o < s.Len && int((pos+o)>>12) < blocks; o += 4 << 10 {
					b, ok := rec.resolve(s.Id, s.Off+o)
					if !ok {
						t.Fatalf("seed %d chunk %d: slice %d offset %d is past its compacted data", seed, indx, s.Id, s.Off+o)
					}
					got[(pos+o)>>12] = b
				}
				pos += s.Len
			}
			for b := range want {
				if got[b] != want[b] {
					t.Fatalf("seed %d chunk %d block %d holds %+v, want %+v (slices %+v)", seed, indx, b, got[b], want[b], ss)
				}
			}
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for indx := uint32(0); indx < chunks; indx++ {
				select {
				case <-stop:
					return
				default:
				}
				c.compactChunk(inode, indx, false, false, 0)
			}
		}
	}()
	stopped := false
	stopCompactor := func() {
		if !stopped {
			stopped = true
			close(stop)
			wg.Wait()
		}
	}
	defer stopCompactor()

	before := rec.count()
	restarts := wmCounters(t, m.getBase().txRestart)
	rng := rand.New(rand.NewSource(seed))
	var length uint64
	for iter := 0; iter < 60; iter++ {
		batch := make([]SliceWrite, 2+rng.Intn(5))
		for i := range batch {
			start := rng.Intn(blocks)
			n := 1 + rng.Intn(min(32, blocks-start))
			skip := rng.Intn(4) // blocks of the object before the slice's data
			batch[i] = SliceWrite{uint32(rng.Intn(chunks)), uint32(start) << 12,
				Slice{Id: wmNewID(t, m), Size: uint32(skip+n) << 12, Off: uint32(skip) << 12, Len: uint32(n) << 12}}
		}
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
			t.Fatalf("seed %d: WriteMulti: %s", seed, st)
		}
		for _, w := range batch { // in order: a later slice of a chunk covers an earlier one
			for b := uint32(0); b < w.Slice.Len>>12; b++ {
				model[w.Indx][w.Off>>12+b] = wmBlock{w.Slice.Id, w.Slice.Off + b<<12}
			}
			length = max(length, uint64(w.Indx)*ChunkSize+uint64(w.Off)+uint64(w.Slice.Len))
		}
		if iter%10 == 9 {
			check()
		}
	}
	stopCompactor()
	check()
	for method, n := range wmCounters(t, m.getBase().txRestart) {
		if n > restarts[method] {
			t.Logf("seed %d: %s restarted %.0f times", seed, method, n-restarts[method])
		}
	}
	t.Logf("seed %d: %d compactions", seed, rec.count()-before)
	if l := wmLength(t, m, inode); l != length {
		t.Fatalf("seed %d: length %d, want %d", seed, l, length)
	}
	if rec.count() == before {
		t.Fatalf("seed %d: no compaction ran", seed)
	}
}

// testWriteMultiIdempotent sends a batch again, as the TKV transaction retry does after a commit whose
// outcome was unknown.
func testWriteMultiIdempotent(t *testing.T, m *kvMeta) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, "wm_idempotent")
	var delta dirStat
	var attr Attr
	commit := func(since time.Time, writes ...SliceWrite) (map[uint32]int, syscall.Errno) {
		t.Helper()
		sorted, _, st := sortSliceWrites(writes)
		if st != 0 {
			t.Fatalf("sort: %s", st)
		}
		counts := make(map[uint32]int)
		delta = dirStat{length: -1} // a batch found there leaves it as it is
		st = m.doWriteMulti(ctx, inode, sorted, time.Now(), since, counts, &delta, &attr)
		if st == 0 && delta.length >= 0 { // as baseMeta.WriteMulti does, so the usage stays right for the tests after this one
			m.updateParentStat(ctx, inode, attr.Parent, delta.length, delta.space)
			m.updateUserGroupStat(ctx, attr.Uid, attr.Gid, delta.space, 0)
		}
		return counts, st
	}
	s1 := SliceWrite{0, 0, wmSlice(t, m, 4<<10)}
	s2 := SliceWrite{0, 4 << 10, wmSlice(t, m, 4<<10)}
	s3 := SliceWrite{1, 0, wmSlice(t, m, 4<<10)}
	want := map[uint32]int{0: 2, 1: 1}
	if counts, st := commit(time.Time{}, s1, s2, s3); st != 0 || !reflect.DeepEqual(counts, want) || delta.length < 0 {
		t.Fatalf("first commit: %s, counts %v, delta %+v", st, counts, delta)
	}
	if counts, st := commit(time.Time{}, s1, s2, s3); st != 0 || !reflect.DeepEqual(counts, want) || delta.length != -1 {
		t.Fatalf("the same batch again: %s, counts %v, delta %+v (want it left alone)", st, counts, delta)
	}
	// A batch lands whole and its ids are new, so one with any of its slices there has landed, even if the others are
	// not there (a compaction absorbed them): nothing more is written.
	s4 := SliceWrite{1, 4 << 10, wmSlice(t, m, 4<<10)}
	s5 := SliceWrite{0, 8 << 10, wmSlice(t, m, 4<<10)}
	if _, st := commit(time.Time{}, s1, s4); st != 0 || delta.length != -1 {
		t.Fatalf("a batch with one of its chunks there: %s, delta %+v, want it taken as landed", st, delta)
	}
	if _, st := commit(time.Time{}, s2, s5); st != 0 || delta.length != -1 {
		t.Fatalf("a batch with part of a chunk there: %s, delta %+v, want it taken as landed", st, delta)
	}
	wmCheckChunk(t, m, inode, 0, s1, s2)
	wmCheckChunk(t, m, inode, 1, s3)
	// A resend of a batch that is not there writes it if the inode is unchanged since the first send, and refuses if
	// it changed (the batch may have landed and been compacted away).
	var now Attr
	if st := m.GetAttr(ctx, inode, &now); st != 0 {
		t.Fatalf("getattr: %s", st)
	}
	ctime := time.Unix(now.Ctime, int64(now.Ctimensec))
	if _, st := commit(ctime, s4); st != errWriteMultiUnknown {
		t.Fatalf("a resend of a batch first sent before the inode changed: %s, want %s", st, errWriteMultiUnknown)
	}
	wmCheckChunk(t, m, inode, 1, s3)
	if _, st := commit(ctime.Add(time.Nanosecond), s4, s5); st != 0 || delta.length < 0 {
		t.Fatalf("a resend with the inode unchanged since the first send: %s, delta %+v", st, delta)
	}
	wmCheckChunk(t, m, inode, 0, s1, s2, s5)
	wmCheckChunk(t, m, inode, 1, s3, s4)
}

// testWriteMultiAccounting compares a WriteMulti with the same slices written one by one with Write: the
// same length and mtime, the same growth of the used space the store counts, the same directory usage.
func testWriteMultiAccounting(t *testing.T, m Meta) {
	ctx := Background()
	wmWaitBackground(t, m) // file deletions of the tests before change the used space
	d1 := wmMkdir(t, m, RootInode, "wm_accounting_multi")
	d2 := wmMkdir(t, m, RootInode, "wm_accounting_single")
	f1 := wmCreate(t, m, d1, "f")
	f2 := wmCreate(t, m, d2, "f")
	batch := func() []SliceWrite { // ends that are not 4 KiB aligned, so the space is not the length
		return []SliceWrite{
			{2, 100, Slice{Id: wmNewID(t, m), Size: 5000, Len: 5000}},
			{0, 0, wmSlice(t, m, 1000)},
			{2, 50, Slice{Id: wmNewID(t, m), Size: 70000, Off: 10, Len: 300}},
			{1, 1 << 20, wmSlice(t, m, 1)},
			{0, 4095, wmSlice(t, m, 2)},
		}
	}
	mtime := time.Unix(1700000000, 5)
	used0 := wmUsedSpace(t, m)
	if st := m.WriteMulti(ctx, f1, batch(), mtime); st != 0 {
		t.Fatalf("WriteMulti: %s", st)
	}
	used1 := wmUsedSpace(t, m)
	sorted, _, st := sortSliceWrites(batch())
	if st != 0 {
		t.Fatalf("sort: %s", st)
	}
	for _, w := range sorted {
		if st := m.Write(ctx, f2, w.Indx, w.Off, w.Slice, mtime); st != 0 {
			t.Fatalf("write: %s", st)
		}
	}
	used2 := wmUsedSpace(t, m)
	var a1, a2 Attr
	if st := m.GetAttr(ctx, f1, &a1); st != 0 {
		t.Fatalf("getattr: %s", st)
	}
	if st := m.GetAttr(ctx, f2, &a2); st != 0 {
		t.Fatalf("getattr: %s", st)
	}
	length := uint64(2*ChunkSize + 5100)
	if a1.Length != length || a2.Length != length || a1.Mtime != a2.Mtime || a1.Mtimensec != a2.Mtimensec {
		t.Fatalf("WriteMulti: length %d mtime %d.%09d; Write: length %d mtime %d.%09d; want length %d",
			a1.Length, a1.Mtime, a1.Mtimensec, a2.Length, a2.Mtime, a2.Mtimensec, length)
	}
	if space := align4K(length) - align4K(0); used1-used0 != space || used2-used1 != space { // a new file counts 4 KiB
		t.Fatalf("the used space grew by %d with WriteMulti and by %d with Write, want %d", used1-used0, used2-used1, space)
	}
	for _, d := range []Ino{d1, d2} {
		if err := waitCheckResult(m, dirStat{length: int64(length), space: align4K(length), inodes: 1}, func() (*dirStat, syscall.Errno) {
			return m.GetDirStat(ctx, d)
		}); err != nil {
			t.Fatalf("usage of directory %d: %s", d, err)
		}
	}
}

// testWriteMultiUserQuota checks that WriteMulti counts its space against the quota of the file's owner:
// the second batch takes the owner past the quota only if the first one was counted.
func testWriteMultiUserQuota(t *testing.T, m Meta) {
	ctx := Background()
	format := testFormat()
	format.UserGroupQuota = true
	if err := wmReinit(t, m, format); err != nil {
		t.Fatalf("enable user and group quotas: %s", err)
	}
	t.Cleanup(func() {
		if err := wmReinit(t, m, testFormat()); err != nil {
			t.Errorf("disable user and group quotas: %s", err)
		}
	})
	const uid, gid = 5001, 6001
	key := strconv.Itoa(uid)
	if err := m.HandleQuota(ctx, QuotaSet, key, UserQuotaType, map[string]*Quota{key: {MaxSpace: 1 << 20, MaxInodes: 10}}, false, false, false); err != nil {
		t.Fatalf("set the quota of user %d: %s", uid, err)
	}
	t.Cleanup(func() {
		if err := m.HandleQuota(ctx, QuotaDel, key, UserQuotaType, nil, false, false, false); err != nil {
			t.Errorf("delete the quota of user %d: %s", uid, err)
		}
		m.getBase().loadQuotas()
	})
	m.getBase().loadQuotas()
	inode := wmCreate(t, m, RootInode, "wm_userquota")
	if st := m.SetAttr(ctx, inode, SetAttrUID|SetAttrGID, 0, &Attr{Uid: uid, Gid: gid}); st != 0 {
		t.Fatalf("chown: %s", st)
	}
	fits := []SliceWrite{{0, 0, wmSlice(t, m, 320<<10)}, {0, 320 << 10, wmSlice(t, m, 320<<10)}}
	if st := m.WriteMulti(ctx, inode, fits, time.Now()); st != 0 {
		t.Fatalf("WriteMulti within the quota: %s", st)
	}
	over := []SliceWrite{{0, 640 << 10, wmSlice(t, m, 320<<10)}, {0, 960 << 10, wmSlice(t, m, 320<<10)}}
	if st := m.WriteMulti(ctx, inode, over, time.Now()); st != syscall.EDQUOT {
		t.Fatalf("WriteMulti to 1280 KiB with a quota of 1 MiB for user %d: %s, want EDQUOT", uid, st)
	}
	wmCheckChunk(t, m, inode, 0, fits...)
	if l := wmLength(t, m, inode); l != 640<<10 {
		t.Fatalf("length %d after a batch over the quota", l)
	}
}

// testWriteMultiFileLock checks that WriteMulti takes the open-file lock, as Write does: the blocking
// compaction at maxSlices then holds back the next write of the file.
func testWriteMultiFileLock(t *testing.T, m Meta) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, "wm_filelock")
	var attr Attr
	if st := m.Open(ctx, inode, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open: %s", st)
	}
	defer func() {
		if st := m.Close(ctx, inode); st != 0 {
			t.Errorf("close: %s", st)
		}
	}()
	f := m.getBase().of.find(inode)
	if f == nil {
		t.Fatalf("open file %d is not in the open-file table", inode)
	}
	batch := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}
	done := make(chan syscall.Errno, 1)
	f.Lock()
	go func() { done <- m.WriteMulti(ctx, inode, batch, time.Now()) }()
	select {
	case st := <-done:
		f.Unlock()
		t.Fatalf("WriteMulti returned (%s) while the open-file lock was held", st)
	case <-time.After(200 * time.Millisecond):
	}
	f.Unlock()
	select {
	case st := <-done:
		if st != 0 {
			t.Fatalf("WriteMulti: %s", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("WriteMulti did not return once the open-file lock was released")
	}
	wmCheckChunk(t, m, inode, 0, batch[0])
	wmCheckChunk(t, m, inode, 1, batch[1])
}

// testWriteMultiMaxSlices fills a chunk far past maxSlices while its compactions fail (as they do while the
// object store is down), then lets them work again, slowly: the next WriteMulti must leave the chunk below
// maxSlices before it returns, which takes more than one blocking compaction.
func testWriteMultiMaxSlices(t *testing.T, m Meta, rec *compactionRecorder) {
	ctx := Background()
	inode := wmCreate(t, m, RootInode, "wm_maxslices")
	limits := m.WriteMultiLimits()
	target := maxSlices + maxCompactSlices // more than one compaction (maxCompactSlices-1 fewer) absorbs
	if m.Name() == "mysql" {
		target = mysqlChunkValueMax/sliceBytes - 2 // what the column holds, less the last batch
	}
	rec.set(t, true, 0)
	for n := 0; n < target; {
		k := min(limits.ChunkSlices, target-n)
		batch := make([]SliceWrite, 0, k+1)
		for i := 0; i < k; i++ {
			batch = append(batch, SliceWrite{0, uint32((n+i)%1024) << 12, wmSlice(t, m, 4<<10)})
		}
		batch = append(batch, SliceWrite{1, uint32(n%1024) << 12, wmSlice(t, m, 4<<10)}) // not a single slice
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
			t.Fatalf("WriteMulti at %d slices: %s", n, st)
		}
		n += k
	}
	wmWaitBackground(t, m)
	ss, st := m.getBase().en.doRead(ctx, inode, 0)
	if st != 0 || len(ss) != target {
		t.Fatalf("chunk 0 holds %d slices (%s), want %d", len(ss), st, target)
	}

	// A compaction left to run in the background would not be done when WriteMulti returns.
	rec.set(t, false, 100*time.Millisecond)
	before := rec.count()
	last := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {0, 4 << 10, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}}
	if st := m.WriteMulti(ctx, inode, last, time.Now()); st != 0 {
		t.Fatalf("WriteMulti: %s", st)
	}
	if ss, st = m.getBase().en.doRead(ctx, inode, 0); st != 0 {
		t.Fatalf("read chunk 0: %s", st)
	}
	if len(ss) >= maxSlices {
		t.Fatalf("chunk 0 holds %d slices when WriteMulti returns, want fewer than %d", len(ss), maxSlices)
	}
	rounds := 1
	if target+2-(maxCompactSlices-1) >= maxSlices {
		rounds = 2
	}
	if n := rec.count() - before; n < rounds {
		t.Fatalf("%d compactions ran before WriteMulti returned, want %d", n, rounds)
	}
}

// wmSpoiler makes a transaction of a second client fail on a conflict at its commit, as a change by
// another client would: armed times, it runs spoil just before a transaction that appends to its chunk
// commits.
type wmSpoiler struct {
	armed    atomic.Int32
	spoiled  atomic.Int32
	attempts atomic.Int32 // commits of the watched transaction tried, spoiled or not
	spoil    func() error
}

func (s *wmSpoiler) before() error {
	for {
		n := s.armed.Load()
		if n <= 0 {
			return nil
		}
		if s.armed.CompareAndSwap(n, n-1) {
			s.spoiled.Add(1)
			return s.spoil()
		}
	}
}

// wmSpoilHook spoils, as a hook of a Redis client, the MULTI/EXEC that appends to chunk key.
type wmSpoilHook struct {
	*wmSpoiler
	key string
}

func (h wmSpoilHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h wmSpoilHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h wmSpoilHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, c := range cmds {
			if args := c.Args(); c.Name() == "rpush" && len(args) > 1 && args[1] == h.key {
				h.attempts.Add(1)
				if err := h.before(); err != nil {
					return err
				}
				break
			}
		}
		return next(ctx, cmds)
	}
}

// wmSpoilingKV is a TKV client that spoils the commits of the transactions that set chunk key.
type wmSpoilingKV struct {
	tkvClient
	*wmSpoiler
	key []byte
}

func (c *wmSpoilingKV) txn(ctx context.Context, f func(*kvTxn) error, retry int) error {
	return c.tkvClient.txn(ctx, func(tx *kvTxn) error {
		tr := &wmSetTracker{kvtxn: tx.kvtxn, key: c.key}
		if err := f(&kvTxn{kvtxn: tr, retry: tx.retry}); err != nil {
			return err
		}
		if tr.hit {
			c.attempts.Add(1)
			return c.before()
		}
		return nil
	}, retry)
}

// wmSetTracker notes whether a transaction sets key.
type wmSetTracker struct {
	kvtxn
	key []byte
	hit bool
}

func (t *wmSetTracker) set(key, value []byte) {
	t.hit = t.hit || bytes.Equal(key, t.key)
	t.kvtxn.set(key, value)
}

// testWriteMultiConflict makes the commit of a second client's WriteMulti fail on a conflict, as when
// another client changes the file meanwhile: retried, the batch lands once; with no retry left, none of
// it lands.
func testWriteMultiConflict(t *testing.T, m Meta) {
	ctx := Background()
	dir := wmMkdir(t, m, RootInode, "wm_conflict")
	inode := wmCreate(t, m, dir, "f")
	sp := &wmSpoiler{spoil: func() error {
		a := Attr{Mtime: time.Now().Unix()}
		if st := m.SetAttr(ctx, inode, SetAttrMtime, 0, &a); st != 0 {
			return st
		}
		return nil
	}}
	// FoundationDB's client retries a conflicting commit inside Transact: the meta layer neither counts
	// that restart nor can limit it
	engineRetries := false
	twin := wmOpenTwin(t, m, func(tw Meta) {
		switch tw := tw.(type) {
		case *redisMeta:
			hook := wmSpoilHook{sp, tw.chunkKey(inode, 0)}
			if cc, ok := tw.rdb.(*redis.ClusterClient); ok {
				// Watch runs on the client of the node that owns the key's slot, with that node's hooks
				if err := cc.ForEachShard(context.Background(), func(_ context.Context, n *redis.Client) error {
					n.AddHook(hook)
					return nil
				}); err != nil {
					t.Fatalf("hook the cluster's nodes: %s", err)
				}
				cc.OnNewNode(func(n *redis.Client) { n.AddHook(hook) })
			} else {
				tw.rdb.AddHook(hook)
			}
		case *kvMeta:
			engineRetries = tw.client.name() == "fdb"
			tw.client = &wmSpoilingKV{tkvClient: tw.client, wmSpoiler: sp, key: tw.chunkKey(inode, 0)}
		default:
			t.Fatalf("no way to make a conflict on %T", tw)
		}
	})
	restarts := func() float64 {
		var n float64
		for _, v := range wmCounters(t, twin.getBase().txRestart) {
			n += v
		}
		return n
	}
	restarted := restarts()
	a := SliceWrite{0, 0, wmSlice(t, m, 4<<10)}
	b := SliceWrite{1, 0, wmSlice(t, m, 4<<10)}
	c := SliceWrite{0, 4 << 10, wmSlice(t, m, 4<<10)}
	sp.armed.Store(1)
	if st := twin.WriteMulti(ctx, inode, []SliceWrite{a, b, c}, time.Now()); st != 0 {
		t.Fatalf("WriteMulti with a conflict: %s", st)
	}
	if n := sp.spoiled.Load(); n != 1 {
		t.Fatalf("%d commits were spoiled, want 1", n)
	}
	if n := sp.attempts.Load(); n < 2 {
		t.Fatalf("the commit was not tried again after the conflict (%d attempts)", n)
	}
	if !engineRetries && restarts() == restarted {
		t.Fatalf("the transaction was not restarted after the conflict")
	}
	wmCheckChunk(t, m, inode, 0, a, c)
	wmCheckChunk(t, m, inode, 1, b)
	length := uint64(ChunkSize + 4<<10)
	if l := wmLength(t, m, inode); l != length {
		t.Fatalf("length %d, want %d", l, length)
	}

	if engineRetries {
		t.Log("the engine retries a conflicting commit itself, whatever the retry limit: skip the no-retry case")
		return
	}
	used := wmUsedSpace(t, twin)
	sp.armed.Store(1)
	noRetry := WrapContext(context.WithValue(context.Background(), txMaxRetryKey{}, 1))
	d := SliceWrite{0, 8 << 10, wmSlice(t, m, 4<<10)}
	e := SliceWrite{2, 0, wmSlice(t, m, 4<<10)}
	if st := twin.WriteMulti(noRetry, inode, []SliceWrite{d, e}, time.Now()); st == 0 {
		t.Fatalf("WriteMulti with a conflict and no retry left succeeded")
	}
	if n := sp.spoiled.Load(); n != 2 {
		t.Fatalf("%d commits were spoiled, want 2", n)
	}
	wmCheckChunk(t, m, inode, 0, a, c)
	wmCheckChunk(t, m, inode, 1, b)
	wmCheckChunk(t, m, inode, 2)
	if l := wmLength(t, m, inode); l != length {
		t.Fatalf("length %d after a failed batch, want %d", l, length)
	}
	if u := wmUsedSpace(t, twin); u != used {
		t.Fatalf("used space %d after a failed batch, %d before", u, used)
	}
}

// testWriteMultiRollback makes a WriteMulti fail on its last chunk, whose value is corrupt: SQL has then
// appended to the first chunks of the batch in the transaction, TKV refuses it before it writes. Nothing
// of the batch may stay behind.
func testWriteMultiRollback(t *testing.T, m Meta) {
	ctx := Background()
	dir := wmMkdir(t, m, RootInode, "wm_rollback")
	inode := wmCreate(t, m, dir, "f")
	pre := SliceWrite{2, 0, wmSlice(t, m, 4<<10)}
	if st := m.Write(ctx, inode, pre.Indx, pre.Off, pre.Slice, time.Now()); st != 0 {
		t.Fatalf("write: %s", st)
	}
	var get func() []byte
	var set func([]byte)
	switch mm := m.(type) {
	case *dbMeta:
		get = func() []byte {
			var ck chunk
			ok, err := mm.db.Where("inode = ? AND indx = ?", inode, pre.Indx).Get(&ck)
			if err != nil || !ok {
				t.Fatalf("read chunk %d: %v (found: %v)", pre.Indx, err, ok)
			}
			return ck.Slices
		}
		set = func(v []byte) {
			n, err := mm.db.Where("inode = ? AND indx = ?", inode, pre.Indx).Cols("slices").Update(&chunk{Slices: v})
			if err != nil || n != 1 {
				t.Fatalf("set chunk %d: %v (%d rows)", pre.Indx, err, n)
			}
		}
	case *kvMeta:
		key := mm.chunkKey(inode, pre.Indx)
		get = func() []byte {
			var v []byte
			if err := mm.txn(ctx, func(tx *kvTxn) error { v = tx.get(key); return nil }); err != nil {
				t.Fatalf("read chunk %d: %s", pre.Indx, err)
			}
			return v
		}
		set = func(v []byte) {
			if err := mm.txn(ctx, func(tx *kvTxn) error { tx.set(key, v); return nil }); err != nil {
				t.Fatalf("set chunk %d: %s", pre.Indx, err)
			}
		}
	default:
		t.Fatalf("no way to corrupt a chunk on %T", m)
	}
	orig := get()
	set(append(append([]byte(nil), orig...), 1, 2, 3, 4, 5))
	restored := false
	restore := func() {
		if !restored {
			restored = true
			set(orig)
		}
	}
	t.Cleanup(restore) // before the file is unlinked

	used := wmUsedSpace(t, m)
	batch := []SliceWrite{{0, 0, wmSlice(t, m, 4<<10)}, {1, 0, wmSlice(t, m, 4<<10)}, {2, 4 << 10, wmSlice(t, m, 4<<10)}}
	if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != syscall.EIO {
		t.Fatalf("WriteMulti onto a corrupt chunk: %s, want EIO", st)
	}
	restore()
	wmCheckChunk(t, m, inode, 0)
	wmCheckChunk(t, m, inode, 1)
	wmCheckChunk(t, m, inode, 2, pre)
	if l := wmLength(t, m, inode); l != 2*ChunkSize+4<<10 {
		t.Fatalf("length %d after a failed batch", l)
	}
	if u := wmUsedSpace(t, m); u != used {
		t.Fatalf("used space %d after a failed batch, %d before", u, used)
	}
	if dm, ok := m.(*dbMeta); ok {
		for _, w := range batch {
			if ref, ok := wmSliceRef(t, dm, w.Slice.Id); ok {
				t.Fatalf("slice %d of the failed batch has a chunk_ref row: %+v", w.Slice.Id, ref)
			}
		}
	}
}

// testWriteMultiInterleave runs WriteMulti while a second client writes single slices into the same chunks,
// extends the file and changes its attributes (Redis and TKV transactions then retry, SQL ones wait for the
// node row). Every batch must stay contiguous and in order in each chunk, every slice must be there once,
// and the length must never fall below the end of a write that has returned.
func testWriteMultiInterleave(t *testing.T, m Meta, rec *compactionRecorder) {
	ctx := Background()
	rec.set(t, true, 0) // a compaction would merge the slices this test looks for
	dir := wmMkdir(t, m, RootInode, "wm_interleave")
	inode := wmCreate(t, m, dir, "f")
	twin := wmOpenTwin(t, m, nil)
	twin.OnMsg(CompactChunk, func(args ...interface{}) error { return errWMCompactionFails })
	restarts := wmCounters(t, m.getBase().txRestart)

	var twinEnd atomic.Uint64 // the furthest end of the second client's writes that have returned
	var singles []SliceWrite  // the second client's writes; read once it stopped
	var twinErr error
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var id uint64
			if st := twin.NewSlice(ctx, &id); st != 0 {
				twinErr = fmt.Errorf("new slice: %s", st)
				return
			}
			// Into the chunks of the batches, and every third one past the end of the file.
			w := SliceWrite{uint32(i % 2), uint32(i%64) << 12, Slice{Id: id, Size: 4 << 10, Len: 4 << 10}}
			if i%3 == 2 {
				w.Indx, w.Off = 3, uint32(i%16000)<<12
			}
			if st := twin.Write(ctx, inode, w.Indx, w.Off, w.Slice, time.Now()); st != 0 {
				twinErr = fmt.Errorf("write: %s", st)
				return
			}
			singles = append(singles, w)
			twinEnd.Store(max(twinEnd.Load(), uint64(w.Indx)*ChunkSize+uint64(w.Off)+uint64(w.Slice.Len)))
			a := Attr{Mtime: time.Now().Unix()}
			if st := twin.SetAttr(ctx, inode, SetAttrMtime, 0, &a); st != 0 {
				twinErr = fmt.Errorf("setattr: %s", st)
				return
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()
	stopped := false
	stopTwin := func() {
		if !stopped {
			stopped = true
			close(stop)
			wg.Wait()
		}
	}
	defer stopTwin()

	var batches [][]SliceWrite
	var end uint64
	for iter := 0; iter < 25; iter++ {
		floor := twinEnd.Load()
		batch := []SliceWrite{
			{0, 0, wmSlice(t, m, 4<<10)}, {1, 8 << 10, wmSlice(t, m, 4<<10)}, {0, 4 << 10, wmSlice(t, m, 4<<10)},
			{1, 12 << 10, wmSlice(t, m, 4<<10)}, {0, 8 << 10, wmSlice(t, m, 4<<10)}, {2, uint32(iter) << 12, wmSlice(t, m, 4<<10)},
		}
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
			t.Fatalf("WriteMulti: %s", st)
		}
		batches = append(batches, batch)
		end = max(end, sliceWritesEnd(batch))
		if l := wmLength(t, m, inode); l < max(floor, end) {
			t.Fatalf("length %d after WriteMulti, below the end %d of a write that had returned", l, max(floor, end))
		}
		time.Sleep(time.Millisecond)
	}
	stopTwin()
	if twinErr != nil {
		t.Fatalf("second client: %s", twinErr)
	}

	pos := make(map[uint64][2]int) // slice id -> chunk, position in it
	for indx := uint32(0); indx < 4; indx++ {
		ss, st := m.getBase().en.doRead(ctx, inode, indx)
		if st != 0 {
			t.Fatalf("read chunk %d: %s", indx, st)
		}
		for i, s := range ss {
			if _, dup := pos[s.id]; dup {
				t.Fatalf("slice %d is in the file twice", s.id)
			}
			pos[s.id] = [2]int{int(indx), i}
		}
	}
	for _, w := range singles {
		if p, ok := pos[w.Slice.Id]; !ok || p[0] != int(w.Indx) {
			t.Fatalf("single slice %d is not in chunk %d (%v, %v)", w.Slice.Id, w.Indx, p, ok)
		}
	}
	for bi, b := range batches {
		last := make(map[uint32]int)
		for _, w := range b {
			p, ok := pos[w.Slice.Id]
			if !ok || p[0] != int(w.Indx) {
				t.Fatalf("slice %d of batch %d is not in chunk %d (%v, %v)", w.Slice.Id, bi, w.Indx, p, ok)
			}
			if l, seen := last[w.Indx]; seen && p[1] != l+1 {
				t.Fatalf("batch %d is not contiguous in chunk %d: position %d after %d", bi, w.Indx, p[1], l)
			}
			last[w.Indx] = p[1]
		}
	}
	if n := len(singles) + len(batches)*len(batches[0]); len(pos) != n {
		t.Fatalf("the file holds %d slices, want %d", len(pos), n)
	}
	if l := wmLength(t, m, inode); l != max(end, twinEnd.Load()) {
		t.Fatalf("length %d, want %d", l, max(end, twinEnd.Load()))
	}
	for method, n := range wmCounters(t, m.getBase().txRestart) {
		if n > restarts[method] {
			t.Logf("%s restarted %.0f times; %d single writes in between", method, n-restarts[method], len(singles))
		}
	}
}

func TestWriteMultiSQLite(t *testing.T) {
	m, err := newSQLMeta("sqlite3", path.Join(t.TempDir(), "jfs-writemulti.db"), testConfig())
	if err != nil || m.Name() != "sqlite3" {
		t.Fatalf("create meta: %s", err)
	}
	wmFlaky(m)
	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Errorf("shutdown: %s", err)
		}
	})
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	testWriteMulti(t, m)
}

func TestWriteMultiMemKV(t *testing.T) {
	_ = os.Remove(settingPath)
	m, err := newKVMeta("memkv", "jfs-writemulti-test", testConfig())
	if err != nil || m.Name() != "memkv" {
		t.Fatalf("create meta: %s", err)
	}
	wmFlaky(m)
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	testWriteMulti(t, m)
}
