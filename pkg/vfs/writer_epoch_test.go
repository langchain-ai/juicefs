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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// putAction is what ruleStore does with one Put.
type putAction struct {
	hold  bool          // wait until the rule changes, then ask again
	fail  bool          // return an error without storing anything
	delay time.Duration // store it after this long
}

// ruleStore passes each Put through a rule the test can change at any time. A held Put asks the rule again whenever it
// changes, so setting the rule to nil releases everything. Every held Put also returns when its context ends (the
// cached store puts with a timeout), so none outlives a test by more than that.
type ruleStore struct {
	object.ObjectStorage
	mu      sync.Mutex
	rule    func(key string) putAction // called with mu held; nil passes everything
	changed chan struct{}              // closed and replaced by setRule
	held    int
	stored  map[uint64][]string // keys stored per slice id
}

var errInjectedPut = errors.New("injected upload failure")

func newRuleStore(inner object.ObjectStorage) *ruleStore {
	return &ruleStore{ObjectStorage: inner, changed: make(chan struct{}), stored: make(map[uint64][]string)}
}

func (r *ruleStore) setRule(rule func(key string) putAction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rule = rule
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *ruleStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	for {
		r.mu.Lock()
		var act putAction
		if r.rule != nil {
			act = r.rule(key)
		}
		changed := r.changed
		if !act.hold {
			r.mu.Unlock()
			if act.fail {
				return errInjectedPut
			}
			if act.delay > 0 {
				t := time.NewTimer(act.delay)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return ctx.Err()
				}
			}
			if err := r.ObjectStorage.Put(ctx, key, in, getters...); err != nil {
				return err
			}
			if id, ok := sliceIDOfKey(key); ok {
				r.mu.Lock()
				r.stored[id] = append(r.stored[id], key)
				r.mu.Unlock()
			}
			return nil
		}
		r.held++
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		r.mu.Lock()
		r.held--
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// waitHeld waits until at least n Puts are held.
func (r *ruleStore) waitHeld(t *testing.T, n int) {
	t.Helper()
	waitUntil(t, 10*time.Second, "uploads held at the store", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.held >= n
	})
}

// storedIDs returns the ids of the slices with at least one object put so far (removed ones included).
func (r *ruleStore) storedIDs() map[uint64]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make(map[uint64]bool, len(r.stored))
	for id := range r.stored {
		ids[id] = true
	}
	return ids
}

func (r *ruleStore) keysOf(id uint64) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stored[id]...)
}

// objectsExist reports how many of the keys the store still holds.
func (r *ruleStore) objectsExist(keys []string) int {
	n := 0
	for _, k := range keys {
		if _, err := r.ObjectStorage.Head(context.Background(), k); err == nil {
			n++
		}
	}
	return n
}

// sliceIDOfKey parses the slice id out of an object key, chunks/<a>/<b>/<id>_<indx>_<size>.
func sliceIDOfKey(key string) (uint64, bool) {
	if !strings.HasPrefix(key, "chunks/") {
		return 0, false
	}
	parts := strings.Split(path.Base(key), "_")
	if len(parts) != 3 {
		return 0, false
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	return id, err == nil
}

func failSlice(id uint64) func(string) putAction {
	return func(key string) putAction {
		got, _ := sliceIDOfKey(key)
		return putAction{fail: got == id}
	}
}

func holdSlice(id uint64) func(string) putAction {
	return func(key string) putAction {
		got, _ := sliceIDOfKey(key)
		return putAction{hold: got == id}
	}
}

func holdAll(string) putAction { return putAction{hold: true} }

// epochCommit is one commit of the writer as the metadata engine received it.
type epochCommit struct {
	inode   Ino
	epoch   uint64 // id of the file's oldest closed epoch when the commit was sent (0 in chunk mode)
	writes  uint64 // the writes of that epoch
	reason  string // why it closed
	mode    string // multi (WriteMulti) or sequential (Write)
	slices  []meta.SliceWrite
	problem string // not "" if the slices sent are not that epoch's, in creation order
	at      time.Time
}

// recordingMeta sits between the writer and the metadata engine: it records the writer's commits, with the epoch each
// one belongs to, and can pause them, so a test can look at the metadata as a crash at that moment would leave it. It
// can also make WriteMulti fail, or look absent.
type recordingMeta struct {
	meta.Meta
	w       *dataWriter
	pause   sync.RWMutex // commits hold it shared; a snapshot holds it exclusively
	mu      sync.Mutex
	commits []epochCommit
	calls   int // WriteMulti calls of the writer
	paused  int // commits waiting for the pause lock

	// set before the file is written
	noMulti bool                   // WriteMulti returns ENOTSUP, and WriteMultiLimits the zero value
	limits  *meta.WriteMultiLimits // WriteMultiLimits returns these
	inject  func(call int) (st syscall.Errno, afterCommit bool)
}

// describe finds the epoch a commit belongs to: the oldest closed one of the file, which commitEpochs commits.
func (r *recordingMeta) describe(inode Ino, writes []meta.SliceWrite, mode string) epochCommit {
	rec := epochCommit{inode: inode, mode: mode, slices: append([]meta.SliceWrite(nil), writes...)}
	f := r.w.find(inode)
	if f == nil || !r.w.epochs {
		return rec
	}
	f.Lock()
	defer f.Unlock()
	if len(f.queue) == 0 {
		rec.problem = "no closed epoch"
		return rec
	}
	e := f.queue[0]
	rec.epoch, rec.writes, rec.reason = e.id, e.writes, e.reason
	var ids []uint64
	for _, s := range e.slices {
		if s.slen > 0 {
			ids = append(ids, s.id)
		}
	}
	switch mode {
	case "multi":
		if len(ids) != len(writes) {
			rec.problem = fmt.Sprintf("%d slices sent, epoch %d has %d", len(writes), e.id, len(ids))
			return rec
		}
		for i, w := range writes {
			if w.Slice.Id != ids[i] {
				rec.problem = fmt.Sprintf("slice %d sent is %d, epoch %d has %d there", i, w.Slice.Id, e.id, ids[i])
				return rec
			}
		}
	case "sequential":
		found := false
		for _, id := range ids {
			found = found || id == writes[0].Slice.Id
		}
		if !found {
			rec.problem = fmt.Sprintf("slice %d is not in epoch %d", writes[0].Slice.Id, e.id)
		}
	}
	return rec
}

func (r *recordingMeta) commit(rec epochCommit, do func() syscall.Errno) syscall.Errno {
	r.mu.Lock()
	r.paused++
	r.mu.Unlock()
	r.pause.RLock()
	defer r.pause.RUnlock()
	r.mu.Lock()
	r.paused--
	r.mu.Unlock()
	st := do()
	if st == 0 {
		rec.at = time.Now()
		r.mu.Lock()
		r.commits = append(r.commits, rec)
		r.mu.Unlock()
	}
	return st
}

func (r *recordingMeta) WriteMulti(ctx meta.Context, inode Ino, writes []meta.SliceWrite, mtime time.Time) syscall.Errno {
	rec := r.describe(inode, writes, "multi")
	if r.noMulti {
		return syscall.ENOTSUP
	}
	r.mu.Lock()
	r.calls++
	call, inject := r.calls, r.inject
	r.mu.Unlock()
	var injected syscall.Errno
	var after bool
	if inject != nil {
		injected, after = inject(call)
	}
	if injected != 0 && !after {
		return injected
	}
	st := r.commit(rec, func() syscall.Errno { return r.Meta.WriteMulti(ctx, inode, writes, mtime) })
	if st == 0 && injected != 0 {
		return injected // the batch landed, and the writer cannot know
	}
	return st
}

func (r *recordingMeta) WriteMultiLimits() meta.WriteMultiLimits {
	if r.noMulti {
		return meta.WriteMultiLimits{}
	}
	if r.limits != nil {
		return *r.limits
	}
	return r.Meta.WriteMultiLimits()
}

func (r *recordingMeta) Write(ctx meta.Context, inode Ino, indx uint32, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	writes := []meta.SliceWrite{{Indx: indx, Off: off, Slice: slice}}
	rec := r.describe(inode, writes, "sequential")
	return r.commit(rec, func() syscall.Errno { return r.Meta.Write(ctx, inode, indx, off, slice, mtime) })
}

func (r *recordingMeta) commitsOf(inode Ino) []epochCommit {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []epochCommit
	for _, c := range r.commits {
		if c.inode == inode {
			out = append(out, c)
		}
	}
	return out
}

func (r *recordingMeta) multiCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingMeta) waitPaused(t *testing.T) {
	t.Helper()
	waitUntil(t, 10*time.Second, "a commit to wait at the pause", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.paused > 0
	})
}

// epochOrderError reports commits of an inode that are not the epochs 1..n in order, each one WriteMulti holding exactly
// the epoch's slices in creation order, with no slice in two of them.
func epochOrderError(commits []epochCommit) error {
	ids := map[uint64]int{}
	for i, c := range commits {
		if c.problem != "" {
			return fmt.Errorf("commit %d (epoch %d): %s", i, c.epoch, c.problem)
		}
		if c.mode != "multi" || c.epoch != uint64(i+1) {
			return fmt.Errorf("commit %d is epoch %d (%s), want epoch %d in one WriteMulti", i, c.epoch, c.mode, i+1)
		}
		if i > 0 && c.writes < commits[i-1].writes {
			return fmt.Errorf("commit %d: epoch %d closed after %d writes, the one before after %d", i, c.epoch, c.writes, commits[i-1].writes)
		}
		for _, s := range c.slices {
			if j, dup := ids[s.Slice.Id]; dup {
				return fmt.Errorf("slice %d is in commits %d and %d", s.Slice.Id, j, i)
			}
			ids[s.Slice.Id] = i
		}
	}
	return nil
}

func checkEpochOrder(t *testing.T, commits []epochCommit) {
	t.Helper()
	if err := epochOrderError(commits); err != nil {
		t.Fatal(err)
	}
}

// committedLength is the file length the commits make: the end of the furthest slice (for a file that starts empty and
// is not truncated). It is not read with GetAttr, which races with the VFS's reads in the metadata client's open file
// cache (baseMeta.Read reads the cached attr without its lock).
func committedLength(commits []epochCommit) uint64 {
	var length uint64
	for _, c := range commits {
		for _, s := range c.slices {
			length = max(length, uint64(s.Indx)*meta.ChunkSize+uint64(s.Off)+uint64(s.Slice.Len))
		}
	}
	return length
}

// testMetaURLs returns the metadata engines of the property tests: memkv, plus JFS_TEST_META_URL if set, for example
// redis://127.0.0.1:6379/1 (its database is flushed) or sqlite3:///tmp/epoch-test.db.
func testMetaURLs() []string {
	urls := []string{"memkv://"}
	if u := os.Getenv("JFS_TEST_META_URL"); u != "" {
		urls = append(urls, u)
	}
	return urls
}

// createEpochTestVFS is createTestVFS with a ruleStore in front of the object store, a recordingMeta in front of the
// writer's metadata engine, the metadata engine at metaURL (reset first, unless memkv) and the chunk config adjustable.
// The writer commits in epochs, whatever JFS_COMMIT_MODE the tests run with.
func createEpochTestVFS(t *testing.T, metaURL string, adjust func(*chunk.Config)) (*VFS, *ruleStore, *recordingMeta) {
	t.Helper()
	return createWriterTestVFS(t, "epoch", metaURL, adjust)
}

// createWriterTestVFS is createEpochTestVFS with the writer in the commit mode given (JFS_COMMIT_MODE).
func createWriterTestVFS(t *testing.T, mode, metaURL string, adjust func(*chunk.Config)) (*VFS, *ruleStore, *recordingMeta) {
	t.Helper()
	t.Setenv("JFS_COMMIT_MODE", mode) // read by NewDataWriter
	mp := "/jfs"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	if metaURL == "" {
		metaURL = "memkv://"
	}
	m := meta.NewClient(metaURL, metaConf)
	if metaURL != "memkv://" {
		if err := m.Reset(); err != nil {
			t.Fatalf("reset %s: %s", metaURL, err)
		}
	}
	format := &meta.Format{
		Name:        "test",
		UUID:        uuid.New().String(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "lz4",
		DirStats:    true,
	}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("setting: %s", err)
	}
	conf := &Config{
		Meta:    metaConf,
		Format:  *format,
		Version: "Juicefs",
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize * 1024,
			Compress:    format.Compression,
			MaxUpload:   2,
			MaxDownload: 200,
			BufferSize:  30 << 20,
			CacheSize:   10 << 20,
			CacheDir:    "memory",
		},
		FuseOpts: &FuseOptions{},
	}
	if adjust != nil {
		adjust(conf.Chunk)
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("create storage: %s", err)
	}
	store := newRuleStore(blob)
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": format.Name}, registry))
	cs := chunk.NewCachedStore(store, *conf.Chunk, registry)
	v := NewVFS(conf, m, cs, registerer, registry)
	dw := v.writer.(*dataWriter)
	rec := &recordingMeta{Meta: m, w: dw}
	dw.m = rec // before any write: commitEpochs and prepareID start with the first one
	t.Cleanup(func() {
		store.setRule(nil)
		if n := dw.invariantBreaks.Load(); n != 0 {
			t.Errorf("%d defensive branches of the epoch writer ran (see the error log)", n)
		}
	})
	return v, store, rec
}

// readFlushRange sets JFS_READ_FLUSH_RANGE (read when the VFS is created) for the rest of the test. The tests of the epoch
// writer that use reads to close epochs while writes go on need it on: with it off, a read flushes the whole file and
// holds back its writes meanwhile, as fsync does. The others run with whatever the environment says, and the package
// runs under both values (the property tests run under both themselves).
func readFlushRange(t *testing.T, on bool) {
	t.Setenv("JFS_READ_FLUSH_RANGE", strconv.FormatBool(on))
}

// forEachReadFlush runs test with JFS_READ_FLUSH_RANGE off and on.
func forEachReadFlush(t *testing.T, test func(t *testing.T)) {
	for _, on := range []bool{false, true} {
		t.Run("readflushrange="+strconv.FormatBool(on), func(t *testing.T) {
			readFlushRange(t, on)
			test(t)
		})
	}
}

func waitUntil(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// sliceState is what the writer holds about one pending slice.
type sliceState struct {
	epoch, id       uint64
	indx, off, slen uint32
	frozen, done    bool
	chunkPos        int
}

// writerState is what the writer holds about a file's pending writes.
type writerState struct {
	open       uint64       // id of the open epoch
	openSlices int          // its slices
	queued     []uint64     // ids of the closed epochs not yet handled, oldest first
	reasons    []string     // why each of them closed
	slices     []sliceState // every pending slice: by epoch, in creation order
	err        syscall.Errno
}

// epochState returns the file's pending epochs and checks the writer's invariants: the closed epochs are queued in id
// order, older than the open one; only the open epoch's slices may take writes (unfrozen); every pending slice is in
// exactly one epoch and in its chunk, each chunk's slices are in epoch order; the counters are right, and a committer
// runs while anything is pending.
func epochState(t *testing.T, v *VFS, ino Ino) writerState {
	t.Helper()
	f := v.writer.(*dataWriter).find(ino)
	if f == nil {
		return writerState{}
	}
	f.Lock()
	defer f.Unlock()
	st := writerState{open: f.open.id, openSlices: len(f.open.slices), err: f.err}
	epochs := append(append([]*epoch(nil), f.queue...), f.open)
	inEpoch := map[*sliceWriter]*epoch{}
	var total, done int
	for i, e := range epochs {
		if e.committed {
			t.Errorf("epoch %d is pending and committed", e.id)
		}
		if i > 0 && e.id <= epochs[i-1].id {
			t.Errorf("epoch %d is queued after epoch %d", e.id, epochs[i-1].id)
		}
		if e != f.open {
			st.queued = append(st.queued, e.id)
			st.reasons = append(st.reasons, e.reason)
		}
		notDone := 0
		for _, s := range e.slices {
			if s.ep != e {
				t.Errorf("a slice of epoch %d says it is in epoch %v", e.id, s.ep)
			}
			if prev, dup := inEpoch[s]; dup {
				t.Errorf("a slice is in epochs %d and %d", prev.id, e.id)
			}
			inEpoch[s] = e
			if !s.freezed && e != f.open {
				t.Errorf("a slice of closed epoch %d is unfrozen", e.id)
			}
			if !s.done {
				notDone++
			} else {
				done++
			}
			pos := -1
			for j, cs := range s.chunk.slices {
				if cs == s {
					pos = j
				}
			}
			if pos < 0 || f.chunks[s.chunk.indx] != s.chunk {
				t.Errorf("a slice of epoch %d is not in its chunk %d", e.id, s.chunk.indx)
			}
			st.slices = append(st.slices, sliceState{epoch: e.id, id: s.id, indx: s.chunk.indx, off: s.off, slen: s.slen,
				frozen: s.freezed, done: s.done, chunkPos: pos})
			total++
		}
		if notDone != e.notDone {
			t.Errorf("epoch %d has %d slices not done, it counts %d", e.id, notDone, e.notDone)
		}
	}
	inChunks := 0
	for _, c := range f.chunks {
		inChunks += len(c.slices)
		for j, s := range c.slices {
			if inEpoch[s] == nil {
				t.Errorf("a slice of chunk %d is in no pending epoch", c.indx)
			} else if j > 0 && inEpoch[c.slices[j-1]] != nil && s.ep.id < c.slices[j-1].ep.id {
				t.Errorf("chunk %d has a slice of epoch %d after one of epoch %d", c.indx, s.ep.id, c.slices[j-1].ep.id)
			}
		}
	}
	if inChunks != total || total != f.pendingCount {
		t.Errorf("the chunks hold %d slices, the epochs %d, the writer counts %d", inChunks, total, f.pendingCount)
	}
	if done != f.donePending {
		t.Errorf("%d pending slices are done, the writer counts %d", done, f.donePending)
	}
	if (f.pendingCount > 0) != f.committing {
		t.Errorf("%d slices pending, committer running: %v", f.pendingCount, f.committing)
	}
	if n := f.w.invariantBreaks.Load(); n != 0 {
		t.Errorf("%d defensive branches of the epoch writer ran (see the error log)", n)
	}
	return st
}

// pendingSliceAt returns the pending slice that starts at off in chunk indx.
func pendingSliceAt(t *testing.T, v *VFS, ino Ino, indx, off uint32) (sliceState, bool) {
	t.Helper()
	for _, s := range epochState(t, v, ino).slices {
		if s.indx == indx && s.off == off {
			return s, true
		}
	}
	return sliceState{}, false
}

// waitSliceIDAt waits until the pending slice at off in chunk indx has its id.
func waitSliceIDAt(t *testing.T, v *VFS, ino Ino, indx, off uint32) uint64 {
	t.Helper()
	var id uint64
	waitUntil(t, 10*time.Second, "a slice id", func() bool {
		s, ok := pendingSliceAt(t, v, ino, indx, off)
		id = s.id
		return ok && id != 0
	})
	return id
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %s", err)
	}
	return m.GetCounter().GetValue()
}

func histogramSum(t *testing.T, h prometheus.Histogram) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("read histogram: %s", err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func capWaits(t *testing.T, reason string) (uint64, float64) {
	t.Helper()
	h, ok := writerSliceCapWait.WithLabelValues(reason).(prometheus.Histogram)
	if !ok {
		t.Fatalf("writer_slice_cap_wait_seconds{reason=%q} is not a histogram", reason)
	}
	return histogramSum(t, h)
}

func mustFsync(t *testing.T, v *VFS, ino Ino, fh uint64) {
	t.Helper()
	if e := v.Fsync(NewLogContext(meta.Background()), ino, 0, fh); e != 0 {
		t.Fatalf("fsync: %s", e)
	}
}

func mustOpen(t *testing.T, v *VFS, ino Ino, flags uint32) uint64 {
	t.Helper()
	_, fh, e := v.Open(NewLogContext(meta.Background()), ino, flags)
	if e != 0 {
		t.Fatalf("open: %s", e)
	}
	return fh
}

// receiveRead waits for a read that startRead started (reader_invalidate_test.go).
func receiveRead(t *testing.T, ch chan readResult, limit time.Duration) readResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(limit):
		t.Fatalf("a read did not return within %s", limit)
	}
	return readResult{}
}

// writerGoroutines counts goroutines running the writer's per-slice, per-chunk and per-file work.
func writerGoroutines() int {
	buf := make([]byte, 8<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		for _, fn := range []string{"vfs.(*chunkWriter).commitThread", "vfs.(*fileWriter).commitEpochs", "vfs.(*sliceWriter).flushData",
			"vfs.(*sliceWriter).prepareID", "vfs.(*dataWriter).removeSlices"} {
			if bytes.Contains(g, []byte(fn)) {
				n++
				break
			}
		}
	}
	return n
}

// waitWriterIdle checks that no writer goroutine is left once every file is flushed. Pending slices of earlier tests
// in the package are committed by their background flusher within seconds, so this waits for those too; a test that
// counts the writer's global metrics calls it first, so no other file's epoch moves them.
func waitWriterIdle(t *testing.T) {
	t.Helper()
	waitUntil(t, 30*time.Second, "the writer goroutines to exit", func() bool { return writerGoroutines() == 0 })
}

// noMemoryCache turns the store's memory cache off. The write throttle counts every page allocated in the process
// (dataWriter.usedBufferSize), and a memory cache is never freed while its VFS lives, which is the rest of the test
// binary: tests that write a lot would leave later writes, in any test, stalled at the throttle.
func noMemoryCache(c *chunk.Config) { c.CacheSize = 0 }

// waitPagesReleased checks that the pages allocated since before (utils.AllocMemory) are freed again: the buffers of
// uploaded, dropped and aborted slices go back. The margin is the chunk package's pool of free 64 KiB pages, which
// still count as allocated (128 of them), plus what other tests' writers may still be finishing, so this catches
// lost write buffers in bulk only.
func waitPagesReleased(t *testing.T, before int64) {
	t.Helper()
	waitUntil(t, 20*time.Second, "the write buffers to be released", func() bool {
		return utils.AllocMemory() <= before+9<<20
	})
}

// fileErr returns the writer's sticky error of the file.
func fileErr(v *VFS, ino Ino) syscall.Errno {
	f := v.writer.(*dataWriter).find(ino)
	if f == nil {
		return 0
	}
	f.Lock()
	defer f.Unlock()
	return f.err
}

// extendPastChunks commits a write at the start of chunk n, so the file is longer than the chunks before it.
func extendPastChunks(t *testing.T, v *VFS, ino Ino, fh uint64, n uint64) {
	t.Helper()
	mustWrite(t, v, ino, fh, n*meta.ChunkSize, []byte("end"))
	mustFsync(t, v, ino, fh)
}

func TestCommitConfigFromEnv(t *testing.T) {
	chunkMode := commitConfig{false, defaultEpochMaxAge, defaultEpochMaxSlices}
	for _, tc := range []struct {
		mode, age, slices string
		unset             bool // the three variables are unset, not empty
		want              commitConfig
		bad               string // the variable the error names, when the config is invalid
	}{
		{unset: true, want: chunkMode},
		{want: chunkMode},
		{mode: "chunk", want: chunkMode},
		{mode: "chunk", age: "2500", slices: "64", want: commitConfig{false, 2500 * time.Millisecond, 64}},
		{mode: "epoch", want: commitConfig{true, defaultEpochMaxAge, defaultEpochMaxSlices}},
		{mode: "epoch", age: "2500", slices: "64", want: commitConfig{true, 2500 * time.Millisecond, 64}},
		{mode: "epoch", age: "1", slices: "2", want: commitConfig{true, time.Millisecond, 2}},
		{mode: "epoch", age: "3600000", slices: "1048576", want: commitConfig{true, time.Hour, 1 << 20}},
		// Invalid values refuse to start (NewDataWriter) rather than run another arm, or other parameters, of an A/B.
		{mode: "chunks", bad: "JFS_COMMIT_MODE"},
		{mode: "Epoch", bad: "JFS_COMMIT_MODE"},
		{mode: " epoch", bad: "JFS_COMMIT_MODE"},
		{mode: "true", bad: "JFS_COMMIT_MODE"},
		{mode: "epoch", age: "0", bad: "JFS_EPOCH_MAX_AGE_MS"},
		{mode: "epoch", age: "-1", bad: "JFS_EPOCH_MAX_AGE_MS"},
		{mode: "epoch", age: "5s", bad: "JFS_EPOCH_MAX_AGE_MS"},
		{mode: "epoch", age: " 5000", bad: "JFS_EPOCH_MAX_AGE_MS"},
		{mode: "epoch", age: "3600001", bad: "JFS_EPOCH_MAX_AGE_MS"},
		{mode: "chunk", slices: "0", bad: "JFS_EPOCH_MAX_SLICES"}, // checked in chunk mode too
		{mode: "epoch", slices: "1", bad: "JFS_EPOCH_MAX_SLICES"}, // a write across a chunk boundary would not fit
		{mode: "epoch", slices: "2000000", bad: "JFS_EPOCH_MAX_SLICES"},
		{mode: "epoch", slices: "99999999999999999999", bad: "JFS_EPOCH_MAX_SLICES"},
	} {
		for name, value := range map[string]string{"JFS_COMMIT_MODE": tc.mode, "JFS_EPOCH_MAX_AGE_MS": tc.age, "JFS_EPOCH_MAX_SLICES": tc.slices} {
			t.Setenv(name, value)
			if tc.unset {
				if err := os.Unsetenv(name); err != nil {
					t.Fatal(err)
				}
			}
		}
		got, err := commitConfigFromEnv()
		switch {
		case tc.bad != "" && (err == nil || !strings.Contains(err.Error(), tc.bad)):
			t.Errorf("mode %q age %q slices %q: error %v, want one about %s", tc.mode, tc.age, tc.slices, err, tc.bad)
		case tc.bad == "" && (err != nil || got != tc.want):
			t.Errorf("mode %q age %q slices %q: %+v, %v, want %+v", tc.mode, tc.age, tc.slices, got, err, tc.want)
		}
	}
}

// A writer in epoch mode registers its metrics, each series present (at zero) before its first event, under the names
// an external metrics sampler reads; one in chunk mode registers none of them.
func TestWriterMetricsAreRegisteredInEpochModeOnly(t *testing.T) {
	want := map[string]int{
		"juicefs_writer_epochs_total": len(epochOutcomes), "juicefs_writer_epoch_closed_total": len(epochCloseReasons),
		"juicefs_writer_epoch_commits_total": len(epochCommitModes), "juicefs_writer_epoch_slices": 1,
		"juicefs_writer_epoch_chunks": 1, "juicefs_writer_epoch_bytes": 1, "juicefs_writer_epoch_age_seconds": 1,
		"juicefs_writer_epoch_upload_wait_seconds": 1, "juicefs_writer_epoch_order_wait_seconds": 1,
		"juicefs_writer_epoch_commit_seconds": 1, "juicefs_writer_epoch_durable_lag_seconds": 1,
		"juicefs_writer_slices_frozen_by_epoch_total": len(epochCloseReasons), "juicefs_writer_read_epoch_wait_seconds": 1,
		"juicefs_writer_slices_created_total": 1, "juicefs_writer_slices_abandoned_total": 1,
		"juicefs_writer_slice_cap_wait_seconds": len(sliceCapReasons),
	}
	for _, mode := range []string{"chunk", "epoch"} {
		t.Run(mode, func(t *testing.T) {
			v, _, _ := createWriterTestVFS(t, mode, "", nil)
			InitMetrics(prometheus.WrapRegistererWithPrefix("juicefs_", v.registry)) // what a mount registers too
			families, err := v.registry.Gather()
			if err != nil {
				t.Fatalf("gather: %s", err)
			}
			series := map[string]int{}
			for _, mf := range families {
				series[mf.GetName()] = len(mf.GetMetric())
			}
			for name, n := range want {
				if mode == "chunk" {
					n = 0
				}
				if series[name] != n {
					t.Errorf("%s has %d series, want %d", name, series[name], n)
				}
			}
		})
	}
}

// JFS_COMMIT_MODE=chunk is the per-chunk writer: one Write per slice, no WriteMulti, no epochs.
func TestCommitModeChunkCommitsEachSlice(t *testing.T) {
	v, _, rec := createWriterTestVFS(t, "chunk", "", nil)
	if v.writer.(*dataWriter).epochs || v.epochCommit {
		t.Fatal("chunk mode is not set")
	}
	ino, fh := newRangeTestFile(t, v)
	for i := uint64(0); i < 3; i++ {
		mustWrite(t, v, ino, fh, i*meta.ChunkSize, []byte("chunk "+strconv.FormatUint(i, 10)))
	}
	mustFsync(t, v, ino, fh)
	commits := rec.commitsOf(ino)
	if len(commits) != 3 || rec.multiCalls() != 0 {
		t.Fatalf("%d commits and %d WriteMulti calls, want 3 Writes", len(commits), rec.multiCalls())
	}
	for _, c := range commits {
		if c.mode != "sequential" || c.epoch != 0 || len(c.slices) != 1 {
			t.Fatalf("commit %+v, want one Write of one slice outside any epoch", c)
		}
	}
}

// (a) An epoch commits only once every one of its slices is uploaded: while one slice in chunk 0 is held, the slices of
// chunks 1 and 2 are uploaded and nothing is committed; then one WriteMulti carries all three.
func TestEpochCommitWaitsForEverySliceOfTheEpoch(t *testing.T) {
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	ctx := NewLogContext(meta.Background())
	mustWrite(t, v, ino, fh, 0, []byte("zero"))
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("one"))
	mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("two"))
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- v.Fsync(ctx, ino, 0, fh) }()
	store.waitHeld(t, 1)
	waitUntil(t, 10*time.Second, "the slices of chunks 1 and 2 to be uploaded", func() bool {
		for _, s := range epochState(t, v, ino).slices {
			if s.indx != 0 && !s.done {
				return false
			}
		}
		return true
	})
	time.Sleep(300 * time.Millisecond) // the time a commit of chunks 1 and 2 would need to go ahead on its own
	for indx := uint32(0); indx < 3; indx++ {
		if n := committedSlices(t, v, ino, indx); n != 0 {
			t.Fatalf("chunk %d has %d committed slices while a slice of its epoch uploads", indx, n)
		}
	}
	select {
	case e := <-fsynced:
		t.Fatalf("fsync returned %v while a slice was held", e)
	default:
	}
	store.setRule(nil)
	select {
	case e := <-fsynced:
		if e != 0 {
			t.Fatalf("fsync: %s", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fsync did not return after the upload was released")
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 1 || len(commits[0].slices) != 3 || commits[0].reason != "flush" {
		t.Fatalf("commits %+v, want one of the 3 slices, closed by the fsync", commits)
	}
	for i, want := range []string{"zero", "one", "two"} {
		if got := mustRead(t, v, ino, fh, uint64(i)*meta.ChunkSize, len(want)); string(got) != want {
			t.Fatalf("read %q, want %q", got, want)
		}
	}
}

// (b) Epochs commit in order: epoch 2, fully uploaded, waits while a slice of epoch 1 is held.
func TestEpochCommitsWaitForOlderEpochs(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	waitWriterIdle(t)       // no other file's epoch moves the metrics
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY) // the waiting reads hold its reader lock, not the writing handle's
	waits, waited := histogramSum(t, writerEpochOrderWait)
	mustWrite(t, v, ino, fh, 0, []byte("first"))
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	first := startRead(v, ino, rfh, 0, 5) // closes epoch 1, waits for it
	store.waitHeld(t, 1)
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("second"))
	second := startRead(v, ino, rfh, meta.ChunkSize, 6) // closes epoch 2, waits for it
	waitUntil(t, 10*time.Second, "epoch 2 to close and upload", func() bool {
		st := epochState(t, v, ino)
		if len(st.queued) != 2 {
			return false
		}
		for _, s := range st.slices {
			if s.epoch == 2 && !s.done {
				return false
			}
		}
		return true
	})
	time.Sleep(300 * time.Millisecond)
	if n := len(rec.commitsOf(ino)); n != 0 {
		t.Fatalf("%d commits while epoch 1 is held", n)
	}
	if n := committedSlices(t, v, ino, 1); n != 0 {
		t.Fatalf("epoch 2 committed ahead of epoch 1")
	}
	if st := epochState(t, v, ino); st.open != 3 || len(st.queued) != 2 || st.reasons[0] != "read" || st.reasons[1] != "read" {
		t.Fatalf("writer state %+v, want epochs 1 and 2 closed by the reads", st)
	}
	store.setRule(nil)
	if r := receiveRead(t, first, 10*time.Second); r.err != 0 || string(r.data) != "first" {
		t.Fatalf("first read: %q %v", r.data, r.err)
	}
	if r := receiveRead(t, second, 10*time.Second); r.err != 0 || string(r.data) != "second" {
		t.Fatalf("second read: %q %v", r.data, r.err)
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || commits[0].writes != 1 || commits[1].writes != 2 {
		t.Fatalf("commits %+v, want epochs 1 and 2 after 1 and 2 writes", commits)
	}
	// Epoch 2 waited for epoch 1 about the 300 ms above; the bound leaves 50 ms for when its uploads finished.
	if n, sum := histogramSum(t, writerEpochOrderWait); n-waits != 2 || sum-waited < 0.25 {
		t.Fatalf("order wait: %d epochs for %.3fs, want 2 and 0.25s or more", n-waits, sum-waited)
	}
	v.Release(NewLogContext(meta.Background()), ino, rfh)
}

// (c) A boundary freezes every slice of the epoch it closes: a write after it that is contiguous with a slice of the
// closed epoch starts a new slice, which commits with the next epoch, and no slice is in two epochs.
func TestEpochBoundaryFreezesEveryOpenSlice(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	waitWriterIdle(t)
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	frozen := counterValue(t, writerSlicesFrozenByEpoch.WithLabelValues("read"))
	store.setRule(holdAll) // keep every slice pending to look at it
	mustWrite(t, v, ino, fh, 0, bytes.Repeat([]byte("a"), 100))
	mustWrite(t, v, ino, fh, meta.ChunkSize, bytes.Repeat([]byte("b"), 100))
	read := startRead(v, ino, rfh, meta.ChunkSize, 100)
	waitUntil(t, 10*time.Second, "the read to close epoch 1", func() bool { return epochState(t, v, ino).open == 2 })
	// Contiguous with the slice at 0 and inside its only block: without the boundary it would extend that slice.
	mustWrite(t, v, ino, fh, 100, bytes.Repeat([]byte("c"), 100))
	st := epochState(t, v, ino)
	if len(st.slices) != 3 {
		t.Fatalf("%d pending slices, want 3: %+v", len(st.slices), st.slices)
	}
	if s := st.slices[2]; s.epoch != 2 || s.indx != 0 || s.off != 100 || s.chunkPos != 1 || s.frozen {
		t.Fatalf("the write after the boundary went to %+v, want a new, unfrozen slice at 100 in chunk 0, in epoch 2", s)
	}
	if !st.slices[0].frozen || !st.slices[1].frozen || st.slices[0].epoch != 1 || st.slices[1].epoch != 1 {
		t.Fatalf("the slices of epoch 1: %+v, want both frozen", st.slices[:2])
	}
	mustWrite(t, v, ino, fh, 200, bytes.Repeat([]byte("d"), 100)) // contiguous with the new slice: it takes the write
	if st = epochState(t, v, ino); len(st.slices) != 3 || st.slices[2].slen != 200 {
		t.Fatalf("a write contiguous with the open slice did not extend it: %+v", st.slices)
	}
	if d := counterValue(t, writerSlicesFrozenByEpoch.WithLabelValues("read")) - frozen; d != 2 {
		t.Fatalf("%v slices frozen by the boundary, want 2", d)
	}
	store.setRule(nil)
	if r := receiveRead(t, read, 10*time.Second); r.err != 0 || !bytes.Equal(r.data, bytes.Repeat([]byte("b"), 100)) {
		t.Fatalf("read: %q %v", r.data, r.err)
	}
	mustFsync(t, v, ino, fh)
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || len(commits[0].slices) != 2 || len(commits[1].slices) != 1 || commits[1].slices[0].Off != 100 {
		t.Fatalf("commits %+v, want epoch 1 with 2 slices and epoch 2 with the slice at 100", commits)
	}
	want := string(bytes.Repeat([]byte("a"), 100)) + string(bytes.Repeat([]byte("c"), 100)) + string(bytes.Repeat([]byte("d"), 100))
	if got := mustRead(t, v, ino, fh, 0, 300); string(got) != want {
		t.Fatalf("read %q", got)
	}
	v.Release(NewLogContext(meta.Background()), ino, rfh)
}

// (d) A read that overlaps the open epoch closes it and waits for it; one that overlaps only a closed epoch waits for
// that one and leaves the open epoch open; one that overlaps nothing pending does not wait, even with an upload held.
func TestEpochReadsWaitForTheEpochsTheyOverlap(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	mustWrite(t, v, ino, fh, 0, []byte("held"))
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	closing := startRead(v, ino, rfh, 0, 4) // overlaps the open epoch 1: closes it
	store.waitHeld(t, 1)
	if st := epochState(t, v, ino); st.open != 2 || len(st.queued) != 1 || st.reasons[0] != "read" {
		t.Fatalf("writer state %+v, want epoch 1 closed by the read", st)
	}
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("newer")) // epoch 2

	// Nothing pending here: a hole inside the file, and a range beyond both writes.
	if got := mustReadWithin(t, 500*time.Millisecond, v, ino, fh, 32<<20, 4); !bytes.Equal(got, make([]byte, 4)) {
		t.Fatalf("read %q from a hole", got)
	}
	if got := mustReadWithin(t, 500*time.Millisecond, v, ino, fh, meta.ChunkSize+100, 4); len(got) != 0 {
		t.Fatalf("read %q past the end of the file", got)
	}
	// Overlaps only epoch 1 (closed): waits for it, and leaves epoch 2 open.
	older := startRead(v, ino, rfh, 0, 4)
	time.Sleep(300 * time.Millisecond)
	select {
	case r := <-older:
		t.Fatalf("a read of the held epoch returned %q %v", r.data, r.err)
	default:
	}
	if st := epochState(t, v, ino); st.open != 2 || st.openSlices != 1 || st.slices[1].frozen {
		t.Fatalf("writer state %+v, want epoch 2 open, its slice unfrozen", st)
	}
	// Overlaps the open epoch 2: closes it, and waits for it, so for epoch 1 too.
	newer := startRead(v, ino, rfh, meta.ChunkSize, 5)
	waitUntil(t, 10*time.Second, "the read to close epoch 2", func() bool { return epochState(t, v, ino).open == 3 })
	select {
	case r := <-newer:
		t.Fatalf("a read of epoch 2 returned %q %v while epoch 1 was held", r.data, r.err)
	case <-time.After(100 * time.Millisecond):
	}
	store.setRule(nil)
	for _, c := range []struct {
		ch   chan readResult
		want string
	}{{closing, "held"}, {older, "held"}, {newer, "newer"}} {
		if r := receiveRead(t, c.ch, 10*time.Second); r.err != 0 || string(r.data) != c.want {
			t.Fatalf("read %q %v, want %q", r.data, r.err, c.want)
		}
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || commits[1].reason != "read" {
		t.Fatalf("commits %+v, want epochs 1 and 2, closed by reads", commits)
	}
	v.Release(NewLogContext(meta.Background()), ino, rfh)
}

// (d) A read waits for the epoch of the newest pending write it overlaps, not the oldest: with epoch 1 held, a newer
// write of the same range in the open epoch 2 is what the read must return, so it closes epoch 2 and waits for it.
func TestEpochReadWaitsForTheNewestWriteItOverlaps(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	mustWrite(t, v, ino, fh, 0, []byte("older"))
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	first := startRead(v, ino, rfh, 0, 5) // closes epoch 1, and waits for it
	store.waitHeld(t, 1)
	mustWrite(t, v, ino, fh, 0, []byte("newer")) // the same range: a new slice, in epoch 2
	second := startRead(v, ino, rfh, 0, 5)
	waitUntil(t, 10*time.Second, "the read to close epoch 2", func() bool { return epochState(t, v, ino).open == 3 })
	store.setRule(nil)
	if r := receiveRead(t, second, 10*time.Second); r.err != 0 || string(r.data) != "newer" {
		t.Fatalf("read %q %v, want the newer write", r.data, r.err)
	}
	if r := receiveRead(t, first, 10*time.Second); r.err != 0 || (string(r.data) != "older" && string(r.data) != "newer") {
		t.Fatalf("first read %q %v", r.data, r.err)
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || commits[0].reason != "read" || commits[1].reason != "read" {
		t.Fatalf("commits %+v, want epochs 1 and 2, closed by the reads", commits)
	}
	v.Release(NewLogContext(meta.Background()), ino, rfh)
}

// A read that crosses a chunk boundary waits for the newest epoch it overlaps in any of its chunks: here a pending write
// of closed epoch 1 in chunk 0 and one of open epoch 2 in chunk 1, so it closes epoch 2 and gets both writes. Waiting
// for the older epoch (the first chunk's) would return the old bytes of chunk 1.
func TestEpochReadAcrossChunksWaitsForTheNewestEpoch(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	a, b := bytes.Repeat([]byte("a"), 4096), bytes.Repeat([]byte("b"), 4096)
	mustWrite(t, v, ino, fh, meta.ChunkSize-4096, a) // the end of chunk 0, epoch 1
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, meta.ChunkSize-4096)))
	first := startRead(v, ino, rfh, meta.ChunkSize-4096, 16) // closes epoch 1, and waits for it
	store.waitHeld(t, 1)
	mustWrite(t, v, ino, fh, meta.ChunkSize, b) // the start of chunk 1, epoch 2
	second := startRead(v, ino, rfh, meta.ChunkSize-4096, 8192)
	waitUntil(t, 10*time.Second, "epoch 2 to close", func() bool { return epochState(t, v, ino).open == 3 })
	if st := epochState(t, v, ino); len(st.reasons) != 2 || st.reasons[1] != "read" {
		t.Fatalf("writer state %+v, want epochs 1 and 2 queued, both closed by the reads", st)
	}
	store.setRule(nil)
	if r := receiveRead(t, second, 10*time.Second); r.err != 0 || !bytes.Equal(r.data, cat(a, b)) {
		t.Fatalf("read across the chunk boundary: %s %v, want both writes", runs(r.data), r.err)
	}
	if r := receiveRead(t, first, 10*time.Second); r.err != 0 || !bytes.Equal(r.data, a[:16]) {
		t.Fatalf("first read %q %v", r.data, r.err)
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || commits[0].reason != "read" || commits[1].reason != "read" {
		t.Fatalf("commits %+v, want epochs 1 and 2, closed by the reads", commits)
	}
	v.Release(NewLogContext(meta.Background()), ino, rfh)
}

// (d) With JFS_READ_FLUSH_RANGE off a read flushes the whole file, as in chunk mode: whatever range it reads, it closes
// the open epoch and waits for every epoch of the file, the older ones included, and it holds back the writes to the
// file meanwhile, as fsync does.
func TestEpochReadWithoutRangeFlushWaitsForEveryEpoch(t *testing.T) {
	readFlushRange(t, false)
	t.Setenv("JFS_EPOCH_MAX_SLICES", "2") // the third slice closes the epoch at the start of its write (cap)
	v, store, rec := createEpochTestVFS(t, "", nil)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY) // the read holds its reader lock, not the writing handle's
	ctx := NewLogContext(meta.Background())
	mustWrite(t, v, ino, fh, 0, []byte("zero"))
	store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("one"))
	mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("two")) // closes epoch 1
	if st := epochState(t, v, ino); st.open != 2 || len(st.queued) != 1 || st.reasons[0] != "cap" {
		t.Fatalf("writer state %+v, want epoch 1 closed by the slice limit and epoch 2 open", st)
	}
	// A read of a hole: it overlaps no pending write, and waits for epochs 1 and 2 all the same.
	read := startRead(v, ino, rfh, 32<<20, 4)
	waitUntil(t, 10*time.Second, "the read to close epoch 2", func() bool { return epochState(t, v, ino).open == 3 })
	wrote := make(chan syscall.Errno, 1)
	go func() { wrote <- v.Write(ctx, ino, []byte("three"), 3*meta.ChunkSize, fh) }()
	time.Sleep(300 * time.Millisecond)
	select {
	case r := <-read:
		t.Fatalf("the read returned %q %v while epoch 1 was held", r.data, r.err)
	case e := <-wrote:
		t.Fatalf("a write returned %v while a read flushed the file", e)
	default:
	}
	if st := epochState(t, v, ino); len(st.queued) != 2 || st.reasons[1] != "flush" || st.openSlices != 0 {
		t.Fatalf("writer state %+v, want epochs 1 and 2 queued, the second closed by the read's flush, and no write since", st)
	}
	if n := len(rec.commitsOf(ino)); n != 0 {
		t.Fatalf("%d commits while epoch 1 is held", n)
	}
	store.setRule(nil)
	if r := receiveRead(t, read, 10*time.Second); r.err != 0 || !bytes.Equal(r.data, make([]byte, 4)) {
		t.Fatalf("read of a hole: %q %v", r.data, r.err)
	}
	select {
	case e := <-wrote:
		if e != 0 {
			t.Fatalf("write: %s", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write did not go on after the read's flush")
	}
	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	if len(commits) != 2 || commits[0].reason != "cap" || len(commits[0].slices) != 2 || commits[1].reason != "flush" {
		t.Fatalf("commits %+v, want epoch 1 (2 slices, cap) and epoch 2 (flush)", commits)
	}
	for indx := uint32(0); indx < 3; indx++ {
		if n := committedSlices(t, v, ino, indx); n != 1 {
			t.Fatalf("chunk %d has %d committed slices after the read, want 1", indx, n)
		}
	}
	mustFsync(t, v, ino, fh)
	v.Release(ctx, ino, rfh)
}

// JFS_READ_FLUSH_RANGE in epoch mode. Either way a read commits the whole epoch holding the pending writes it overlaps,
// so the writes made before it to other ranges commit with it (in chunk mode the flag leaves them pending,
// TestReadFlushRangeFlag). The flag decides what a read that overlaps no pending write does: with it on, it waits for
// nothing and commits nothing; with it off (unset, empty or false), it flushes the whole file, as fsync does.
func TestEpochReadFlushRangeFlag(t *testing.T) {
	for _, tc := range []struct {
		name, value    string
		unset, enabled bool
	}{
		{name: "unset", unset: true},
		{name: "false", value: "false"},
		{name: "true", value: "true", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JFS_READ_FLUSH_RANGE", tc.value)
			if tc.unset {
				if err := os.Unsetenv("JFS_READ_FLUSH_RANGE"); err != nil {
					t.Fatal(err)
				}
			}
			v, _, rec := createEpochTestVFS(t, "", nil)
			if v.readFlushRange != tc.enabled {
				t.Fatalf("range-limited read flushing is %v, want %v", v.readFlushRange, tc.enabled)
			}
			ino, fh := newRangeTestFile(t, v)
			mustWrite(t, v, ino, fh, 0, []byte("read me"))
			mustWrite(t, v, ino, fh, 3*meta.ChunkSize, []byte("before the read"))
			if got := mustRead(t, v, ino, fh, 0, 7); string(got) != "read me" {
				t.Fatalf("read %q", got)
			}
			for _, indx := range []uint32{0, 3} {
				if n := committedSlices(t, v, ino, indx); n != 1 {
					t.Fatalf("chunk %d has %d committed slices after a read of chunk 0, want 1: the read's epoch holds it", indx, n)
				}
			}
			mustWrite(t, v, ino, fh, 5*meta.ChunkSize, []byte("after the read"))
			// The background flusher leaves the new epoch alone for a second after its last write.
			if got := mustReadWithin(t, 500*time.Millisecond, v, ino, fh, 0, 7); string(got) != "read me" {
				t.Fatalf("read %q", got)
			}
			want := 1
			if tc.enabled {
				want = 0
			}
			if n := committedSlices(t, v, ino, 5); n != want {
				t.Fatalf("a read of chunk 0 left chunk 5 with %d committed slices, want %d", n, want)
			}
			mustFsync(t, v, ino, fh)
			if n := committedSlices(t, v, ino, 5); n != 1 {
				t.Fatalf("chunk 5 has %d committed slices after fsync, want 1", n)
			}
			checkEpochOrder(t, rec.commitsOf(ino))
		})
	}
}

// (h) After a failed upload a read in epoch mode fails, with JFS_READ_FLUSH_RANGE off as with it on
// (TestEpochFailedUploadDropsTheEpochAndEveryNewerOne): the writes it may have to see are lost. In chunk mode a read
// ignores the error of its flush and returns what is committed, as before.
func TestReadAfterAFailedUploadWithoutRangeFlush(t *testing.T) {
	readFlushRange(t, false)
	for _, mode := range []string{"chunk", "epoch"} {
		t.Run(mode, func(t *testing.T) {
			// One retry, so the failing upload gives up after a second.
			v, store, _ := createWriterTestVFS(t, mode, "", func(c *chunk.Config) { c.MaxRetries = 1; noMemoryCache(c) })
			ino, fh := newRangeTestFile(t, v)
			ctx := NewLogContext(meta.Background())
			extendPastChunks(t, v, ino, fh, 1) // committed
			store.setRule(func(string) putAction { return putAction{fail: true} })
			mustWrite(t, v, ino, fh, 0, []byte("lost"))
			for _, off := range []uint64{0, meta.ChunkSize} { // the lost write, and data committed before it
				buf := make([]byte, 3)
				n, e := v.Read(ctx, ino, buf, off, fh)
				switch {
				case mode == "epoch" && e != syscall.EIO:
					t.Fatalf("read at %d after a failed upload: %d bytes, %v, want EIO", off, n, e)
				case mode == "chunk" && e != 0:
					t.Fatalf("read at %d after a failed upload: %v, want the committed data", off, e)
				case mode == "chunk" && off == meta.ChunkSize && string(buf[:n]) != "end":
					t.Fatalf("read at %d after a failed upload: %q, want the committed data", off, buf[:n])
				}
			}
			if e := v.Fsync(ctx, ino, 0, fh); e != syscall.EIO {
				t.Fatalf("fsync after a failed upload: %v, want EIO", e)
			}
			store.setRule(nil)
			v.Release(ctx, ino, fh)
		})
	}
}

// (h) After a failed upload a truncate in epoch mode returns the error and leaves the metadata alone: it would land on
// top of metadata that lacks writes made before it. In chunk mode it ignores the error of its flush, as before.
func TestTruncateAfterAFailedUpload(t *testing.T) {
	for _, mode := range []string{"chunk", "epoch"} {
		t.Run(mode, func(t *testing.T) {
			// One retry, so the failing upload gives up after a second.
			v, store, _ := createWriterTestVFS(t, mode, "", func(c *chunk.Config) { c.MaxRetries = 1; noMemoryCache(c) })
			ino, fh := newRangeTestFile(t, v)
			ctx := NewLogContext(meta.Background())
			extendPastChunks(t, v, ino, fh, 1) // committed
			store.setRule(func(string) putAction { return putAction{fail: true} })
			mustWrite(t, v, ino, fh, 0, []byte("lost"))
			var attr Attr
			e := v.Truncate(ctx, ino, 1, fh, &attr)
			store.setRule(nil)
			if st := v.Meta.GetAttr(ctx, ino, &attr); st != 0 {
				t.Fatalf("getattr: %s", st)
			}
			switch {
			case mode == "epoch" && (e != syscall.EIO || attr.Length != meta.ChunkSize+3):
				t.Fatalf("truncate after a failed upload: %v, length %d; want EIO and the length before", e, attr.Length)
			case mode == "chunk" && (e != 0 || attr.Length != 1):
				t.Fatalf("truncate after a failed upload: %v, length %d; want it truncated", e, attr.Length)
			}
			v.Release(ctx, ino, fh)
		})
	}
}

// A read whose flush is interrupted returns EINTR, with JFS_READ_FLUSH_RANGE off as with it on: it stopped waiting for
// the epoch of the writes it must see, which may still commit. Any other failure of the flush is EIO.
func TestEpochInterruptedReadReturnsEINTR(t *testing.T) {
	forEachReadFlush(t, func(t *testing.T) {
		v, store, _ := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
		mustWrite(t, v, ino, fh, 0, []byte("held"))
		store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
		ctx := NewLogContext(meta.NewContext(1, 0, []uint32{0}))
		read := make(chan readResult, 1)
		go func() {
			buf := make([]byte, 4)
			n, e := v.Read(ctx, ino, buf, 0, rfh)
			read <- readResult{buf[:n], e}
		}()
		store.waitHeld(t, 1)
		ctx.Cancel()
		if r := receiveRead(t, read, 10*time.Second); r.err != syscall.EINTR {
			t.Fatalf("interrupted read: %q %v, want EINTR", r.data, r.err)
		}
		store.setRule(nil)
		mustFsync(t, v, ino, fh)
		if got := mustRead(t, v, ino, rfh, 0, 4); string(got) != "held" {
			t.Fatalf("read %q after the fsync", got)
		}
		v.Release(NewLogContext(meta.Background()), ino, rfh)
	})
}

// (d) A commit drops what the reader fetched ahead of its ranges. With read-ahead on, a read that stops short of a
// pending overwrite fetches the rest of the reader's block from the committed slices, the old bytes of the overwrite
// included; a read of the overwrite waits for its commit, and must then get the new bytes, not the fetched ones.
// (TestReadAheadOfAPendingOverwriteIsReplacedWhenItCommits runs with read-ahead off, so it never holds them.)
func TestCommitInvalidatesTheReadAheadOfItsRanges(t *testing.T) {
	readFlushRange(t, true) // without it the first read commits the overwrite, and the reader never holds its old bytes
	for _, mode := range []string{"chunk", "epoch"} {
		t.Run(mode, func(t *testing.T) {
			v, _, _ := createWriterTestVFS(t, mode, "", func(c *chunk.Config) { c.Readahead = 8 << 20 })
			ino, fh := newRangeTestFile(t, v)
			mustWrite(t, v, ino, fh, 0, bytes.Repeat([]byte("a"), 8<<20))
			mustFsync(t, v, ino, fh)
			rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
			mustWrite(t, v, ino, fh, 1<<20, []byte("bbbb"))
			if got := mustRead(t, v, ino, rfh, 0, 4096); !bytes.Equal(got, bytes.Repeat([]byte("a"), 4096)) {
				t.Fatalf("read %q", got)
			}
			waitUntil(t, 10*time.Second, "the read-ahead to fetch the old bytes of the overwrite", func() bool {
				return readerHolds(v, ino, 1<<20)
			})
			if got := mustRead(t, v, ino, rfh, 1<<20-2, 8); string(got) != "aabbbbaa" {
				t.Fatalf("read %q, want the overwrite", got)
			}
			v.Release(NewLogContext(meta.Background()), ino, rfh)
		})
	}
}

// readerHolds reports whether the reader holds the bytes at off of the file, fetched and ready to be served.
func readerHolds(v *VFS, ino Ino, off uint64) bool {
	var ready bool
	v.reader.(*dataReader).visit(ino, func(f *fileReader) {
		f.visit(func(s *sliceReader) bool {
			if s.state == READY && s.block.off <= off && off < s.block.end() {
				ready = true
			}
			return true
		})
	})
	return ready
}

// (e) Slices frozen before their epoch closes (the fifth unfrozen slice of a chunk, a full chunk) upload early, and
// commit with their epoch, not before it.
func TestEpochAutoFrozenSlicesCommitWithTheirEpoch(t *testing.T) {
	t.Run("fifth unfrozen slice", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		for i := uint64(0); i < 6; i++ { // far apart: each starts a slice; the sixth freezes the first (findWritableSlice)
			mustWrite(t, v, ino, fh, i*(8<<20), []byte("slice "+strconv.FormatUint(i, 10)))
		}
		waitUntil(t, 10*time.Second, "the first slice to be frozen and uploaded", func() bool {
			s, ok := pendingSliceAt(t, v, ino, 0, 0)
			return ok && s.frozen && s.done
		})
		time.Sleep(200 * time.Millisecond)
		if n := len(rec.commitsOf(ino)); n != 0 {
			t.Fatalf("%d commits while the epoch is open", n)
		}
		if st := epochState(t, v, ino); st.open != 1 || st.openSlices != 6 {
			t.Fatalf("writer state %+v, want the 6 slices in the open epoch", st)
		}
		mustFsync(t, v, ino, fh)
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		if len(commits) != 1 || len(commits[0].slices) != 6 || commits[0].slices[0].Off != 0 {
			t.Fatalf("commits %+v, want one of the 6 slices, the auto-frozen one first", commits)
		}
	})
	t.Run("full chunk", func(t *testing.T) {
		t.Setenv("JFS_EPOCH_MAX_AGE_MS", "60000")
		pages := utils.AllocMemory()
		v, _, rec := createEpochTestVFS(t, "", func(c *chunk.Config) { c.BufferSize = 300 << 20; c.MaxUpload = 8; noMemoryCache(c) })
		ino, fh := newRangeTestFile(t, v)
		ctx := NewLogContext(meta.Background())
		// Writes to chunk 1 every 100 ms keep the file from going idle, so the epoch stays open.
		stop := make(chan struct{})
		ticked := make(chan syscall.Errno, 1)
		go func() {
			off := uint64(meta.ChunkSize)
			for {
				select {
				case <-stop:
					ticked <- 0
					return
				case <-time.After(100 * time.Millisecond):
				}
				if e := v.Write(ctx, ino, []byte("tick"), off, fh); e != 0 {
					ticked <- e
					return
				}
				off += 4
			}
		}()
		block := bytes.Repeat([]byte("f"), 1<<20)
		for off := uint64(0); off < meta.ChunkSize; off += 1 << 20 {
			mustWrite(t, v, ino, fh, off, block)
		}
		waitUntil(t, 30*time.Second, "the full chunk to be uploaded", func() bool {
			s, ok := pendingSliceAt(t, v, ino, 0, 0)
			return ok && s.frozen && s.done
		})
		time.Sleep(200 * time.Millisecond)
		n := len(rec.commitsOf(ino))
		st := epochState(t, v, ino)
		close(stop)
		if e := <-ticked; e != 0 {
			t.Fatalf("a write to chunk 1: %s", e)
		}
		if n != 0 || st.open != 1 {
			t.Fatalf("%d commits, writer state %+v: the full chunk committed ahead of its epoch", n, st)
		}
		mustFsync(t, v, ino, fh)
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		if len(commits) != 1 || len(commits[0].slices) != 2 || commits[0].slices[0].Slice.Len != meta.ChunkSize {
			t.Fatalf("commits %+v, want one: the full chunk, then chunk 1", commits)
		}
		if got := mustRead(t, v, ino, fh, meta.ChunkSize-4, 8); string(got) != "fffftick" {
			t.Fatalf("read %q", got)
		}
		v.Release(ctx, ino, fh)
		waitPagesReleased(t, pages)
	})
}

// (f) The background flusher closes an epoch 5 s after its first slice while the file is written without pause, and
// one second after the last write when it goes idle.
func TestEpochBackgroundRules(t *testing.T) {
	t.Run("age", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		start := time.Now()
		var last time.Time
		for n := 0; time.Since(start) < 7*time.Second; n++ {
			for indx := uint64(0); indx < 3; indx++ {
				mustWrite(t, v, ino, fh, indx*meta.ChunkSize+uint64(n)*8, []byte("8 bytes!"))
			}
			last = time.Now()
			time.Sleep(200 * time.Millisecond)
		}
		commits := rec.commitsOf(ino)
		if len(commits) != 1 || commits[0].reason != "age" {
			t.Fatalf("commits %+v in 7 s of writes, want one, closed by its age", commits)
		}
		if took := commits[0].at.Sub(start); took < 5*time.Second || took > 5300*time.Millisecond {
			t.Fatalf("the first epoch committed %s after the first write, want 5 to 5.3 s", took)
		}
		waitUntil(t, 5*time.Second, "the second epoch to commit", func() bool { return len(rec.commitsOf(ino)) == 2 })
		commits = rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		if idle := commits[1].at.Sub(last); commits[1].reason != "idle" || idle < time.Second || idle > 1300*time.Millisecond {
			t.Fatalf("the second epoch closed by %s and committed %s after the last write, want idle, 1 to 1.3 s", commits[1].reason, idle)
		}
		if got := mustRead(t, v, ino, fh, 2*meta.ChunkSize, 8); string(got) != "8 bytes!" {
			t.Fatalf("read %q", got)
		}
	})
	// Soundness condition 3: the age counts from the epoch's first slice. A new slice every 200 ms keeps the file busy,
	// and the flusher freezes each slice once it is idle for a second, so the epoch's oldest unfrozen slice is never
	// much older than a second: an age counted from it would never reach 5 s.
	t.Run("age from the first slice", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		start := time.Now()
		checked := false
		for n := uint64(0); len(rec.commitsOf(ino)) == 0 && time.Since(start) < 7*time.Second; n++ {
			mustWrite(t, v, ino, fh, (n%8)*meta.ChunkSize+(n/8)<<20, []byte("a new slice"))
			if !checked && time.Since(start) > 2500*time.Millisecond {
				checked = true
				if s, ok := pendingSliceAt(t, v, ino, 0, 0); !ok || !s.frozen || s.epoch != 1 || epochState(t, v, ino).open != 1 {
					t.Fatalf("the first slice %+v (found %v), want it frozen while its epoch 1 is still open", s, ok)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		commits := rec.commitsOf(ino)
		if len(commits) == 0 {
			t.Fatal("no commit in 7 s of writes")
		}
		if took := commits[0].at.Sub(start); commits[0].reason != "age" || took < 5*time.Second || took > 5300*time.Millisecond {
			t.Fatalf("the first epoch closed by %s and committed %s after its first slice, want age, 5 to 5.3 s", commits[0].reason, took)
		}
	})
	t.Run("idle", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		start := time.Now()
		mustWrite(t, v, ino, fh, 0, []byte("idle"))
		waitUntil(t, 5*time.Second, "the epoch to commit", func() bool { return len(rec.commitsOf(ino)) == 1 })
		c := rec.commitsOf(ino)[0]
		if took := c.at.Sub(start); c.reason != "idle" || took < time.Second || took > 1300*time.Millisecond {
			t.Fatalf("the epoch closed by %s and committed %s after its write, want idle, 1 to 1.3 s", c.reason, took)
		}
	})
}

// (g) Every path that flushes the writer commits every epoch, in order, with slices pending in several chunks and
// uploads finishing out of order: the background flusher, fsync, truncate, FlushAll and close.
func TestEpochEveryFlushPathCommitsEverything(t *testing.T) {
	pages := utils.AllocMemory()
	v, store, rec := createEpochTestVFS(t, "", func(c *chunk.Config) { c.MaxUpload = 8; noMemoryCache(c) })
	ino, fh := newRangeTestFile(t, v)
	extendPastChunks(t, v, ino, fh, 4)
	ctx := NewLogContext(meta.Background())
	// Each round writes one slice to each of chunks 0-3 (one epoch); the oldest one uploads last.
	writeRound := func(tag string) {
		t.Helper()
		mustWrite(t, v, ino, fh, 0, []byte(tag+" 0"))
		slow := waitSliceIDAt(t, v, ino, 0, 0)
		store.setRule(func(key string) putAction {
			if id, _ := sliceIDOfKey(key); id == slow {
				return putAction{delay: 80 * time.Millisecond}
			}
			return putAction{}
		})
		for i := uint64(1); i < 4; i++ {
			mustWrite(t, v, ino, fh, i*meta.ChunkSize+i*4096, []byte(tag+" "+strconv.FormatUint(i, 10)))
		}
	}
	check := func(tag, reason string) {
		t.Helper()
		if st := epochState(t, v, ino); len(st.slices) != 0 {
			t.Fatalf("%s: slices still pending: %+v", tag, st)
		}
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		if c := commits[len(commits)-1]; c.reason != reason || len(c.slices) != 4 {
			t.Fatalf("%s: the last commit is %+v, want the round's 4 slices, closed by %s", tag, c, reason)
		}
		for i := uint64(0); i < 4; i++ {
			want := tag + " " + strconv.FormatUint(i, 10)
			if got := mustRead(t, v, ino, fh, i*meta.ChunkSize+i*4096, len(want)); string(got) != want {
				t.Fatalf("%s: read %q, want %q", tag, got, want)
			}
		}
	}

	writeRound("idle")
	waitUntil(t, 10*time.Second, "the background flusher to commit the epoch", func() bool { return len(epochState(t, v, ino).slices) == 0 })
	check("idle", "idle")

	writeRound("fsync")
	mustFsync(t, v, ino, fh)
	check("fsync", "flush")

	writeRound("truncate")
	var attr Attr
	if e := v.Truncate(ctx, ino, int64(4*meta.ChunkSize+3), fh, &attr); e != 0 {
		t.Fatalf("truncate: %s", e)
	}
	check("truncate", "flush")

	writeRound("flushall")
	if err := v.FlushAll(""); err != nil {
		t.Fatalf("flush all: %s", err)
	}
	check("flushall", "flush")

	writeRound("close")
	if e := v.Flush(ctx, ino, fh, 0); e != 0 {
		t.Fatalf("close: %s", e)
	}
	check("close", "flush")
	v.Release(ctx, ino, fh)
	waitWriterIdle(t)
	waitPagesReleased(t, pages)
}

// (h) Failures. A failed upload in epoch k: nothing of epoch k commits, the objects of its uploaded slices are removed,
// epoch k+1 is dropped with its objects, and fsync, reads and writes return EIO; nothing hangs, and the buffers go back.
func TestEpochFailedUploadDropsTheEpochAndEveryNewerOne(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	waitWriterIdle(t)
	pages := utils.AllocMemory()
	// One retry, so the failing upload gives up after a second.
	v, store, rec := createEpochTestVFS(t, "", func(c *chunk.Config) { c.MaxRetries = 1; noMemoryCache(c) })
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	ctx := NewLogContext(meta.Background())
	extendPastChunks(t, v, ino, fh, 4) // epoch 1
	abandoned := counterValue(t, writerSlicesAbandoned)
	failed, dropped := counterValue(t, writerEpochs.WithLabelValues("failed")), counterValue(t, writerEpochs.WithLabelValues("abandoned"))
	mustWrite(t, v, ino, fh, 0, []byte("fails"))
	store.setRule(failSlice(waitSliceIDAt(t, v, ino, 0, 0)))
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("uploaded, then removed"))
	uploaded := waitSliceIDAt(t, v, ino, 1, 0)
	// The reads close epochs 2 and 3; slice 1 of epoch 2 and the slice of epoch 3 upload at once, well before the
	// failing upload of epoch 2 gives up.
	read2 := startRead(v, ino, rfh, meta.ChunkSize, 5)
	waitUntil(t, 10*time.Second, "the read to close epoch 2", func() bool { return epochState(t, v, ino).open == 3 })
	mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("dropped"))
	newer := waitSliceIDAt(t, v, ino, 2, 0)
	read3 := startRead(v, ino, rfh, 2*meta.ChunkSize, 5)
	waitUntil(t, 10*time.Second, "slice 1 of epoch 2 to be uploaded", func() bool { return len(store.keysOf(uploaded)) > 0 })

	for _, r := range []chan readResult{read2, read3} {
		if res := receiveRead(t, r, 30*time.Second); res.err != syscall.EIO {
			t.Fatalf("a read waiting for a failed epoch: %q %v, want EIO", res.data, res.err)
		}
	}
	fsyncStart := time.Now()
	if e := v.Fsync(ctx, ino, 0, fh); e != syscall.EIO {
		t.Fatalf("fsync after a failed upload: %v, want EIO", e)
	}
	if took := time.Since(fsyncStart); took > 5*time.Second {
		t.Fatalf("fsync took %s after a failed upload", took)
	}
	putBefore := store.storedIDs()
	if e := v.Write(ctx, ino, []byte("after"), 3*meta.ChunkSize, fh); e != syscall.EIO {
		t.Fatalf("write after the failure: %v, want EIO", e)
	}
	if n, e := v.Read(ctx, ino, make([]byte, 3), 4*meta.ChunkSize, rfh); e != syscall.EIO {
		t.Fatalf("read of data committed before the failure: %d bytes, %v, want EIO", n, e)
	}
	if e := v.Fsync(ctx, ino, 0, fh); e != syscall.EIO {
		t.Fatalf("second fsync: %v, want EIO", e)
	}
	if commits := rec.commitsOf(ino); len(commits) != 1 {
		t.Fatalf("%d commits, want only epoch 1: %+v", len(commits), commits)
	}
	for indx := uint32(0); indx < 4; indx++ {
		if n := committedSlices(t, v, ino, indx); n != 0 {
			t.Fatalf("chunk %d has %d committed slices after its epoch failed", indx, n)
		}
	}
	waitUntil(t, 10*time.Second, "the uploaded objects to be removed", func() bool {
		return store.objectsExist(store.keysOf(uploaded)) == 0 && store.objectsExist(store.keysOf(newer)) == 0
	})
	for id := range store.storedIDs() {
		if !putBefore[id] {
			t.Fatalf("slice %d was uploaded after the failure", id)
		}
	}
	if d := counterValue(t, writerSlicesAbandoned) - abandoned; d != 3 {
		t.Fatalf("%v slices dropped, want 3", d)
	}
	if f, d := counterValue(t, writerEpochs.WithLabelValues("failed"))-failed, counterValue(t, writerEpochs.WithLabelValues("abandoned"))-dropped; f != 1 || d != 1 {
		t.Fatalf("%v epochs failed and %v abandoned, want 1 each", f, d)
	}
	v.Release(ctx, ino, rfh)
	v.Release(ctx, ino, fh)
	waitWriterIdle(t)
	waitPagesReleased(t, pages)

	// Once the file's writer is gone, a new handle reads what was committed.
	waitUntil(t, 10*time.Second, "the file's writer to go", func() bool { return v.writer.(*dataWriter).find(ino) == nil })
	fh = mustOpen(t, v, ino, syscall.O_RDWR)
	if got := mustRead(t, v, ino, fh, meta.ChunkSize, 5); !bytes.Equal(got, make([]byte, 5)) {
		t.Fatalf("after reopening, the dropped write reads %q, want the hole", got)
	}
	if got := mustRead(t, v, ino, fh, 4*meta.ChunkSize, 3); string(got) != "end" {
		t.Fatalf("after reopening, the committed data reads %q", got)
	}
	v.Release(ctx, ino, fh)
}

// (h) A commit the metadata engine refuses before its transaction (ENOSPC) leaves nothing: the epoch's objects are
// removed. A commit whose outcome the writer cannot know (EIO) may have landed: its objects are kept, and every newer
// epoch is dropped with its objects.
func TestEpochFailedCommit(t *testing.T) {
	t.Run("ENOSPC", func(t *testing.T) {
		v, store, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		extendPastChunks(t, v, ino, fh, 4) // commit 1
		rec.mu.Lock()
		rec.inject = func(call int) (syscall.Errno, bool) {
			if call == 2 {
				return syscall.ENOSPC, false
			}
			return 0, false
		}
		rec.mu.Unlock()
		mustWrite(t, v, ino, fh, 0, []byte("zero"))
		mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("one"))
		ids := []uint64{waitSliceIDAt(t, v, ino, 0, 0), waitSliceIDAt(t, v, ino, 1, 0)}
		if e := v.Fsync(NewLogContext(meta.Background()), ino, 0, fh); e != syscall.ENOSPC {
			t.Fatalf("fsync: %v, want ENOSPC", e)
		}
		for _, id := range ids {
			keys := store.keysOf(id)
			if len(keys) == 0 {
				t.Fatalf("slice %d was not uploaded", id)
			}
			waitUntil(t, 10*time.Second, "the objects of the refused epoch to be removed", func() bool { return store.objectsExist(keys) == 0 })
		}
		if e := v.Write(NewLogContext(meta.Background()), ino, []byte("x"), 0, fh); e != syscall.ENOSPC {
			t.Fatalf("write after the failure: %v, want ENOSPC", e)
		}
		if n := len(rec.commitsOf(ino)); n != 1 {
			t.Fatalf("%d commits, want 1", n)
		}
		v.Release(NewLogContext(meta.Background()), ino, fh)
	})
	t.Run("EIO", func(t *testing.T) {
		readFlushRange(t, true) // reads close epochs while writes go on
		waitWriterIdle(t)       // no other file's epoch moves the metrics
		v, store, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
		ctx := NewLogContext(meta.Background())
		extendPastChunks(t, v, ino, fh, 4) // commit 1
		errorCloses := counterValue(t, writerEpochClosed.WithLabelValues("error"))
		rec.mu.Lock()
		rec.inject = func(call int) (syscall.Errno, bool) {
			if call == 2 {
				return syscall.EIO, true // after the batch landed
			}
			return 0, false
		}
		rec.mu.Unlock()
		rec.pause.Lock() // holds the commit of epoch 2 until epoch 3 is uploaded
		mustWrite(t, v, ino, fh, 0, []byte("landed"))
		landed := waitSliceIDAt(t, v, ino, 0, 0)
		read2 := startRead(v, ino, rfh, 0, 6)
		rec.waitPaused(t)
		mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("dropped"))
		dropped := waitSliceIDAt(t, v, ino, 1, 0)
		read3 := startRead(v, ino, rfh, meta.ChunkSize, 7)
		waitUntil(t, 10*time.Second, "epoch 3 to close and upload", func() bool {
			st := epochState(t, v, ino)
			return len(st.queued) == 2 && st.slices[1].done
		})
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("open")) // the open epoch 4
		rec.pause.Unlock()
		for _, r := range []chan readResult{read2, read3} {
			if res := receiveRead(t, r, 10*time.Second); res.err != syscall.EIO {
				t.Fatalf("a read after a commit failed: %q %v, want EIO", res.data, res.err)
			}
		}
		// The failure closed the open epoch (fail), so its slice is dropped at once rather than after the flusher's
		// second of idleness.
		if st := epochState(t, v, ino); st.open != 5 || st.openSlices != 0 {
			t.Fatalf("writer state %+v after the failure, want epoch 4 closed", st)
		}
		if d := counterValue(t, writerEpochClosed.WithLabelValues("error")) - errorCloses; d != 1 {
			t.Fatalf("%v epochs closed by the failure, want 1", d)
		}
		if e := v.Fsync(ctx, ino, 0, fh); e != syscall.EIO {
			t.Fatalf("fsync: %v, want EIO", e)
		}
		waitUntil(t, 10*time.Second, "the objects of the dropped epoch to be removed", func() bool {
			return store.objectsExist(store.keysOf(dropped)) == 0
		})
		if keys := store.keysOf(landed); len(keys) == 0 || store.objectsExist(keys) != len(keys) {
			t.Fatalf("the objects of the epoch whose commit failed with EIO were removed (%d of %d left)", store.objectsExist(keys), len(keys))
		}
		v.Release(ctx, ino, rfh)
		v.Release(ctx, ino, fh)
		waitUntil(t, 10*time.Second, "the file's writer to go", func() bool { return v.writer.(*dataWriter).find(ino) == nil })
		// The batch did land: with its objects kept, its data reads back.
		fh = mustOpen(t, v, ino, syscall.O_RDWR)
		if got := mustRead(t, v, ino, fh, 0, 6); string(got) != "landed" {
			t.Fatalf("after reopening, the landed epoch reads %q", got)
		}
		if got := mustRead(t, v, ino, fh, meta.ChunkSize, 7); !bytes.Equal(got, make([]byte, 7)) {
			t.Fatalf("after reopening, the dropped epoch reads %q, want the hole", got)
		}
		v.Release(ctx, ino, fh)
	})
}

// (h) A failed upload dooms its epoch and every newer one as soon as the slice gives up, before commitEpochs gets to
// that epoch: the slices of newer epochs frozen after it drop their data without uploading it. An older epoch, held
// meanwhile, still commits.
func TestEpochFailedUploadStopsTheUploadsOfNewerEpochs(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	// One retry, so the failing upload gives up after a second; room for it next to the two held uploads.
	v, store, rec := createEpochTestVFS(t, "", func(c *chunk.Config) { c.MaxRetries = 1; c.MaxUpload = 8 })
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	ctx := NewLogContext(meta.Background())
	extendPastChunks(t, v, ino, fh, 4) // epoch 1
	mustWrite(t, v, ino, fh, 0, []byte("older"))
	older := waitSliceIDAt(t, v, ino, 0, 0)
	store.setRule(holdIDs(older))
	read2 := startRead(v, ino, rfh, 0, 5) // closes epoch 2, held
	store.waitHeld(t, 1)
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("fails"))
	mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("held"))
	failing, held := waitSliceIDAt(t, v, ino, 1, 0), waitSliceIDAt(t, v, ino, 2, 0)
	store.setRule(func(key string) putAction {
		id, _ := sliceIDOfKey(key)
		return putAction{hold: id == older || id == held, fail: id == failing}
	})
	read3 := startRead(v, ino, rfh, meta.ChunkSize, 5) // closes epoch 3
	waitUntil(t, 10*time.Second, "the failing upload to give up", func() bool {
		s, ok := pendingSliceAt(t, v, ino, 1, 0)
		return ok && s.done
	})
	mustWrite(t, v, ino, fh, 3*meta.ChunkSize, []byte("newer"))
	newer := waitSliceIDAt(t, v, ino, 3, 0)
	read4 := startRead(v, ino, rfh, 3*meta.ChunkSize, 5) // closes epoch 4: its slice drops its data
	waitUntil(t, 10*time.Second, "the slice of epoch 4 to be dropped", func() bool {
		s, ok := pendingSliceAt(t, v, ino, 3, 0)
		return ok && s.done
	})
	if st := epochState(t, v, ino); st.err != 0 || len(st.queued) != 3 {
		t.Fatalf("writer state %+v, want epochs 2-4 pending and no error yet", st)
	}
	if keys := store.keysOf(newer); len(keys) != 0 {
		t.Fatalf("the slice of epoch 4 was uploaded (%v) after an upload of epoch 3 failed", keys)
	}

	store.setRule(holdIDs(held)) // epoch 2 commits
	if r := receiveRead(t, read2, 10*time.Second); r.err != 0 || string(r.data) != "older" {
		t.Fatalf("read of the older epoch: %q %v", r.data, r.err)
	}
	if commits := rec.commitsOf(ino); len(commits) != 2 || commits[1].epoch != 2 {
		t.Fatalf("commits %+v, want epochs 1 and 2", commits)
	}
	store.setRule(nil) // epoch 3 fails, and epoch 4 is dropped
	for _, r := range []chan readResult{read3, read4} {
		if res := receiveRead(t, r, 10*time.Second); res.err != syscall.EIO {
			t.Fatalf("a read of a failed or dropped epoch: %q %v, want EIO", res.data, res.err)
		}
	}
	waitUntil(t, 10*time.Second, "the objects of the failed epoch to be removed", func() bool {
		return len(store.keysOf(held)) > 0 && store.objectsExist(store.keysOf(held)) == 0
	})
	if n := len(rec.commitsOf(ino)); n != 2 {
		t.Fatalf("%d commits, want 2", n)
	}
	if keys := store.keysOf(newer); len(keys) != 0 {
		t.Fatalf("the slice of epoch 4 was uploaded (%v)", keys)
	}
	v.Release(ctx, ino, rfh)
	v.Release(ctx, ino, fh)
}

// (h) A slice dropped after a failure while one of its uploads is still in flight (FlushTo uploads the full blocks of a
// slice while it takes writes) waits for that upload before it removes its blocks: a block that landed after the
// removal would stay in the object store, with no metadata referring to it. A slice whose own upload failed removes
// the blocks that did land, its last, partial one included.
func TestEpochDroppedSliceLeavesNoObjectBehind(t *testing.T) {
	t.Run("upload in flight", testEpochDroppedSliceWaitsForItsUploads)
	t.Run("failed upload", func(t *testing.T) {
		// One retry, so the failing upload gives up after a second.
		v, store, _ := createEpochTestVFS(t, "", func(c *chunk.Config) { c.MaxRetries = 1; noMemoryCache(c) })
		ino, fh := newRangeTestFile(t, v)
		mustWrite(t, v, ino, fh, 0, []byte("x"))
		id := waitSliceIDAt(t, v, ino, 0, 0)
		first := fmt.Sprintf("%d_0_", id)
		store.setRule(func(key string) putAction { return putAction{fail: strings.HasPrefix(path.Base(key), first)} })
		mustWrite(t, v, ino, fh, 1, bytes.Repeat([]byte("y"), 4<<20)) // a full first block, and a last one of a byte
		if e := v.Fsync(NewLogContext(meta.Background()), ino, 0, fh); e != syscall.EIO {
			t.Fatalf("fsync: %v, want EIO", e)
		}
		keys := store.keysOf(id)
		if len(keys) != 1 || !strings.HasSuffix(keys[0], "_1_1") {
			t.Fatalf("the slice put %v, want its last block only", keys)
		}
		waitUntil(t, 10*time.Second, "the last block of the failed slice to be removed", func() bool { return store.objectsExist(keys) == 0 })
		v.Release(NewLogContext(meta.Background()), ino, fh)
	})
}

func testEpochDroppedSliceWaitsForItsUploads(t *testing.T) {
	readFlushRange(t, true) // a read closes epoch 2 while epoch 3 takes writes
	v, store, rec := createEpochTestVFS(t, "", noMemoryCache)
	ino, fh := newRangeTestFile(t, v)
	rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
	ctx := NewLogContext(meta.Background())
	extendPastChunks(t, v, ino, fh, 4) // commit 1
	rec.mu.Lock()
	rec.inject = func(call int) (syscall.Errno, bool) {
		if call == 2 {
			return syscall.EIO, true
		}
		return 0, false
	}
	rec.mu.Unlock()
	rec.pause.Lock() // holds the commit of epoch 2 while epoch 3 takes its writes
	mustWrite(t, v, ino, fh, 0, []byte("fails"))
	read2 := startRead(v, ino, rfh, 0, 5)
	rec.waitPaused(t)
	// Epoch 3, open: a slice whose first block is full, so it uploads while the slice still takes writes; held.
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("x"))
	id := waitSliceIDAt(t, v, ino, 1, 0)
	store.setRule(holdIDs(id))
	mustWrite(t, v, ino, fh, meta.ChunkSize+1, bytes.Repeat([]byte("y"), 4<<20))
	store.waitHeld(t, 1)
	rec.pause.Unlock() // epoch 2 fails, which closes epoch 3 and drops its slice
	if r := receiveRead(t, read2, 10*time.Second); r.err != syscall.EIO {
		t.Fatalf("read: %q %v, want EIO", r.data, r.err)
	}
	time.Sleep(300 * time.Millisecond) // the time the dropped slice needs to remove its blocks, the held one included
	store.setRule(nil)                 // the held block lands
	waitUntil(t, 10*time.Second, "the held block to land", func() bool { return len(store.keysOf(id)) > 0 })
	waitUntil(t, 10*time.Second, "the block that landed after the drop to be removed", func() bool {
		return store.objectsExist(store.keysOf(id)) == 0
	})
	if e := v.Fsync(ctx, ino, 0, fh); e != syscall.EIO {
		t.Fatalf("fsync: %v, want EIO", e)
	}
	v.Release(ctx, ino, rfh)
	v.Release(ctx, ino, fh)
}

// (i) The epoch limits are checked at the start of a Write, so a write that crosses a chunk boundary is never split
// between two epochs: the client's slice limit, and the engine's chunk and per-chunk limits.
func TestEpochLimitsCloseTheEpochBeforeAWrite(t *testing.T) {
	t.Run("slices", func(t *testing.T) {
		t.Setenv("JFS_EPOCH_MAX_SLICES", "5")
		v, _, rec := createEpochTestVFS(t, "", nil)
		ino, fh := newRangeTestFile(t, v)
		for i := uint64(0); i < 3; i++ {
			mustWrite(t, v, ino, fh, i<<20, []byte("single"))
		}
		mustWrite(t, v, ino, fh, meta.ChunkSize-8, []byte("crosses the boundary"))  // 3 + 2 slices: fits
		mustWrite(t, v, ino, fh, 8<<20, []byte("sixth"))                            // 5 + 1: closes epoch 1
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize-8, []byte("crosses another one")) // 1 + 2: fits
		mustWrite(t, v, ino, fh, 4<<20, []byte("fourth"))                           // 3 + 1: fits
		mustWrite(t, v, ino, fh, 3*meta.ChunkSize-8, []byte("crosses a third"))     // 4 + 2: closes epoch 2
		mustFsync(t, v, ino, fh)
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		var sizes []int
		var reasons []string
		for _, c := range commits {
			sizes, reasons = append(sizes, len(c.slices)), append(reasons, c.reason)
		}
		if fmt.Sprint(sizes) != "[5 4 2]" || fmt.Sprint(reasons) != "[cap cap flush]" {
			t.Fatalf("epochs of %v slices closed by %v, want [5 4 2] by [cap cap flush]", sizes, reasons)
		}
		if got := mustRead(t, v, ino, fh, 2*meta.ChunkSize-8, 19); string(got) != "crosses another one" {
			t.Fatalf("read %q", got)
		}
	})
	t.Run("engine", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", nil)
		rec.limits = &meta.WriteMultiLimits{Slices: 100, Chunks: 2, ChunkSlices: 2}
		ino, fh := newRangeTestFile(t, v)
		mustWrite(t, v, ino, fh, 0, []byte("chunk 0"))
		mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("chunk 1"))
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize, []byte("a third chunk: closes epoch 1"))
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize+1<<20, []byte("chunk 2 again"))
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize+2<<20, []byte("a third slice in chunk 2: closes epoch 2"))
		mustWrite(t, v, ino, fh, meta.ChunkSize-8, []byte("chunks 0 and 1: 3 chunks, closes epoch 3"))
		mustWrite(t, v, ino, fh, 2*meta.ChunkSize-8, []byte("chunks 1 and 2: 3 chunks, closes epoch 4"))
		mustFsync(t, v, ino, fh)
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		var got []string
		for _, c := range commits {
			var chunks []uint32
			for _, s := range c.slices {
				chunks = append(chunks, s.Indx)
			}
			got = append(got, fmt.Sprint(chunks))
		}
		if want := "[0 1] [2 2] [2] [0 1] [1 2]"; strings.Join(got, " ") != want {
			t.Fatalf("epochs with slices in chunks %v, want %s", got, want)
		}
	})
}

// (i) The per-file slice caps hold writes back: behind a held epoch (gated: all pending slices) and with every upload
// held (buffered: slices holding buffers), where the slices of older epochs hold the count and the open epoch stays
// open; and when the open epoch alone holds the count, where closing it lets the writes go on at once instead of after
// the second the file takes to go idle.
func TestEpochSliceCapsHoldWritesBack(t *testing.T) {
	for _, tc := range []struct {
		reason            string
		buffered, pending int
		holdAll           bool
	}{
		{"gated", defaultMaxBufferedSlices, 20, false},
		{"buffered", 10, defaultMaxPendingSlices, true},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			readFlushRange(t, true) // a read closes the epoch of the held slice while writes go on
			v, store, rec := createEpochTestVFS(t, "", noMemoryCache)
			dw := v.writer.(*dataWriter)
			dw.maxBufferedSlices, dw.maxPendingSlices = tc.buffered, tc.pending // before any write of this VFS
			ino, fh := newRangeTestFile(t, v)
			rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
			ctx := NewLogContext(meta.Background())
			extendPastChunks(t, v, ino, fh, 4)
			waits, waited := capWaits(t, tc.reason)
			mustWrite(t, v, ino, fh, 0, []byte("held"))
			if tc.holdAll {
				store.setRule(holdAll)
			} else {
				store.setRule(holdSlice(waitSliceIDAt(t, v, ino, 0, 0)))
			}
			held := startRead(v, ino, rfh, 0, 4) // closes the epoch of the held slice
			waitUntil(t, 10*time.Second, "the read to close the epoch", func() bool { return len(epochState(t, v, ino).queued) == 1 })
			limit := min(tc.buffered, tc.pending)
			wrote := make(chan error, 1)
			go func() {
				for i := 1; i <= limit+5; i++ { // each in another chunk than the one before: a new slice each
					if e := v.Write(ctx, ino, []byte("more"), uint64(1+i%3)*meta.ChunkSize+uint64(i)*8192, fh); e != 0 {
						wrote <- fmt.Errorf("write %d: %s", i, e)
						return
					}
				}
				wrote <- nil
			}()
			f := dw.find(ino)
			waitUntil(t, 10*time.Second, "a write to be held back", func() bool {
				f.Lock()
				defer f.Unlock()
				return f.capwaiting > 0
			})
			// The held slice of the closed epoch is what holds the count at the limit: the open epoch has fewer slices
			// than the limit, so the wait leaves it open.
			st := epochState(t, v, ino)
			if len(st.slices) != limit || st.openSlices != limit-1 || len(st.queued) != 1 || st.reasons[0] != "read" {
				t.Errorf("a write is held back with %d slices pending, %d in the open epoch, closed epochs %v by %v; want %d, %d, one by read",
					len(st.slices), st.openSlices, st.queued, st.reasons, limit, limit-1)
			}
			time.Sleep(100 * time.Millisecond)
			select {
			case err := <-wrote:
				t.Fatalf("the writes finished at the cap (%v)", err)
			default:
			}
			store.setRule(nil)
			select {
			case err := <-wrote:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the writes did not go on after the upload was released")
			}
			if r := receiveRead(t, held, 10*time.Second); r.err != 0 || string(r.data) != "held" {
				t.Fatalf("read %q %v", r.data, r.err)
			}
			mustFsync(t, v, ino, fh)
			checkEpochOrder(t, rec.commitsOf(ino))
			if n, sum := capWaits(t, tc.reason); n == waits || sum-waited < 0.1 {
				t.Fatalf("%s cap: %d waits for %.3fs recorded, want 1 or more and 0.1s or more", tc.reason, n-waits, sum-waited)
			}
			v.Release(ctx, ino, rfh)
			v.Release(ctx, ino, fh)
		})
	}
	t.Run("open epoch", func(t *testing.T) {
		v, _, rec := createEpochTestVFS(t, "", noMemoryCache)
		dw := v.writer.(*dataWriter)
		dw.maxBufferedSlices = 10
		ino, fh := newRangeTestFile(t, v)
		waits, waited := capWaits(t, "buffered")
		start := time.Now()
		for i := 0; i < 35; i++ { // one slice in each of 10 chunks fills the open epoch; none has started uploading
			mustWrite(t, v, ino, fh, uint64(i%10)*meta.ChunkSize+uint64(i/10)*8192, []byte("more"))
		}
		took := time.Since(start)
		n, sum := capWaits(t, "buffered")
		if n-waits < 3 || took > time.Second || sum-waited > 0.5 {
			t.Fatalf("35 writes took %s with %d cap waits for %.3fs, want 3 or more waits, all under a second", took, n-waits, sum-waited)
		}
		mustFsync(t, v, ino, fh)
		commits := rec.commitsOf(ino)
		checkEpochOrder(t, commits)
		if len(commits) < 4 || commits[0].reason != "cap" || len(commits[0].slices) != 10 {
			t.Fatalf("commits %+v, want epochs of 10 slices closed by the cap", commits)
		}
	})
}

// holdIDs holds the uploads of the slices given, and passes every other one.
func holdIDs(ids ...uint64) func(string) putAction {
	held := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		held[id] = true
	}
	return func(key string) putAction {
		id, _ := sliceIDOfKey(key)
		return putAction{hold: held[id]}
	}
}

// (i) A write held at a slice cap by the slices of an older epoch leaves the open epoch open: closing it could not take
// the count down (their uploads, buffered, or their commit, gated, do), and would only cut its slices short, one new
// slice per held write. A sequential stream held at the cap goes on in the same slice.
func TestEpochCapWaitLeavesTheOpenEpochOpen(t *testing.T) {
	readFlushRange(t, true) // reads close epochs while writes go on
	for _, reason := range []string{"buffered", "gated"} {
		t.Run(reason, func(t *testing.T) {
			waitWriterIdle(t) // no other file's epoch moves the metrics
			v, store, rec := createEpochTestVFS(t, "", func(c *chunk.Config) { c.MaxUpload = 8; noMemoryCache(c) })
			dw := v.writer.(*dataWriter)
			if reason == "buffered" {
				dw.maxBufferedSlices = 4 // before any write of this VFS
			} else {
				dw.maxPendingSlices = 4
			}
			ino, fh := newRangeTestFile(t, v)
			rfh := mustOpen(t, v, ino, syscall.O_RDONLY)
			ctx := NewLogContext(meta.Background())
			extendPastChunks(t, v, ino, fh, 4) // epoch 1
			capCloses := counterValue(t, writerEpochClosed.WithLabelValues("cap"))
			waits, _ := capWaits(t, reason)
			// Epoch 2: a slice in each of chunks 1-3, closed by a read. Buffered: the three uploads are held. Gated: the
			// first is, so the epoch waits with the other two uploaded.
			var ids []uint64
			for i := uint64(1); i <= 3; i++ {
				mustWrite(t, v, ino, fh, i*meta.ChunkSize, []byte("older"))
				ids = append(ids, waitSliceIDAt(t, v, ino, uint32(i), 0))
			}
			held := ids[:1]
			if reason == "buffered" {
				held = ids
			}
			store.setRule(holdIDs(held...))
			read := startRead(v, ino, rfh, meta.ChunkSize, 5) // closes epoch 2
			store.waitHeld(t, len(held))
			waitUntil(t, 10*time.Second, "the uploads of epoch 2 that are not held", func() bool {
				done := 0
				for _, s := range epochState(t, v, ino).slices {
					if s.done {
						done++
					}
				}
				return done == 3-len(held)
			})

			// A sequential stream in chunk 0: its first write is the fourth pending slice, the second is held.
			block := bytes.Repeat([]byte("s"), 4096)
			const writes = 4
			mustWrite(t, v, ino, fh, 0, block)
			wrote := make(chan syscall.Errno, 1)
			go func() {
				for i := uint64(1); i < writes; i++ {
					if e := v.Write(ctx, ino, block, i*4096, fh); e != 0 {
						wrote <- e
						return
					}
				}
				wrote <- 0
			}()
			f := dw.find(ino)
			waitUntil(t, 10*time.Second, "a write to be held back", func() bool {
				f.Lock()
				defer f.Unlock()
				return f.capwaiting > 0
			})
			time.Sleep(200 * time.Millisecond) // the time the held write would need to close the open epoch
			st := epochState(t, v, ino)
			if st.open != 3 || st.openSlices != 1 || len(st.queued) != 1 || st.slices[len(st.slices)-1].frozen {
				t.Fatalf("writer state %+v with a write held at the %s cap, want epoch 3 open, its one slice unfrozen", st, reason)
			}

			// One upload of epoch 2 (buffered) or the whole epoch (gated) goes through: the stream goes on, in its slice.
			if reason == "buffered" {
				store.setRule(holdIDs(ids[1:]...))
			} else {
				store.setRule(nil)
			}
			select {
			case e := <-wrote:
				if e != 0 {
					t.Fatalf("write: %s", e)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the writes did not go on below the cap")
			}
			st = epochState(t, v, ino)
			if last := st.slices[len(st.slices)-1]; st.open != 3 || st.openSlices != 1 || last.off != 0 || last.slen != writes*4096 {
				t.Fatalf("writer state %+v, want the stream in one slice of %d bytes in epoch 3", st, writes*4096)
			}
			store.setRule(nil)
			if r := receiveRead(t, read, 10*time.Second); r.err != 0 || string(r.data) != "older" {
				t.Fatalf("read %q %v", r.data, r.err)
			}
			mustFsync(t, v, ino, fh)
			commits := rec.commitsOf(ino)
			checkEpochOrder(t, commits)
			if last := commits[len(commits)-1]; len(commits) != 3 || len(last.slices) != 1 || last.slices[0].Slice.Len != writes*4096 || last.reason != "flush" {
				t.Fatalf("commits %+v, want epoch 3 as one slice of the stream, closed by the fsync", commits)
			}
			if d := counterValue(t, writerEpochClosed.WithLabelValues("cap")) - capCloses; d != 0 {
				t.Fatalf("%v epochs closed by a cap", d)
			}
			if n, _ := capWaits(t, reason); n == waits {
				t.Fatalf("no %s cap wait recorded", reason)
			}
			v.Release(ctx, ino, rfh)
			v.Release(ctx, ino, fh)
		})
	}
}

// (i) A write that meets a hard stall of the write buffers (twice their size) closes the open epoch, so its slices upload
// and free their last, partial block; the other writes of the same stall do not, since the usage is the whole
// client's: until a write sees the buffers below their size, which ends the stall.
func TestEpochBufferStallClosesTheOpenEpochOnce(t *testing.T) {
	waitWriterIdle(t)
	v, _, rec := createEpochTestVFS(t, "", nil)
	dw := v.writer.(*dataWriter)
	var used atomic.Int64
	dw.bufferUsed = used.Load // before any write of this VFS
	size := dw.bufferSize
	ino, fh := newRangeTestFile(t, v)
	ctx := NewLogContext(meta.Background())
	closes := counterValue(t, writerEpochClosed.WithLabelValues("buffer"))
	block := bytes.Repeat([]byte("b"), 4096)
	write := func(off uint64) chan syscall.Errno {
		ch := make(chan syscall.Errno, 1)
		go func() { ch <- v.Write(ctx, ino, block, off, fh) }()
		return ch
	}
	wrote := func(ch chan syscall.Errno) {
		t.Helper()
		select {
		case e := <-ch:
			if e != 0 {
				t.Fatalf("write: %s", e)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a write did not return after the stall")
		}
	}

	mustWrite(t, v, ino, fh, 0, block) // epoch 1
	used.Store(3 * size)
	first := write(4096) // closes epoch 1, and waits
	waitUntil(t, 10*time.Second, "the stalled write to close epoch 1", func() bool { return epochState(t, v, ino).open == 2 })
	used.Store(size + size/2) // over the buffer size, but not stalled
	wrote(first)              // a new slice in epoch 2

	used.Store(3 * size) // stalled again, and the buffers never went below their size: the same stall
	second := write(8192)
	time.Sleep(300 * time.Millisecond) // the time it would need to close epoch 2
	if st := epochState(t, v, ino); st.open != 2 || st.openSlices != 1 {
		t.Fatalf("writer state %+v, want epoch 2 left open by the second write of the stall", st)
	}
	select {
	case e := <-second:
		t.Fatalf("a write returned %v in a stall", e)
	default:
	}
	used.Store(size / 2)
	wrote(second)

	mustWrite(t, v, ino, fh, 12288, block) // sees the buffers below their size: the stall is over
	used.Store(3 * size)
	third := write(16384) // a new stall: closes epoch 2
	waitUntil(t, 10*time.Second, "the next stall to close epoch 2", func() bool { return epochState(t, v, ino).open == 3 })
	used.Store(0)
	wrote(third)
	mustFsync(t, v, ino, fh)

	commits := rec.commitsOf(ino)
	checkEpochOrder(t, commits)
	var got []string
	for _, c := range commits {
		got = append(got, fmt.Sprintf("%s:%d", c.reason, len(c.slices)))
	}
	if want := "buffer:1 buffer:1 flush:1"; strings.Join(got, " ") != want {
		t.Fatalf("epochs %v (reason:slices), want %s", got, want)
	}
	if n := commits[1].slices[0].Slice.Len; n != 3*4096 {
		t.Fatalf("epoch 2 holds %d bytes, want the 3 writes of the stall and after it", n)
	}
	if d := counterValue(t, writerEpochClosed.WithLabelValues("buffer")) - closes; d != 2 {
		t.Fatalf("%v epochs closed by the buffer stall, want 2", d)
	}
}

// (l) An engine without WriteMulti: epochs commit one Write per slice, in creation order.
func TestEpochWithoutWriteMultiCommitsSliceBySlice(t *testing.T) {
	waitWriterIdle(t)
	v, _, rec := createEpochTestVFS(t, "", nil)
	rec.noMulti = true
	ino, fh := newRangeTestFile(t, v)
	sequential, multi := counterValue(t, writerEpochCommits.WithLabelValues("sequential")), counterValue(t, writerEpochCommits.WithLabelValues("multi"))
	var order []string
	for i := uint64(0); i < 6; i++ {
		off := (2-i%3)*meta.ChunkSize + i<<20
		mustWrite(t, v, ino, fh, off, []byte("slice "+strconv.FormatUint(i, 10)))
		order = append(order, fmt.Sprint(off/meta.ChunkSize, off%meta.ChunkSize))
	}
	mustFsync(t, v, ino, fh)
	mustWrite(t, v, ino, fh, 3*meta.ChunkSize, []byte("second epoch"))
	mustFsync(t, v, ino, fh)
	commits := rec.commitsOf(ino)
	if len(commits) != 7 {
		t.Fatalf("%d commits, want 7", len(commits))
	}
	for i, c := range commits {
		wantEpoch := uint64(1)
		if i == 6 {
			wantEpoch = 2
		}
		if c.mode != "sequential" || c.problem != "" || c.epoch != wantEpoch {
			t.Fatalf("commit %d: %+v, want a Write of epoch %d", i, c, wantEpoch)
		}
		if i < 6 {
			if got := fmt.Sprint(c.slices[0].Indx, c.slices[0].Off); got != order[i] {
				t.Fatalf("commit %d is the slice at %s, want %s: not in creation order", i, got, order[i])
			}
		}
	}
	if s, m := counterValue(t, writerEpochCommits.WithLabelValues("sequential"))-sequential, counterValue(t, writerEpochCommits.WithLabelValues("multi"))-multi; s != 2 || m != 0 {
		t.Fatalf("%v sequential and %v multi epoch commits, want 2 and 0", s, m)
	}
	if got := mustRead(t, v, ino, fh, 2*meta.ChunkSize+3<<20, 7); string(got) != "slice 3" {
		t.Fatalf("read %q", got)
	}
}

// A WriteMulti the metadata engine refuses as too large (E2BIG, which the epoch limits should prevent) is committed one
// Write per slice, in creation order, as with an engine without WriteMulti: the epoch commits rather than fail the file.
func TestEpochTooLargeForWriteMultiCommitsSliceBySlice(t *testing.T) {
	waitWriterIdle(t) // no other file's epoch moves the metrics
	v, _, rec := createEpochTestVFS(t, "", nil)
	rec.mu.Lock()
	rec.inject = func(int) (syscall.Errno, bool) { return syscall.E2BIG, false }
	rec.mu.Unlock()
	ino, fh := newRangeTestFile(t, v)
	sequential := counterValue(t, writerEpochCommits.WithLabelValues("sequential"))
	mustWrite(t, v, ino, fh, meta.ChunkSize, []byte("one"))
	mustWrite(t, v, ino, fh, 0, []byte("zero"))
	mustFsync(t, v, ino, fh)
	commits := rec.commitsOf(ino)
	if len(commits) != 2 || commits[0].mode != "sequential" || commits[1].mode != "sequential" || commits[0].epoch != 1 ||
		commits[1].epoch != 1 || commits[0].slices[0].Indx != 1 || commits[1].slices[0].Indx != 0 {
		t.Fatalf("commits %+v, want the two slices of epoch 1 written one by one, in creation order", commits)
	}
	if d := counterValue(t, writerEpochCommits.WithLabelValues("sequential")) - sequential; d != 1 {
		t.Fatalf("%v sequential epoch commits, want 1", d)
	}
	if got := mustRead(t, v, ino, fh, meta.ChunkSize, 3); string(got) != "one" {
		t.Fatalf("read %q", got)
	}
}

// wordStamp is the value a piece writes into each 4-byte word it covers: its index among the pieces written, from 1.
func wordStamp(piece int) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(piece))
	return b[:]
}

// piece is the part of a write in one chunk, word aligned.
type piece struct {
	off, end uint64
}

// wordModel is the content of the test regions after some writes, one stamp per word.
type wordModel struct {
	regions []piece
	words   [][]uint32
	applied int    // pieces applied
	length  uint64 // file length after them
}

func newWordModel(regions []piece) *wordModel {
	m := &wordModel{regions: regions}
	for _, r := range regions {
		m.words = append(m.words, make([]uint32, (r.end-r.off)/4))
	}
	return m
}

func (m *wordModel) apply(p piece) {
	m.applied++
	for i, r := range m.regions {
		for off := max(p.off, r.off); off < min(p.end, r.end); off += 4 {
			m.words[i][(off-r.off)/4] = uint32(m.applied)
		}
	}
	m.length = max(m.length, p.end)
}

// epochCrashCheck compares what a crash would leave of the file with the file after its first writes.
type epochCrashCheck struct {
	v           *VFS
	rec         *recordingMeta
	ino         Ino
	regions     []piece
	reader      DataReader // not the VFS's: it has read nothing yet, and nothing invalidates it
	model       *wordModel // the file after the writes the last snapshot found committed
	modelWrites int

	mu      sync.Mutex
	pieces  []piece // every piece written or being written, in order
	ends    []int   // the pieces of write i are pieces[ends[i-1]:ends[i]]
	durable int     // the committed data holds at least this many writes
	count   int
}

func (c *epochCrashCheck) raiseDurable(n int) {
	c.mu.Lock()
	c.durable = max(c.durable, n)
	c.mu.Unlock()
}

func (c *epochCrashCheck) written() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ends)
}

// snapshot pauses the writer's commits, reads the file as a crash would leave it and checks it: the commits so far are
// the epochs 1..n in order, each with exactly its slices, and the committed data is exactly the file after the first
// writes(n) writes, where writes(n) is the number of writes made before epoch n closed, and no fewer than fsyncs and
// reads made durable. It is not safe for concurrent use.
func (c *epochCrashCheck) snapshot(t *testing.T) error {
	c.mu.Lock()
	lower := c.durable
	c.mu.Unlock()

	c.rec.pause.Lock()
	c.mu.Lock()
	upper := len(c.ends)
	pieces := append([]piece(nil), c.pieces...)
	ends := append([]int(nil), c.ends...)
	c.count++
	c.mu.Unlock()
	commits := c.rec.commitsOf(c.ino)
	orderErr := epochOrderError(commits)
	epochState(t, c.v, c.ino) // checks the writer's invariants
	length := committedLength(commits)
	ctx := meta.Background()
	var readErr error
	got := make([][]byte, len(c.regions))
	fr := c.reader.Open(c.ino, length)
	for i, r := range c.regions {
		got[i] = make([]byte, r.end-r.off)
		for off := uint64(0); off < r.end-r.off && r.off+off < length && readErr == nil; off += 1 << 20 {
			// past the end of the file the buffer stays zero, as a hole
			if _, e := fr.Read(ctx, r.off+off, got[i][off:min(off+1<<20, r.end-r.off)]); e != 0 {
				readErr = fmt.Errorf("fresh read at %d: %s", r.off+off, e)
			}
		}
	}
	fr.Close(ctx)
	c.rec.pause.Unlock()
	if orderErr != nil {
		return orderErr
	}
	if readErr != nil {
		return readErr
	}

	var writes int
	if n := len(commits); n > 0 {
		writes = int(commits[n-1].writes)
	}
	if writes > upper || writes < lower {
		return fmt.Errorf("the last committed epoch closed after %d writes; %d were written and %d must be durable", writes, upper, lower)
	}
	if writes < c.modelWrites {
		return fmt.Errorf("the committed writes went back from %d to %d", c.modelWrites, writes)
	}
	for ; c.modelWrites < writes; c.modelWrites++ {
		start := 0
		if c.modelWrites > 0 {
			start = ends[c.modelWrites-1]
		}
		for _, p := range pieces[start:ends[c.modelWrites]] {
			c.model.apply(p)
		}
	}
	if length != c.model.length {
		return fmt.Errorf("committed length %d, want %d after %d writes", length, c.model.length, writes)
	}
	for i, r := range c.regions {
		for w, want := range c.model.words[i] {
			if stamp := binary.LittleEndian.Uint32(got[i][w*4:]); stamp != want {
				return fmt.Errorf("at %d the committed data has piece %d, want %d: not the file after exactly %d writes (%d commits)",
					r.off+uint64(w)*4, stamp, want, writes, len(commits))
			}
		}
	}
	return nil
}

// (j) Random writes across four chunks, some crossing chunk boundaries, with random upload delays, reads and fsyncs on
// two handles. Whenever commits are paused, the commits so far are the epochs 1..n, in order, each one WriteMulti with
// exactly its slices, and a fresh reader sees exactly the file after the writes made before epoch n closed: not some
// prefix of the history, that one. Set JFS_TEST_META_URL to run it on another metadata engine too.
func TestEpochCrashLeavesTheFileAtAnEpochBoundary(t *testing.T) {
	forEachReadFlush(t, func(t *testing.T) {
		for _, url := range testMetaURLs() {
			for _, seed := range []int64{1, 2, time.Now().UnixNano()} {
				t.Run(fmt.Sprintf("%s/%d", strings.SplitN(url, ":", 2)[0], seed), func(t *testing.T) { testEpochRandom(t, url, seed) })
			}
		}
		// Epochs of at most 3 slices: most boundaries come from the limit, at the start of a write.
		t.Run("memkv/maxslices=3/3", func(t *testing.T) {
			t.Setenv("JFS_EPOCH_MAX_SLICES", "3")
			testEpochRandom(t, "", 3)
		})
	})
	waitWriterIdle(t)
}

func testEpochRandom(t *testing.T, metaURL string, seed int64) {
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	pages := utils.AllocMemory()
	v, store, rec := createEpochTestVFS(t, metaURL, func(c *chunk.Config) { c.MaxUpload = 8; noMemoryCache(c) })
	ino, fh := newRangeTestFile(t, v)
	fh2 := mustOpen(t, v, ino, syscall.O_RDWR)
	handles := []uint64{fh, fh2}
	// Chunk 0 holds slices longer than a 4 MiB block; the others hold writes that cross the chunk boundaries.
	regions := []piece{{0, 10 << 20}}
	for k := uint64(1); k <= 3; k++ {
		regions = append(regions, piece{k*meta.ChunkSize - 1<<20, k*meta.ChunkSize + 1<<20})
	}
	var delayMu sync.Mutex
	delayRng := rand.New(rand.NewSource(seed + 1))
	store.setRule(func(string) putAction {
		delayMu.Lock()
		defer delayMu.Unlock()
		switch r := delayRng.Intn(100); {
		case r < 40:
			return putAction{}
		case r < 95:
			return putAction{delay: time.Duration(delayRng.Intn(4000)) * time.Microsecond}
		default:
			return putAction{delay: time.Duration(20+delayRng.Intn(60)) * time.Millisecond}
		}
	})
	check := &epochCrashCheck{v: v, rec: rec, ino: ino, regions: regions,
		reader: NewDataReader(v.Conf, v.Meta, v.Store), model: newWordModel(regions)}

	done := make(chan struct{})
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
	}
	defer stop()
	wg.Add(1)
	go func() {
		defer wg.Done()
		srng := rand.New(rand.NewSource(seed + 2))
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Duration(1+srng.Intn(25)) * time.Millisecond):
			}
			if err := check.snapshot(t); err != nil {
				t.Errorf("snapshot %d: %s", check.count, err)
				return
			}
		}
	}()

	live := newWordModel(regions)
	ctx := NewLogContext(meta.Background())
	randomIn := func(r piece) uint64 { return r.off + uint64(rng.Intn(int((r.end-r.off)/4)))*4 }
	var last piece
	for op := 0; op < 1200 && !t.Failed(); op++ {
		fh := handles[rng.Intn(len(handles))] // the writer is per file; handles only order their own operations
		switch r := rng.Intn(100); {
		case r < 12: // read a random range and check it against everything written so far
			reg := rng.Intn(len(regions))
			ro := randomIn(regions[reg])
			rl := min(uint64(1+rng.Intn(64<<10/4))*4, regions[reg].end-ro)
			check.mu.Lock()
			newest := 0 // the newest write the read overlaps
			for i, end := range check.ends {
				start := 0
				if i > 0 {
					start = check.ends[i-1]
				}
				for _, p := range check.pieces[start:end] {
					if p.off < ro+rl && ro < p.end {
						newest = i + 1
					}
				}
			}
			check.mu.Unlock()
			buf := make([]byte, rl)
			n, e := v.Read(ctx, ino, buf, ro, fh)
			if e != 0 {
				t.Fatalf("read (%d,%d): %s", ro, rl, e)
			}
			if want := min(rl, live.length-min(live.length, ro)); uint64(n) != want {
				t.Fatalf("read (%d,%d) returned %d bytes, want %d", ro, rl, n, want)
			}
			for w := 0; w < n/4; w++ {
				if stamp, want := binary.LittleEndian.Uint32(buf[w*4:]), live.words[reg][(ro-regions[reg].off)/4+uint64(w)]; stamp != want {
					t.Fatalf("read at %d returned piece %d, want %d", ro+uint64(w)*4, stamp, want)
				}
			}
			if v.readFlushRange {
				// The read committed the epoch of the newest write it overlaps and so, in order, every write before it.
				check.raiseDurable(newest)
			} else {
				check.raiseDurable(check.written()) // the read flushed the whole file, as an fsync
			}
		case r < 15:
			before := check.written()
			mustFsync(t, v, ino, fh)
			check.raiseDurable(before)
		case r < 19:
			time.Sleep(time.Duration(rng.Intn(1500)) * time.Millisecond / 100)
		default: // write: append to the last write, overwrite part of it, or anywhere in a region
			var size uint64
			switch s := rng.Intn(100); {
			case s < 60:
				size = uint64(1+rng.Intn(16<<10/4)) * 4
			case s < 92:
				size = uint64(1+rng.Intn(256<<10/4)) * 4
			default:
				size = uint64(1+rng.Intn(5<<20/2/4)) * 4
			}
			reg := rng.Intn(len(regions))
			off := randomIn(regions[reg])
			switch s := rng.Intn(100); {
			case s < 40 && last.end > 0:
				off = last.end
			case s < 55 && last.end > 0:
				off = randomIn(last)
			}
			reg = -1
			for i := range regions {
				if off >= regions[i].off && off < regions[i].end {
					reg = i
				}
			}
			if reg < 0 {
				reg = rng.Intn(len(regions))
				off = randomIn(regions[reg])
			}
			end := min(off+size, regions[reg].end)
			// A write that crosses a chunk boundary is two pieces, one per chunk, each with its own stamp: a snapshot
			// that holds one of them without the other finds the write split between two epochs.
			var pieces []piece
			for p := off; p < end; {
				pe := min(end, (p/meta.ChunkSize+1)*meta.ChunkSize)
				pieces = append(pieces, piece{p, pe})
				p = pe
			}
			check.mu.Lock()
			first := len(check.pieces) + 1
			check.pieces = append(check.pieces, pieces...)
			check.ends = append(check.ends, len(check.pieces))
			check.mu.Unlock()
			data := make([]byte, 0, end-off)
			for i, p := range pieces {
				stamp := wordStamp(first + i)
				for w := p.off; w < p.end; w += 4 {
					data = append(data, stamp...)
				}
				live.apply(p)
			}
			if e := v.Write(ctx, ino, data, off, fh); e != 0 {
				t.Fatalf("write (%d,%d): %s", off, len(data), e)
			}
			last = piece{off, end}
		}
	}
	stop()
	if t.Failed() {
		return
	}
	all := check.written()
	mustFsync(t, v, ino, fh)
	check.raiseDurable(all)
	if err := check.snapshot(t); err != nil {
		t.Fatalf("after the last fsync: %s", err)
	}
	if check.modelWrites != all {
		t.Fatalf("after the last fsync the committed data holds %d writes of %d", check.modelWrites, all)
	}
	for i := range regions {
		for w, want := range live.words[i] {
			if got := check.model.words[i][w]; got != want {
				t.Fatalf("at %d the committed data has piece %d, the live model %d", regions[i].off+uint64(w)*4, got, want)
			}
		}
	}
	commits := rec.commitsOf(ino)
	reasons := map[string]int{}
	for _, c := range commits {
		reasons[c.reason]++
	}
	t.Logf("%d writes, %d epochs (closed by %v), %d snapshots", all, len(commits), reasons, check.count)
	v.Release(ctx, ino, fh)
	v.Release(ctx, ino, fh2)
	waitWriterIdle(t)
	waitPagesReleased(t, pages)
}

// concurrentPiece is the part of one write in one chunk.
type concurrentPiece struct {
	stamp    uint32
	off, end uint64
}

type concurrentWrite struct {
	writer     int
	stamp      uint32
	pieces     []concurrentPiece
	start, end int64 // since the test started; end is 0 while the write is in flight
}

type concurrentSpan struct{ start, end int64 }

// concurrentLog is what the writers, readers and fsyncs of TestEpochConcurrentHandlesLeaveABoundary did, and when.
type concurrentLog struct {
	mu     sync.Mutex
	writes []*concurrentWrite
	fsyncs []concurrentSpan // fsyncs that returned, and reads when they flush the whole file (JFS_READ_FLUSH_RANGE off)
}

// (k) Concurrent writers, each on its own handle and appending in its own region across a chunk boundary, with readers
// and fsyncs on other handles and random upload delays. Whenever commits are paused, what a crash would leave must be
// an epoch boundary: every write is durable whole or not at all (both chunks of one that crosses a boundary), and the
// durable writes are exactly as many as were made before the last committed epoch closed. It must also agree with the
// order in which the operations happened: a durable write makes durable every write that ended before it started, and
// so does an fsync that returned; each writer's durable writes are a prefix of its writes. Reads see every write that
// ended before they started and nothing of a write that had not started. Set JFS_TEST_META_URL to run it on another
// metadata engine too.
func TestEpochConcurrentHandlesLeaveABoundary(t *testing.T) {
	forEachReadFlush(t, func(t *testing.T) {
		for _, url := range testMetaURLs() {
			for _, seed := range []int64{11, 12, time.Now().UnixNano()} {
				t.Run(fmt.Sprintf("%s/%d", strings.SplitN(url, ":", 2)[0], seed), func(t *testing.T) { testEpochConcurrent(t, url, seed) })
			}
		}
		t.Run("memkv/maxslices=3/13", func(t *testing.T) {
			t.Setenv("JFS_EPOCH_MAX_SLICES", "3")
			testEpochConcurrent(t, "", 13)
		})
	})
	waitWriterIdle(t)
}

func testEpochConcurrent(t *testing.T, metaURL string, seed int64) {
	t.Logf("seed %d", seed)
	const writers, readers = 4, 2
	const regionLen = 8 << 20 // long enough that the writers go on for most of runFor
	const runFor = 3 * time.Second
	t0 := time.Now()
	now := func() int64 { return int64(time.Since(t0)) }
	pages := utils.AllocMemory()
	v, store, rec := createEpochTestVFS(t, metaURL, func(c *chunk.Config) { c.MaxUpload = 8; noMemoryCache(c) })
	ino, fh0 := newRangeTestFile(t, v)
	ctx := NewLogContext(meta.Background())
	regionOff := func(k int) uint64 { return uint64(k+1)*meta.ChunkSize - regionLen/2 } // crosses the end of chunk k
	var delayMu sync.Mutex
	drng := rand.New(rand.NewSource(seed))
	store.setRule(func(string) putAction {
		delayMu.Lock()
		defer delayMu.Unlock()
		switch r := drng.Intn(100); {
		case r < 50:
			return putAction{}
		case r < 97:
			return putAction{delay: time.Duration(drng.Intn(5000)) * time.Microsecond}
		default:
			return putAction{delay: time.Duration(50+drng.Intn(250)) * time.Millisecond}
		}
	})
	var writerFhs, readerFhs []uint64
	for k := 0; k < writers; k++ {
		writerFhs = append(writerFhs, mustOpen(t, v, ino, syscall.O_RDWR))
	}
	for r := 0; r < readers; r++ {
		readerFhs = append(readerFhs, mustOpen(t, v, ino, syscall.O_RDONLY))
	}
	fsyncFh := mustOpen(t, v, ino, syscall.O_RDWR)

	var lg concurrentLog
	var stamp atomic.Uint32
	done := make(chan struct{})
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
	}
	defer stop()
	stopped := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	for k := 0; k < writers; k++ {
		wg.Add(1)
		go func(k int, fh uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*31 + int64(k)))
			pos, end := regionOff(k), regionOff(k)+regionLen
			for pos < end && !stopped() {
				size := uint64(1+rng.Intn(64<<10/4)) * 4
				if rng.Intn(10) == 0 {
					size = uint64(1+rng.Intn(512<<10/4)) * 4
				}
				we := min(end, pos+size)
				w := &concurrentWrite{writer: k, stamp: stamp.Add(1)}
				for p := pos; p < we; {
					pe := min(we, (p/meta.ChunkSize+1)*meta.ChunkSize)
					w.pieces = append(w.pieces, concurrentPiece{w.stamp, p, pe})
					p = pe
				}
				data := make([]byte, 0, we-pos)
				for p := pos; p < we; p += 4 {
					data = append(data, wordStamp(int(w.stamp))...)
				}
				lg.mu.Lock()
				w.start = now()
				lg.writes = append(lg.writes, w)
				lg.mu.Unlock()
				if e := v.Write(ctx, ino, data, pos, fh); e != 0 {
					t.Errorf("write (%d,%d): %s", pos, len(data), e)
					return
				}
				lg.mu.Lock()
				w.end = now()
				lg.mu.Unlock()
				pos = we
				// About 12 ms between writes, so a writer takes about runFor to fill its region.
				time.Sleep(time.Duration(rng.Intn(25000)) * time.Microsecond)
			}
		}(k, writerFhs[k])
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed + 7))
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Duration(5+rng.Intn(60)) * time.Millisecond):
			}
			s := now()
			if e := v.Fsync(ctx, ino, 0, fsyncFh); e != 0 {
				t.Errorf("fsync: %s", e)
				return
			}
			lg.mu.Lock()
			lg.fsyncs = append(lg.fsyncs, concurrentSpan{s, now()})
			lg.mu.Unlock()
		}
	}()
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int, fh uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*17 + int64(r)))
			for !stopped() {
				k := rng.Intn(writers)
				ro := regionOff(k) + uint64(rng.Intn(regionLen/4))*4
				rl := min(uint64(1+rng.Intn(128<<10/4))*4, regionOff(k)+regionLen-ro)
				buf := make([]byte, rl)
				rs := now()
				lg.mu.Lock()
				var before []*concurrentWrite
				for _, w := range lg.writes {
					if w.end != 0 && w.end < rs {
						before = append(before, w)
					}
				}
				lg.mu.Unlock()
				n, e := v.Read(ctx, ino, buf, ro, fh)
				re := now()
				if e != 0 {
					t.Errorf("read (%d,%d): %s", ro, rl, e)
					return
				}
				if !v.readFlushRange { // the read flushed the whole file: it makes durable what an fsync does
					lg.mu.Lock()
					lg.fsyncs = append(lg.fsyncs, concurrentSpan{rs, re})
					lg.mu.Unlock()
				}
				buf = buf[:n]
				started := map[uint32]bool{}
				lg.mu.Lock()
				for _, w := range lg.writes {
					if w.start < re {
						started[w.stamp] = true
					}
				}
				lg.mu.Unlock()
				for i := 0; i+4 <= len(buf); i += 4 {
					if s := binary.LittleEndian.Uint32(buf[i:]); s != 0 && !started[s] {
						t.Errorf("read at %d saw write %d, which had not started", ro+uint64(i), s)
						return
					}
				}
				for _, w := range before {
					for _, p := range w.pieces {
						for o := max(p.off, ro); o < min(p.end, ro+rl); o += 4 {
							if o+4 > ro+uint64(len(buf)) {
								t.Errorf("read (%d,%d) returned %d bytes, but write %d ended before it started", ro, rl, len(buf), w.stamp)
								return
							}
							if s := binary.LittleEndian.Uint32(buf[o-ro:]); s != w.stamp {
								t.Errorf("read at %d saw write %d, want %d, which ended before the read started", o, s, w.stamp)
								return
							}
						}
					}
				}
			}
		}(r, readerFhs[r])
	}

	snapshots := 0
	fresh := NewDataReader(v.Conf, v.Meta, v.Store) // reads nothing but the snapshots: nothing to invalidate
	snap := func(final bool) error {
		rec.pause.Lock()
		defer rec.pause.Unlock()
		lg.mu.Lock()
		writes := make([]concurrentWrite, len(lg.writes))
		for i, w := range lg.writes {
			writes[i] = *w
		}
		fsyncs := append([]concurrentSpan(nil), lg.fsyncs...)
		lg.mu.Unlock()
		commits := rec.commitsOf(ino)
		if err := epochOrderError(commits); err != nil {
			return err
		}
		epochState(t, v, ino) // checks the writer's invariants
		length := committedLength(commits)
		fr := fresh.Open(ino, length)
		defer fr.Close(meta.Background())
		written := make([]uint64, writers) // how far into its region each writer has written
		for _, w := range writes {
			written[w.writer] = max(written[w.writer], w.pieces[len(w.pieces)-1].end-regionOff(w.writer))
		}
		got := make([][]byte, writers)
		for k := 0; k < writers; k++ {
			got[k] = make([]byte, regionLen)
			for off := uint64(0); off < written[k] && regionOff(k)+off < length; off += 1 << 20 {
				if _, e := fr.Read(meta.Background(), regionOff(k)+off, got[k][off:min(off+1<<20, regionLen)]); e != 0 {
					return fmt.Errorf("fresh read at %d: %s", regionOff(k)+off, e)
				}
			}
		}
		snapshots++
		durable := map[uint32]bool{}
		for _, w := range writes {
			has, all := false, true
			for _, p := range w.pieces {
				for o := p.off; o < p.end; o += 4 {
					if binary.LittleEndian.Uint32(got[w.writer][o-regionOff(w.writer):]) == p.stamp {
						has = true
					} else {
						all = false
					}
				}
			}
			if has && !all {
				return fmt.Errorf("write %d (%d pieces) is durable in part", w.stamp, len(w.pieces))
			}
			durable[w.stamp] = has
		}
		var committed uint64 // writes made before the last committed epoch closed
		if n := len(commits); n > 0 {
			committed = commits[n-1].writes
		}
		n := 0
		for _, d := range durable {
			if d {
				n++
			}
		}
		if uint64(n) != committed {
			return fmt.Errorf("%d writes are durable, but the last committed epoch (%d) closed after %d", n, len(commits), committed)
		}
		var latestStart int64 = -1 // the latest start of a durable write
		var latestStamp uint32
		lastDurable := map[int]uint32{}
		for _, w := range writes {
			if durable[w.stamp] {
				if w.start > latestStart {
					latestStart, latestStamp = w.start, w.stamp
				}
				lastDurable[w.writer] = max(lastDurable[w.writer], w.stamp)
			}
		}
		var fsyncStart int64 = -1 // the latest start of a returned fsync
		for _, fs := range fsyncs {
			fsyncStart = max(fsyncStart, fs.start)
		}
		for _, w := range writes {
			var need string
			switch {
			case final:
				need = "the last fsync returned"
			case w.end != 0 && w.end < latestStart:
				need = fmt.Sprintf("write %d, which started after it ended, is", latestStamp)
			case w.end != 0 && w.end < fsyncStart:
				need = "an fsync that started after it ended returned"
			case w.stamp < lastDurable[w.writer]:
				need = fmt.Sprintf("a later write of the same writer (%d) is", lastDurable[w.writer])
			default:
				continue
			}
			if !durable[w.stamp] {
				return fmt.Errorf("write %d (writer %d) is not durable, but %s durable", w.stamp, w.writer, need)
			}
		}
		return nil
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		srng := rand.New(rand.NewSource(seed + 3))
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Duration(1+srng.Intn(20)) * time.Millisecond):
			}
			if err := snap(false); err != nil {
				t.Errorf("snapshot %d: %s", snapshots, err)
				return
			}
		}
	}()
	for deadline := time.Now().Add(runFor); time.Now().Before(deadline) && !t.Failed(); {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if t.Failed() {
		return
	}
	mustFsync(t, v, ino, fsyncFh)
	if err := snap(true); err != nil {
		t.Fatalf("after the last fsync: %s", err)
	}
	lg.mu.Lock()
	t.Logf("%d writes, %d fsyncs, %d epochs, %d snapshots", len(lg.writes), len(lg.fsyncs), len(rec.commitsOf(ino)), snapshots)
	lg.mu.Unlock()
	for _, fh := range append(append(append([]uint64{fh0}, writerFhs...), readerFhs...), fsyncFh) {
		v.Release(ctx, ino, fh)
	}
	waitWriterIdle(t)
	waitPagesReleased(t, pages)
}
