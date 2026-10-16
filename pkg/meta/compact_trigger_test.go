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

package meta

import (
	"os"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestCompactTriggersMemKV(t *testing.T) {
	_ = os.Remove(settingPath)
	m, err := newKVMeta("memkv", "jfs-compact-trigger-test", testConfig())
	if err != nil || m.Name() != "memkv" {
		t.Fatalf("create meta: %s", err)
	}
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	testCompactTriggers(t, m)
}

func TestCompactTriggersSQLite(t *testing.T) {
	m, err := newSQLMeta("sqlite3", path.Join(t.TempDir(), "jfs-compact-trigger.db"), testConfig())
	if err != nil || m.Name() != "sqlite3" {
		t.Fatalf("create meta: %s", err)
	}
	t.Cleanup(func() {
		if err := m.Shutdown(); err != nil {
			t.Errorf("shutdown: %s", err)
		}
	})
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	testCompactTriggers(t, m)
}

// TestCompactTriggerMetricsExported checks the names and type of the compaction counters, and that InitMetrics
// exports every trigger at zero before its first compaction.
func TestCompactTriggerMetricsExported(t *testing.T) {
	m := newBaseMeta("", testConfig())
	reg := prometheus.NewPedanticRegistry()
	m.InitMetrics(prometheus.WrapRegistererWithPrefix("juicefs_", reg))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %s", err)
	}
	for _, name := range []string{"juicefs_compactions_total", "juicefs_compacted_bytes_total"} {
		i := slices.IndexFunc(mfs, func(mf *dto.MetricFamily) bool { return mf.GetName() == name })
		if i < 0 {
			t.Fatalf("%s is not exported", name)
		}
		if mfs[i].GetType() != dto.MetricType_COUNTER {
			t.Fatalf("%s is a %s, want a counter", name, mfs[i].GetType())
		}
		var triggers []string
		for _, mt := range mfs[i].GetMetric() {
			if len(mt.GetLabel()) != 1 || mt.GetLabel()[0].GetName() != "trigger" {
				t.Fatalf("%s has labels %v, want only trigger", name, mt.GetLabel())
			}
			if v := mt.GetCounter().GetValue(); v != 0 {
				t.Fatalf("%s{trigger=%q} = %v before any compaction", name, mt.GetLabel()[0].GetValue(), v)
			}
			triggers = append(triggers, mt.GetLabel()[0].GetValue())
		}
		want := slices.Clone(compactTriggers)
		slices.Sort(want)
		slices.Sort(triggers)
		if !slices.Equal(triggers, want) {
			t.Fatalf("%s exports triggers %v, want %v", name, triggers, want)
		}
	}
}

// ctRecorder is a CompactChunk handler that writes no data, and counts the compactions it is given and their size,
// summed as the data compaction sums it for compact_size_histogram_bytes. With fail set it then fails them, as a
// compaction whose data could not be written.
type ctRecorder struct {
	sync.Mutex
	calls int
	bytes float64
	fail  bool
}

func (r *ctRecorder) handle(args ...interface{}) error {
	var size uint32
	for _, s := range args[0].([]Slice) {
		size += s.Len
	}
	r.Lock()
	defer r.Unlock()
	r.calls++
	r.bytes += float64(size)
	if r.fail {
		return errWMCompactionFails
	}
	return nil
}

func (r *ctRecorder) get() (int, float64) {
	r.Lock()
	defer r.Unlock()
	return r.calls, r.bytes
}

// ctFill puts n slices of 4 KiB on chunk indx of inode through the engine, which, unlike Write and WriteMulti, starts
// no compaction.
func ctFill(t *testing.T, m Meta, inode Ino, indx uint32, n int) {
	t.Helper()
	for done := 0; done < n; {
		k := min(n-done, maxCompactSlices-1)
		writes := make([]SliceWrite, k)
		for i := range writes {
			writes[i] = SliceWrite{indx, uint32((done+i)%1024) << 12, wmSlice(t, m, 4<<10)}
		}
		var delta dirStat
		var attr Attr
		if st := m.getBase().en.doWriteMulti(Background(), inode, writes, time.Now(), time.Time{}, make(map[uint32]int), &delta, &attr); st != 0 {
			t.Fatalf("fill chunk %d of inode %d: %s", indx, inode, st)
		}
		done += k
	}
	if ss, st := m.getBase().en.doRead(Background(), inode, indx); st != 0 || len(ss) != n {
		t.Fatalf("chunk %d of inode %d holds %d slices (%s), want %d", indx, inode, len(ss), st, n)
	}
}

// ctCheck runs op and checks that the compactions it made are all counted under trigger, as many and as large as
// the CompactChunk handler was given: want of them, or (want < 0) at least one. A background compaction has
// until a deadline to reach the handler; with background false they must all be done when op returns.
func ctCheck(t *testing.T, m Meta, rec *ctRecorder, trigger string, want int, background bool, op func()) {
	t.Helper()
	base := m.getBase()
	counts, sizes := wmCounters(t, base.compactCount), wmCounters(t, base.compactBytes)
	calls, bytes := rec.get()
	op()
	if background {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if n, _ := rec.get(); n-calls >= max(want, 1) {
				break
			}
		}
		wmWaitBackground(t, m)
	}
	n, b := rec.get()
	n, b = n-calls, b-bytes
	if want >= 0 && n != want || want < 0 && n < 1 {
		t.Fatalf("the handler was given %d compactions, want %d", n, want)
	}
	if b <= 0 {
		t.Fatalf("the handler was given %v bytes to compact", b)
	}
	gotCounts, gotSizes := wmCounters(t, base.compactCount), wmCounters(t, base.compactBytes)
	for tr := range gotCounts {
		if !slices.Contains(compactTriggers, tr) {
			t.Errorf("compactions_total has an unknown trigger %q", tr)
		}
	}
	for _, tr := range compactTriggers {
		var wantN, wantB float64
		if tr == trigger {
			wantN, wantB = float64(n), b
		}
		if d := gotCounts[tr] - counts[tr]; d != wantN {
			t.Errorf("compactions_total{trigger=%q} rose by %v, want %v", tr, d, wantN)
		}
		if d := gotSizes[tr] - sizes[tr]; d != wantB {
			t.Errorf("compacted_bytes_total{trigger=%q} rose by %v, want %v", tr, d, wantB)
		}
	}
}

// testCompactTriggers starts each kind of compaction and checks that it is counted under its trigger.
func testCompactTriggers(t *testing.T, m Meta) {
	if err := m.Init(testFormat(), false); err != nil {
		t.Fatalf("init: %s", err)
	}
	if err := m.NewSession(false); err != nil {
		t.Fatalf("new session: %s", err)
	}
	defer func() {
		wmWaitBackground(t, m)
		if err := m.CloseSession(); err != nil {
			t.Errorf("close session: %s", err)
		}
		wmWaitBackground(t, m)
	}()
	rec := &ctRecorder{}
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	m.OnMsg(CompactChunk, rec.handle)
	ctx := Background()
	interval := m.getBase().writeCompactionInterval
	write := func(t *testing.T, inode Ino, n int) {
		s := wmSlice(t, m, 4<<10)
		if st := m.Write(ctx, inode, 0, uint32(n%1024)<<12, s, time.Now()); st != 0 {
			t.Fatalf("write: %s", st)
		}
	}
	writeMulti := func(t *testing.T, inode Ino, n int) {
		batch := []SliceWrite{{0, uint32(n%1024) << 12, wmSlice(t, m, 4<<10)}, {0, uint32((n+1)%1024) << 12, wmSlice(t, m, 4<<10)}}
		if st := m.WriteMulti(ctx, inode, batch, time.Now()); st != 0 {
			t.Fatalf("WriteMulti: %s", st)
		}
	}

	t.Run("write_interval", func(t *testing.T) {
		inode := wmCreate(t, m, RootInode, "ct_write_interval")
		ctFill(t, m, inode, 0, interval-2)
		ctCheck(t, m, rec, compactTriggerWriteInterval, 1, true, func() { write(t, inode, interval-2) })
	})
	t.Run("write_backlog", func(t *testing.T) {
		inode := wmCreate(t, m, RootInode, "ct_write_backlog")
		ctFill(t, m, inode, 0, writeCompactionDebtThreshold)
		ctCheck(t, m, rec, compactTriggerWriteBacklog, 1, true, func() { write(t, inode, writeCompactionDebtThreshold) })
	})
	t.Run("write_sync", func(t *testing.T) {
		inode := wmCreate(t, m, RootInode, "ct_write_sync")
		ctFill(t, m, inode, 0, maxSlices-1)
		ctCheck(t, m, rec, compactTriggerWriteSync, 1, false, func() { write(t, inode, maxSlices-1) })
	})
	if m.WriteMultiLimits().Slices > 0 {
		t.Run("writemulti_interval", func(t *testing.T) {
			inode := wmCreate(t, m, RootInode, "ct_writemulti_interval")
			ctFill(t, m, inode, 0, interval-2)
			ctCheck(t, m, rec, compactTriggerWriteInterval, 1, true, func() { writeMulti(t, inode, interval-2) })
		})
		t.Run("writemulti_backlog", func(t *testing.T) {
			inode := wmCreate(t, m, RootInode, "ct_writemulti_backlog")
			ctFill(t, m, inode, 0, writeCompactionDebtThreshold)
			ctCheck(t, m, rec, compactTriggerWriteBacklog, 1, true, func() { writeMulti(t, inode, writeCompactionDebtThreshold) })
		})
		t.Run("writemulti_sync", func(t *testing.T) {
			inode := wmCreate(t, m, RootInode, "ct_writemulti_sync")
			ctFill(t, m, inode, 0, maxSlices-1)
			ctCheck(t, m, rec, compactTriggerWriteSync, 1, false, func() { writeMulti(t, inode, maxSlices-1) })
		})
	}
	t.Run("read", func(t *testing.T) {
		inode := wmCreate(t, m, RootInode, "ct_read")
		ctFill(t, m, inode, 0, 5)
		ctCheck(t, m, rec, compactTriggerRead, 1, true, func() {
			var ss []Slice
			if st := m.Read(ctx, inode, 0, &ss); st != 0 {
				t.Fatalf("read: %s", st)
			}
		})
	})
	t.Run("compact", func(t *testing.T) {
		// One compaction takes maxCompactSlices slices: Compact compacts the chunk again, under the same trigger,
		// for the 10 slices after them and the one that the first compaction made.
		inode := wmCreate(t, m, RootInode, "ct_compact")
		ctFill(t, m, inode, 0, maxCompactSlices+10)
		ctCheck(t, m, rec, compactTriggerCompact, 2, false, func() {
			if st := m.Compact(ctx, inode, 1, func() {}, func() {}); st != 0 {
				t.Fatalf("compact: %s", st)
			}
		})
	})
	t.Run("failed", func(t *testing.T) {
		// A compaction whose data could not be written is counted too, as compact_size_histogram_bytes counts it.
		inode := wmCreate(t, m, RootInode, "ct_failed")
		ctFill(t, m, inode, 0, 3)
		rec.Lock()
		rec.fail = true
		rec.Unlock()
		defer func() {
			rec.Lock()
			rec.fail = false
			rec.Unlock()
		}()
		ctCheck(t, m, rec, compactTriggerCompact, 1, false, func() {
			if st := m.Compact(ctx, inode, 1, func() {}, func() {}); st != 0 {
				t.Fatalf("compact: %s", st)
			}
		})
		if ss, st := m.getBase().en.doRead(ctx, inode, 0); st != 0 || len(ss) != 3 {
			t.Fatalf("chunk 0 holds %d slices (%s) after a failed compaction, want 3", len(ss), st)
		}
	})
	t.Run("gc", func(t *testing.T) {
		inode := wmCreate(t, m, RootInode, "ct_gc")
		ctFill(t, m, inode, 0, 3)
		// CompactAll compacts every chunk of the volume: the chunks of the tests before may be there too.
		ctCheck(t, m, rec, compactTriggerGC, -1, false, func() {
			p, bar := utils.MockProgress()
			defer p.Done()
			if st := m.CompactAll(ctx, 1, bar); st != 0 {
				t.Fatalf("compact all: %s", st)
			}
		})
		if ss, st := m.getBase().en.doRead(ctx, inode, 0); st != 0 || len(ss) != 1 {
			t.Fatalf("chunk 0 holds %d slices (%s) after CompactAll, want 1", len(ss), st)
		}
	})
}
