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

package vfs

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// compactHistogram returns the count and the sum of compact_size_histogram_bytes.
func compactHistogram(t *testing.T) (uint64, float64) {
	var d dto.Metric
	if err := compactSizeHistogram.Write(&d); err != nil {
		t.Fatalf("read compact_size_histogram_bytes: %s", err)
	}
	return d.GetHistogram().GetSampleCount(), d.GetHistogram().GetSampleSum()
}

func TestCompact(t *testing.T) {
	cconf := chunk.Config{
		BlockSize:   256 * 1024,
		Compress:    "lz4",
		MaxUpload:   2,
		MaxDownload: 200,
		BufferSize:  30 << 20,
		CacheSize:   10 << 20,
		CacheDir:    "memory",
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	store := chunk.NewCachedStore(blob, cconf, nil)

	// prepare the slices
	var slices []meta.Slice
	var total int
	for i := 0; i < 100; i++ {
		buf := make([]byte, 100+i*100)
		for j := range buf {
			buf[j] = byte(i)
		}
		cid := uint64(i)
		w := store.NewWriter(cid, 0)
		if n, e := w.WriteAt(buf, 0); e != nil {
			t.Fatalf("write chunk %d: %s", cid, e)
		} else {
			total += n
		}
		if e := w.Finish(len(buf)); e != nil {
			t.Fatalf("flush chunk %d: %s", cid, e)
		}
		slices = append(slices, meta.Slice{Id: cid, Size: uint32(len(buf)), Len: uint32(len(buf))})
	}

	// compact
	var cid uint64 = 1000
	count, sum := compactHistogram(t)
	err := Compact(cconf, store, slices, cid, 0)
	if err != nil {
		t.Fatalf("compact %d slices : %s", len(slices), err)
	}
	if c, s := compactHistogram(t); c != count+1 || s != sum+float64(total) {
		t.Fatalf("compact_size_histogram_bytes went from %d (sum %v) to %d (sum %v), want one more of %d bytes", count, sum, c, s, total)
	}

	// verify result
	r := store.NewReader(cid, total)
	var off int
	for i := 0; i < 100; i++ {
		buf := make([]byte, 100+i*100)
		page := chunk.NewPage(buf)
		n, err := r.ReadAt(context.Background(), page, off)
		if err != nil {
			t.Fatalf("read chunk %d at %d: %s", cid, off, err)
		} else if n != len(buf) {
			t.Fatalf("short read: %d", n)
		}
		for j := range buf {
			if buf[j] != byte(i) {
				t.Fatalf("invalid byte at %d: %d !=%d", j, buf[j], i)
			}
		}
		off += len(buf)
		defer page.Release()
	}

	// failed
	_ = store.Remove(1, 200)
	err = Compact(cconf, store, slices, cid, 0)
	if err == nil {
		t.Fatalf("compact should fail with read but got nil")
	}
	if c, s := compactHistogram(t); c != count+2 || s != sum+2*float64(total) {
		t.Fatalf("a failed compaction is not in compact_size_histogram_bytes: %d (sum %v), want %d (sum %v)", c, s, count+2, sum+2*float64(total))
	}

	// TODO: inject write failure
}

// compactCounters returns the values of the counters of compactions by trigger in reg, by name and trigger.
func compactCounters(t *testing.T, reg *prometheus.Registry) map[string]map[string]float64 {
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %s", err)
	}
	values := make(map[string]map[string]float64)
	for _, mf := range mfs {
		if name := mf.GetName(); name == "juicefs_compactions_total" || name == "juicefs_compacted_bytes_total" {
			values[name] = make(map[string]float64)
			for _, mt := range mf.GetMetric() {
				for _, l := range mt.GetLabel() {
					if l.GetName() == "trigger" {
						values[name][l.GetValue()] = mt.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return values
}

// TestCompactTriggerMetrics checks that the counters of compactions by trigger, which the metadata client keeps,
// agree with compact_size_histogram_bytes, which the data compaction observes, for a compaction of real data.
func TestCompactTriggerMetrics(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	reg := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_", reg)
	v.Meta.InitMetrics(registerer)
	InitMetrics(registerer)
	v.Meta.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return v.Store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	v.Meta.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return Compact(*v.Conf.Chunk, v.Store, args[0].([]meta.Slice), args[1].(uint64), args[2].(uint8))
	})

	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	fe, fh, e := v.Create(ctx, 1, "compacted", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	defer v.Release(ctx, fe.Inode, fh)
	want := make([]byte, 3<<12)
	for i := 0; i < 3; i++ { // one slice each
		buf := want[i<<12 : (i+1)<<12]
		for j := range buf {
			buf[j] = byte(i + 1)
		}
		if e = v.Write(ctx, fe.Inode, buf, uint64(i)<<12, fh); e != 0 {
			t.Fatalf("write %d: %s", i, e)
		}
		if e = v.Flush(ctx, fe.Inode, fh, 0); e != 0 {
			t.Fatalf("flush %d: %s", i, e)
		}
	}
	var ss []meta.Slice
	if e = v.Meta.Read(meta.Background(), fe.Inode, 0, &ss); e != 0 || len(ss) != 3 {
		t.Fatalf("chunk 0 holds %d slices (%s), want 3", len(ss), e)
	}

	count, sum := compactHistogram(t)
	before := compactCounters(t, reg)
	if e = v.Meta.Compact(meta.Background(), fe.Inode, 1, func() {}, func() {}); e != 0 {
		t.Fatalf("compact: %s", e)
	}
	c, s := compactHistogram(t)
	if c != count+1 || s != sum+float64(len(want)) {
		t.Fatalf("compact_size_histogram_bytes went from %d (sum %v) to %d (sum %v), want one more of %d bytes", count, sum, c, s, len(want))
	}
	after := compactCounters(t, reg)
	for name, rose := range map[string]float64{"juicefs_compactions_total": float64(c - count), "juicefs_compacted_bytes_total": s - sum} {
		if len(after[name]) == 0 {
			t.Fatalf("%s is not exported", name)
		}
		for trigger, value := range after[name] {
			expect := before[name][trigger]
			if trigger == "compact" {
				expect += rose
			}
			if value != expect {
				t.Errorf("%s{trigger=%q} = %v, want %v", name, trigger, value, expect)
			}
		}
	}

	if e = v.Meta.Read(meta.Background(), fe.Inode, 0, &ss); e != 0 || len(ss) != 1 {
		t.Fatalf("chunk 0 holds %d slices (%s) after the compaction, want 1", len(ss), e)
	}
	got := make([]byte, len(want))
	if n, e := v.Read(ctx, fe.Inode, got, 0, fh); e != 0 || n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("read %d bytes (%s) after the compaction, want the %d written", n, e, len(want))
	}
}
