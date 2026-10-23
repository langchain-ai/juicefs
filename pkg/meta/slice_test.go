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
	"math/rand"
	"reflect"
	"testing"
)

const testNewID = 1 << 40

// piece is one visible range of a chunk: where its bytes come from.
type piece struct {
	pos, len uint32
	id       uint64
	off      uint32
}

// visible resolves a chunk's entries to the source of every byte, mapping
// ranges of the compaction output slice back through data. Holes have id 0.
func visible(ss []*slice, data []Slice) []piece {
	var out []piece
	add := func(p piece) {
		if n := len(out); n > 0 {
			l := &out[n-1]
			if l.id == p.id && l.pos+l.len == p.pos && (p.id == 0 || l.off+l.len == p.off) {
				l.len += p.len
				return
			}
		}
		if p.id == 0 {
			p.off = 0
		}
		out = append(out, p)
	}
	var pos uint32
	for _, s := range buildSlice(ss) {
		if s.Id != testNewID {
			add(piece{pos, s.Len, s.Id, s.Off})
			pos += s.Len
			continue
		}
		// Walk data from s.Off for s.Len bytes.
		var doff uint32
		for _, d := range data {
			start, stop := max(doff, s.Off), min(doff+d.Len, s.Off+s.Len)
			if start < stop {
				src := piece{pos + start - s.Off, stop - start, d.Id, d.Off + start - doff}
				add(src)
			}
			doff += d.Len
		}
		pos += s.Len
	}
	// A hole at the end reads the same as no data at all.
	for len(out) > 0 && out[len(out)-1].id == 0 {
		out = out[:len(out)-1]
	}
	return out
}

// apply returns ss with plan applied: ss[0], then one entry per extent.
func apply(ss []*slice, plan keepPlan) []*slice {
	var size uint32
	for _, e := range plan.extents {
		size += e[1]
	}
	out := []*slice{ss[0]}
	var off uint32
	for _, e := range plan.extents {
		out = append(out, newSlice(e[0], testNewID, size, off, e[1]))
		off += e[1]
	}
	return out
}

func checkKeepPlan(t *testing.T, ss []*slice) keepPlan {
	t.Helper()
	plan, ok := planKeepCompaction(ss)
	if !ok {
		t.Fatalf("no keep plan for %d slices", len(ss))
	}
	if len(plan.extents) == 0 {
		return plan
	}
	if want, got := visible(ss, nil), visible(apply(ss, plan), plan.data); !reflect.DeepEqual(want, got) {
		t.Fatalf("compaction changed the chunk's content:\nbefore %+v\nafter  %+v", want, got)
	}
	var dataLen, extLen uint32
	for _, d := range plan.data {
		dataLen += d.Len
	}
	for _, e := range plan.extents {
		extLen += e[1]
	}
	if dataLen != extLen {
		t.Fatalf("copies %d bytes for %d bytes of extents", dataLen, extLen)
	}
	return plan
}

func base64M() *slice { return newSlice(0, 1, ChunkSize, 0, ChunkSize) }

func TestPlanKeepCompactionScattered(t *testing.T) {
	ss := []*slice{base64M()}
	for i, pos := range []uint32{30 << 20, 10 << 20, 50 << 20, 10<<20 + 4096, 60 << 20} {
		ss = append(ss, newSlice(pos, uint64(10+i), 8192, 0, 8192))
	}
	plan := checkKeepPlan(t, ss)
	// The two writes at 10 MiB overlap, so four ranges.
	if len(plan.extents) != 4 {
		t.Fatalf("want 4 extents, got %v", plan.extents)
	}
	if plan.extents[0] != [2]uint32{10 << 20, 4096 + 8192} {
		t.Fatalf("overlapping writes not merged: %v", plan.extents[0])
	}
}

func TestPlanKeepCompactionMergesNearbyRanges(t *testing.T) {
	ss := []*slice{base64M(),
		newSlice(1<<20, 10, 4096, 0, 4096),
		newSlice(1<<20+64<<10, 11, 4096, 0, 4096), // 60 KiB after the first: merged, copying the gap
		newSlice(8<<20, 12, 4096, 0, 4096),        // far away: its own range
	}
	plan := checkKeepPlan(t, ss)
	want := [][2]uint32{{1 << 20, 64<<10 + 4096}, {8 << 20, 4096}}
	if !reflect.DeepEqual(plan.extents, want) {
		t.Fatalf("want %v, got %v", want, plan.extents)
	}
}

func TestPlanKeepCompactionHoles(t *testing.T) {
	ss := []*slice{
		newSlice(0, 1, 32<<20, 0, 32<<20),       // base covers half the chunk
		newSlice(4<<20, 0, 0, 0, 1<<20),         // punched hole over the base
		newSlice(40<<20, 10, 8192, 0, 8192),     // write past the base's end
		newSlice(20<<20, 11, 1<<20, 4096, 8192), // write from the middle of a slice
	}
	plan := checkKeepPlan(t, ss)
	if len(plan.extents) != 3 {
		t.Fatalf("want 3 extents, got %v", plan.extents)
	}
	if plan.data[0].Id != 0 || plan.data[0].Len != 1<<20 {
		t.Fatalf("hole over the base not rewritten as zeros: %+v", plan.data[0])
	}
}

func TestPlanKeepCompactionRepeated(t *testing.T) {
	ss := []*slice{base64M()}
	rng := rand.New(rand.NewSource(1))
	var next uint64 = 10
	for round := 0; round < 20; round++ {
		for i := 0; i < 4; i++ {
			pos := uint32(rng.Intn(ChunkSize/4096-4)) * 4096
			ss = append(ss, newSlice(pos, next, 16384, 0, 16384))
			next++
		}
		plan := checkKeepPlan(t, ss)
		// Commit it, with the new slice under a fresh id as the engines would.
		var size uint32
		for _, e := range plan.extents {
			size += e[1]
		}
		committed := []*slice{ss[0]}
		var off uint32
		for _, e := range plan.extents {
			committed = append(committed, newSlice(e[0], next, size, off, e[1]))
			off += e[1]
		}
		next++
		ss = committed
		if again, ok := planKeepCompaction(ss); !ok || len(again.extents) != 0 {
			t.Fatalf("round %d: a compacted chunk was planned again: %v", round, again.extents)
		}
	}
}

func TestPlanKeepCompactionRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for iter := 0; iter < 2000; iter++ {
		ss := []*slice{newSlice(uint32(rng.Intn(4))<<20, 1, ChunkSize, 0, ChunkSize-4<<20)}
		for i := 0; i < 2+rng.Intn(30); i++ {
			l := uint32(1+rng.Intn(64)) * 4096
			pos := uint32(rng.Intn(int(ChunkSize-l)/4096)) * 4096
			id := uint64(10 + i)
			if rng.Intn(10) == 0 {
				id = 0 // a punched hole
			}
			off := uint32(rng.Intn(4)) * 4096
			ss = append(ss, newSlice(pos, id, l+off, off, l))
		}
		if _, ok := planKeepCompaction(ss); ok {
			checkKeepPlan(t, ss)
		}
	}
}

func TestPlanKeepCompactionFallsBack(t *testing.T) {
	small := []*slice{newSlice(0, 1, 512<<10, 0, 512<<10), newSlice(0, 2, 4096, 0, 4096), newSlice(8192, 3, 4096, 0, 4096)}
	if _, ok := planKeepCompaction(small); ok {
		t.Fatal("kept a slice smaller than keepMinLen")
	}
	mostly := []*slice{base64M(), newSlice(0, 2, 40<<20, 0, 40<<20), newSlice(50<<20, 3, 4096, 0, 4096)}
	if _, ok := planKeepCompaction(mostly); ok {
		t.Fatal("kept a slice that is mostly overwritten")
	}
	many := []*slice{base64M()}
	for i := 0; i <= keepMaxExtents; i++ {
		many = append(many, newSlice(uint32(i)*(keepMergeGap+8192), uint64(10+i), 4096, 0, 4096))
	}
	if _, ok := planKeepCompaction(many); ok {
		t.Fatal("kept a slice with too many changed ranges")
	}
}

func TestPlanKeepCompactionNothingToDo(t *testing.T) {
	for _, ss := range [][]*slice{
		{base64M()},
		{base64M(), newSlice(4096, 2, 4096, 0, 4096)},
		{base64M(), newSlice(4096, 2, 8192, 0, 4096), newSlice(1<<20, 2, 8192, 4096, 4096)},
	} {
		if plan, ok := planKeepCompaction(ss); !ok || len(plan.extents) != 0 {
			t.Fatalf("planned work for %d slices: ok=%v %v", len(ss), ok, plan.extents)
		}
	}
}

func TestCompactOnRead(t *testing.T) {
	for _, c := range []struct {
		runs, entries, visible int
		want                   bool
	}{
		{4, 4, 4, false},
		{5, 5, 3, true},
		{4, 4, 7, true},   // few entries, but fragmented
		{2, 6, 11, false}, // a chunk compacted by planKeepCompaction
		{4, 9, 15, false},
	} {
		if got := compactOnRead(c.runs, c.entries, c.visible); got != c.want {
			t.Errorf("compactOnRead(%d, %d, %d) = %v", c.runs, c.entries, c.visible, got)
		}
	}
}

func TestSliceRuns(t *testing.T) {
	ss := []*slice{base64M(), newSlice(0, 2, 8192, 0, 4096), newSlice(8192, 2, 8192, 4096, 4096),
		newSlice(0, 0, 0, 0, 4096), newSlice(0, 0, 0, 0, 4096), newSlice(0, 3, 4096, 0, 4096)}
	if n := sliceRuns(ss); n != 5 {
		t.Fatalf("want 5 runs, got %d", n)
	}
}
