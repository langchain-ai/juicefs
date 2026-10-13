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

import "github.com/juicedata/juicefs/pkg/utils"

type slice struct {
	id    uint64
	size  uint32
	off   uint32
	len   uint32
	pos   uint32
	left  *slice
	right *slice
}

func newSlice(pos uint32, id uint64, cleng, off, len uint32) *slice {
	if len == 0 {
		return nil
	}
	s := &slice{}
	s.pos = pos
	s.id = id
	s.size = cleng
	s.off = off
	s.len = len
	s.left = nil
	s.right = nil
	return s
}

func (s *slice) read(buf []byte) {
	rb := utils.ReadBuffer(buf)
	s.pos = rb.Get32()
	s.id = rb.Get64()
	s.size = rb.Get32()
	s.off = rb.Get32()
	s.len = rb.Get32()
}

func (s *slice) cut(pos uint32) (left, right *slice) {
	if s == nil {
		return nil, nil
	}
	if pos <= s.pos {
		if s.left == nil {
			s.left = newSlice(pos, 0, 0, 0, s.pos-pos)
		}
		left, s.left = s.left.cut(pos)
		return left, s
	} else if pos < s.pos+s.len {
		l := pos - s.pos
		right = newSlice(pos, s.id, s.size, s.off+l, s.len-l)
		right.right = s.right
		s.len = l
		s.right = nil
		return s, right
	} else {
		if s.right == nil {
			s.right = newSlice(s.pos+s.len, 0, 0, 0, pos-s.pos-s.len)
		}
		s.right, right = s.right.cut(pos)
		return s, right
	}
}

func (s *slice) visit(f func(*slice)) {
	if s == nil {
		return
	}
	s.left.visit(f)
	right := s.right
	f(s) // s could be freed
	right.visit(f)
}

const sliceBytes = 24

func marshalSlice(pos uint32, id uint64, size, off, len uint32) []byte {
	w := utils.NewBuffer(sliceBytes)
	w.Put32(pos)
	w.Put64(id)
	w.Put32(size)
	w.Put32(off)
	w.Put32(len)
	return w.Bytes()
}

func readSlices(vals []string) []*slice {
	slices := make([]slice, len(vals))
	ss := make([]*slice, len(vals))
	for i, val := range vals {
		if len(val) != sliceBytes {
			logger.Errorf("corrupt slice: len=%d, val=%v", len(val), []byte(val))
			return nil
		}
		s := &slices[i]
		s.read([]byte(val))
		ss[i] = s
	}
	return ss
}

func readSliceBuf(buf []byte) []*slice {
	if len(buf)%sliceBytes != 0 {
		logger.Errorf("corrupt slices: len=%d", len(buf))
		return nil
	}
	nSlices := len(buf) / sliceBytes
	slices := make([]slice, nSlices)
	ss := make([]*slice, nSlices)
	for i := 0; i < len(buf); i += sliceBytes {
		s := &slices[i/sliceBytes]
		s.read(buf[i:])
		ss[i/sliceBytes] = s
	}
	return ss
}

func buildSlice(ss []*slice) []Slice {
	var root *slice
	for i := range ss {
		s := new(slice)
		*s = *ss[i]
		var right *slice
		s.left, right = root.cut(s.pos)
		_, s.right = right.cut(s.pos + s.len)
		root = s
	}
	var pos uint32
	var chunk []Slice
	root.visit(func(s *slice) {
		if s.pos > pos {
			chunk = append(chunk, Slice{Size: s.pos - pos, Len: s.pos - pos})
			pos = s.pos
		}
		chunk = append(chunk, Slice{Id: s.id, Size: s.size, Off: s.off, Len: s.len})
		pos += s.len
	})
	return chunk
}

func compactChunk(ss []*slice) (uint32, uint32, []Slice) {
	var chunk = buildSlice(ss)
	var pos uint32
	n := len(chunk)
	for n > 1 {
		if chunk[0].Id == 0 {
			pos += chunk[0].Len
			chunk = chunk[1:]
			n--
		} else if chunk[n-1].Id == 0 {
			chunk = chunk[:n-1]
			n--
		} else {
			break
		}
	}
	if n == 1 && chunk[0].Id == 0 {
		chunk[0].Len = 1
	}
	var size uint32
	for _, c := range chunk {
		size += c.Len
	}
	return pos, size, chunk
}

func skipSome(chunk []*slice) int {
	var skipped int
	var total = len(chunk)
OUT:
	for skipped < total {
		ss := chunk[skipped:]
		pos, size, c := compactChunk(ss)
		first := ss[0]
		if first.len < (1<<20) || first.len*5 < size || size == 0 {
			// it's too small
			break
		}
		isFirst := func(pos uint32, s Slice) bool {
			return pos == first.pos && s.Id == first.id && s.Off == first.off && s.Len == first.len
		}
		if !isFirst(pos, c[0]) {
			// it's not the first slice, compact it
			break
		}
		for _, s := range ss[1:] {
			if *s == *first {
				break OUT
			}
		}
		skipped++
	}
	return skipped
}

// A file whose ProfileXattr is ProfileVMDisk is compacted with
// planKeepCompaction, so a clone keeps sharing the data it has not overwritten.
const (
	ProfileXattr  = "user.juicefs.profile"
	ProfileVMDisk = "vm-disk"

	keepMinLen     = 1 << 20   // the oldest slice is kept only if at least this long
	keepMergeGap   = 128 << 10 // changed ranges closer than this are rewritten as one
	keepMaxExtents = 256       // more changed ranges than this fall back to a full compaction
)

// keepPlan rewrites only the changed ranges of a chunk into one new slice, which
// the chunk then references once per range on top of its oldest slice.
type keepPlan struct {
	data    []Slice     // what to copy into the new slice, in order
	extents [][2]uint32 // position and length of each rewritten range
}

// planKeepCompaction plans a compaction of ss that leaves ss[0] in place and
// rewrites the ranges later slices changed. ok is false when the chunk should be
// compacted the usual way instead: ss[0] is too small to be worth keeping, or so
// much has changed that keeping it would leave more pieces than data. An empty
// plan with ok means there is nothing to do.
func planKeepCompaction(ss []*slice) (plan keepPlan, ok bool) {
	if len(ss) < 2 {
		return plan, true
	}
	first := ss[0]
	if first.id == 0 || first.len < keepMinLen {
		return plan, false
	}
	// Already compacted this way, with nothing written since.
	if len(ss) == 2 || sliceRuns(ss[1:]) == 1 {
		return plan, true
	}
	segs := buildSlice(ss)
	end := first.pos + first.len
	var pos, dirty uint32
	for _, s := range segs {
		kept := s.Id == first.id && s.Size == first.size && pos >= first.pos && s.Off == first.off+(pos-first.pos)
		// A hole over the kept range hides its data, so it has to be rewritten too.
		if !kept && (s.Id != 0 || (pos < end && pos+s.Len > first.pos)) {
			if n := len(plan.extents); n > 0 && pos-(plan.extents[n-1][0]+plan.extents[n-1][1]) < keepMergeGap {
				plan.extents[n-1][1] = pos + s.Len - plan.extents[n-1][0]
			} else {
				plan.extents = append(plan.extents, [2]uint32{pos, s.Len})
			}
		}
		pos += s.Len
	}
	for _, e := range plan.extents {
		dirty += e[1]
	}
	if len(plan.extents) > keepMaxExtents || dirty > first.len/2 {
		return keepPlan{}, false
	}
	plan.data = clipSegments(segs, plan.extents)
	return plan, true
}

// clipSegments returns the parts of segs inside extents, in order.
func clipSegments(segs []Slice, extents [][2]uint32) []Slice {
	var out []Slice
	var pos uint32
	i := 0
	for _, s := range segs {
		for i < len(extents) && extents[i][0]+extents[i][1] <= pos {
			i++
		}
		for j := i; j < len(extents) && extents[j][0] < pos+s.Len; j++ {
			start, stop := max(pos, extents[j][0]), min(pos+s.Len, extents[j][0]+extents[j][1])
			if start >= stop {
				continue
			}
			c := Slice{Id: s.Id, Size: s.Size, Off: s.Off + start - pos, Len: stop - start}
			if s.Id == 0 {
				c.Size, c.Off = c.Len, 0
			}
			out = append(out, c)
		}
		pos += s.Len
	}
	return out
}

// sliceRuns counts the entries of ss, counting consecutive entries of one slice
// once: a chunk compacted by planKeepCompaction lists its new slice per range.
func sliceRuns(ss []*slice) int {
	var n int
	for i, s := range ss {
		if i == 0 || s.id == 0 || s.id != ss[i-1].id || s.size != ss[i-1].size {
			n++
		}
	}
	return n
}
