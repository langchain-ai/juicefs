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

//nolint:errcheck
package meta

import (
	"reflect"
	"testing"
	"time"
)

// A chunk with no records reads as zeros, so hole-only records must not create chunks: neither
// when cloning a file nor when punching, zeroing or truncating a sparse range.
func testZeroChunks(t *testing.T, m Meta) {
	testCloneSkipsZeroOnlyChunks(t, m)
	testZeroRangeOnSparseFile(t, m)
}

type dataRange struct {
	pos uint32
	Slice
}

// readDataRanges returns the parts of a chunk that are backed by a slice; the rest reads as zeros.
func readDataRanges(t *testing.T, m Meta, inode Ino, indx uint32) []dataRange {
	t.Helper()
	var ss []Slice
	if st := m.Read(Background(), inode, indx, &ss); st != 0 {
		t.Fatalf("read inode %d chunk %d: %s", inode, indx, st)
	}
	var rs []dataRange
	var pos uint32
	for _, s := range ss {
		if s.Id > 0 {
			rs = append(rs, dataRange{pos, s})
		}
		pos += s.Len
	}
	return rs
}

// chunkRecords returns the number of raw records stored for a chunk; 0 means the chunk does not exist.
func chunkRecords(t *testing.T, m Meta, inode Ino, indx uint32) int {
	t.Helper()
	ss, st := m.getBase().en.doRead(Background(), inode, indx)
	if st != 0 {
		t.Fatalf("raw read inode %d chunk %d: %s", inode, indx, st)
	}
	return len(ss)
}

func testCloneSkipsZeroOnlyChunks(t *testing.T, m Meta) {
	ctx := Background()
	var srcDir Ino
	if st := m.Mkdir(ctx, RootInode, "zeroChunkSrc", 0755, 022, 0, &srcDir, nil); st != 0 {
		t.Fatalf("mkdir zeroChunkSrc: %s", st)
	}
	var src Ino
	if st := m.Mknod(ctx, srcDir, "f", TypeFile, 0644, 022, 0, "", &src, nil); st != 0 {
		t.Fatalf("mknod f: %s", st)
	}
	var id uint64
	if st := m.NewSlice(ctx, &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	if st := m.Write(ctx, src, 0, 0, Slice{Id: id, Size: 1 << 20, Off: 0, Len: 1 << 20}, time.Now()); st != 0 {
		t.Fatalf("write: %s", st)
	}
	// Copying a sparse range writes hole records, as older clients also did for discards: chunk 0
	// gets one over part of its data, chunks 1 and 2 get only hole records.
	var hole Ino
	if st := m.Mknod(ctx, RootInode, "zeroChunkHole", TypeFile, 0644, 022, 0, "", &hole, nil); st != 0 {
		t.Fatalf("mknod zeroChunkHole: %s", st)
	}
	if st := m.Truncate(ctx, hole, 0, 3*ChunkSize, nil, false); st != 0 {
		t.Fatalf("truncate zeroChunkHole: %s", st)
	}
	if st := m.CopyFileRange(ctx, hole, 0, src, 4096, 8192, 0, nil, nil); st != 0 {
		t.Fatalf("copy hole into chunk 0: %s", st)
	}
	if st := m.CopyFileRange(ctx, hole, 0, src, ChunkSize, 2*ChunkSize, 0, nil, nil); st != 0 {
		t.Fatalf("copy hole into chunks 1 and 2: %s", st)
	}
	// Chunk 3 is only covered by the length.
	if st := m.Truncate(ctx, src, 0, 3*ChunkSize+100, nil, false); st != 0 {
		t.Fatalf("truncate: %s", st)
	}
	for indx := uint32(0); indx <= 2; indx++ {
		if n := chunkRecords(t, m, src, indx); n == 0 {
			t.Fatalf("source chunk %d has no records", indx)
		}
	}
	if got := readDataRanges(t, m, src, 0); len(got) != 2 {
		t.Fatalf("source chunk 0 reads %+v, want data around a hole", got)
	}

	check := func(label string, dst Ino) {
		t.Helper()
		var sattr, dattr Attr
		if st := m.GetAttr(ctx, src, &sattr); st != 0 {
			t.Fatalf("getattr source: %s", st)
		}
		if st := m.GetAttr(ctx, dst, &dattr); st != 0 {
			t.Fatalf("%s: getattr: %s", label, st)
		}
		if dattr.Length != sattr.Length {
			t.Fatalf("%s: length %d, want %d", label, dattr.Length, sattr.Length)
		}
		if n, want := chunkRecords(t, m, dst, 0), chunkRecords(t, m, src, 0); n != want {
			t.Fatalf("%s: chunk 0 has %d records, want %d", label, n, want)
		}
		for indx := uint32(1); indx <= 3; indx++ {
			if n := chunkRecords(t, m, dst, indx); n != 0 {
				t.Fatalf("%s: zero-only chunk %d was copied (%d records)", label, indx, n)
			}
		}
		for indx := uint32(0); indx <= 3; indx++ {
			if got, want := readDataRanges(t, m, dst, indx), readDataRanges(t, m, src, indx); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: chunk %d reads %+v, want %+v", label, indx, got, want)
			}
		}
	}

	// One file: doCloneEntry.
	var count, total uint64
	if st := m.Clone(ctx, srcDir, src, srcDir, "fclone", CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total); st != 0 {
		t.Fatalf("clone file: %s", st)
	}
	var fclone Ino
	if st := m.Lookup(ctx, srcDir, "fclone", &fclone, &Attr{}, false); st != 0 {
		t.Fatalf("lookup fclone: %s", st)
	}
	check("file clone", fclone)

	// A directory: its files go through doBatchClone.
	if st := m.Clone(ctx, RootInode, srcDir, RootInode, "zeroChunkDst", CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total); st != 0 {
		t.Fatalf("clone dir: %s", st)
	}
	var dstDir, dst Ino
	if st := m.Lookup(ctx, RootInode, "zeroChunkDst", &dstDir, &Attr{}, false); st != 0 {
		t.Fatalf("lookup zeroChunkDst: %s", st)
	}
	for _, name := range []string{"f", "fclone"} {
		if st := m.Lookup(ctx, dstDir, name, &dst, &Attr{}, false); st != 0 {
			t.Fatalf("lookup %s: %s", name, st)
		}
		check("batch clone of "+name, dst)
	}
}

func testZeroRangeOnSparseFile(t *testing.T, m Meta) {
	ctx := Background()
	var inode Ino
	if st := m.Mknod(ctx, RootInode, "zeroRangeSparse", TypeFile, 0644, 022, 0, "", &inode, nil); st != 0 {
		t.Fatalf("mknod: %s", st)
	}
	noChunks := func(label string, indxs ...uint32) {
		t.Helper()
		for _, indx := range indxs {
			if n := chunkRecords(t, m, inode, indx); n != 0 {
				t.Fatalf("%s: chunk %d has %d records, want none", label, indx, n)
			}
		}
	}
	length := func(label string, want uint64) {
		t.Helper()
		var attr Attr
		if st := m.GetAttr(ctx, inode, &attr); st != 0 {
			t.Fatalf("%s: getattr: %s", label, st)
		}
		if attr.Length != want {
			t.Fatalf("%s: length %d, want %d", label, attr.Length, want)
		}
	}

	if st := m.Truncate(ctx, inode, 0, 4*ChunkSize+100, nil, false); st != 0 {
		t.Fatalf("truncate: %s", st)
	}
	noChunks("extend sparse file", 0, 1, 2, 3, 4)
	if st := m.Fallocate(ctx, inode, fallocPunchHole|fallocKeepSize, 100, 3*ChunkSize, nil); st != 0 {
		t.Fatalf("punch hole: %s", st)
	}
	noChunks("punch hole", 0, 1, 2, 3, 4)
	length("punch hole", 4*ChunkSize+100)
	if st := m.Fallocate(ctx, inode, fallocZeroRange, 0, 5*ChunkSize, nil); st != 0 {
		t.Fatalf("zero range: %s", st)
	}
	noChunks("zero range", 0, 1, 2, 3, 4)
	length("zero range", 5*ChunkSize)

	// Chunks with data still get the hole records that hide it. Each chunk stays under 5 records
	// so that reads do not start a compaction.
	ids := map[uint32]uint64{}
	for _, indx := range []uint32{1, 2} {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("new slice: %s", st)
		}
		if st := m.Write(ctx, inode, indx, 0, Slice{Id: id, Size: 1 << 20, Off: 0, Len: 1 << 20}, time.Now()); st != 0 {
			t.Fatalf("write chunk %d: %s", indx, st)
		}
		ids[indx] = id
	}
	if st := m.Fallocate(ctx, inode, fallocPunchHole|fallocKeepSize, 2*ChunkSize+4096, ChunkSize, nil); st != 0 {
		t.Fatalf("punch hole over data: %s", st)
	}
	want := []dataRange{{0, Slice{Id: ids[2], Size: 1 << 20, Off: 0, Len: 4096}}}
	if got := readDataRanges(t, m, inode, 2); !reflect.DeepEqual(got, want) {
		t.Fatalf("after punch over data: chunk 2 reads %+v, want %+v", got, want)
	}
	noChunks("punch hole over data", 0, 3, 4)

	// Shrinking hides the data past the new end, and growing again must not expose it.
	if st := m.Truncate(ctx, inode, 0, ChunkSize+512<<10, nil, false); st != 0 {
		t.Fatalf("shrink: %s", st)
	}
	if st := m.Truncate(ctx, inode, 0, 3*ChunkSize+100, nil, false); st != 0 {
		t.Fatalf("grow: %s", st)
	}
	length("shrink and grow", 3*ChunkSize+100)
	want = []dataRange{{0, Slice{Id: ids[1], Size: 1 << 20, Off: 0, Len: 512 << 10}}}
	if got := readDataRanges(t, m, inode, 1); !reflect.DeepEqual(got, want) {
		t.Fatalf("after shrink and grow: chunk 1 reads %+v, want %+v", got, want)
	}
	if got := readDataRanges(t, m, inode, 2); len(got) != 0 {
		t.Fatalf("after shrink and grow: chunk 2 reads %+v, want none", got)
	}
	noChunks("shrink and grow", 0, 3, 4)
}
