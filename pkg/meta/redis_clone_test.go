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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Concurrent clones of one multi-chunk file (a burst of boxes from one snapshot image) must each get
// the source's chunk lists, sparse chunks included, and bump every slice's refcount once per clone.
func TestRedisConcurrentCloneOfOneFile(t *testing.T) {
	m := newTestRedisMeta(t, 12)
	ctx := Background()
	var srcDir, dstDir Ino
	if st := m.Mkdir(ctx, RootInode, "src_concurrent", 0777, 022, 0, &srcDir, nil); st != 0 {
		t.Fatalf("mkdir src_concurrent: %s", st)
	}
	if st := m.Mkdir(ctx, RootInode, "dst_concurrent", 0777, 022, 0, &dstDir, nil); st != 0 {
		t.Fatalf("mkdir dst_concurrent: %s", st)
	}
	var src Ino
	if st := m.Mknod(ctx, srcDir, "rootfs.ext4", TypeFile, 0644, 022, 0, "", &src, nil); st != 0 {
		t.Fatalf("mknod rootfs.ext4: %s", st)
	}
	// Chunks 0, 1 and 3 hold data; chunk 2 is a hole.
	const sliceSize = uint32(4096)
	written := map[uint32]uint64{}
	for _, indx := range []uint32{0, 1, 3} {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("new slice: %s", st)
		}
		if st := m.Write(ctx, src, indx, 0, Slice{Id: id, Size: sliceSize, Off: 0, Len: sliceSize}, time.Now()); st != 0 {
			t.Fatalf("write chunk %d: %s", indx, st)
		}
		written[indx] = id
	}

	refsBefore := map[uint32]int64{}
	for indx, id := range written {
		refsBefore[indx] = redisSliceRefCount(t, m, id, sliceSize)
	}

	const clones = 16
	errs := make([]error, clones)
	var wg sync.WaitGroup
	for i := range clones {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var count, total uint64
			if st := m.Clone(ctx, srcDir, src, dstDir, fmt.Sprintf("clone-%d", i), CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total); st != 0 {
				errs[i] = st
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("clone-%d: %s", i, err)
		}
	}

	for indx, id := range written {
		if got := redisSliceRefCount(t, m, id, sliceSize); got != refsBefore[indx]+clones {
			t.Fatalf("chunk %d slice refcount: want %d, got %d", indx, refsBefore[indx]+clones, got)
		}
	}
	for i := range clones {
		var ino Ino
		var attr Attr
		if st := m.Lookup(ctx, dstDir, fmt.Sprintf("clone-%d", i), &ino, &attr, false); st != 0 {
			t.Fatalf("lookup clone-%d: %s", i, st)
		}
		for indx := uint32(0); indx < 4; indx++ {
			var want, got []Slice
			if st := m.Read(ctx, src, indx, &want); st != 0 {
				t.Fatalf("read source chunk %d: %s", indx, st)
			}
			if st := m.Read(ctx, ino, indx, &got); st != 0 {
				t.Fatalf("read clone-%d chunk %d: %s", i, indx, st)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("clone-%d chunk %d: want %+v, got %+v", i, indx, want, got)
			}
		}
	}
}
