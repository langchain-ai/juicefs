/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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
	"strings"
	"syscall"
	"time"
)

// writeMultiLimits narrows the chunk limit to what each client's transaction can hold. One doWriteMulti
// reads and rewrites the inode plus every chunk it touches, and a chunk value is up to about
// (maxSlices+ChunkSlices)*24 bytes after the append: (2500+999)*24 = 84 KB (baseMeta.WriteMultiLimits
// lowers ChunkSlices to 999, 99 with etcd). The chunk numbers are conservative bounds from the documented
// defaults, not measurements:
//   - etcd: --max-txn-ops 128 (one compare per read key plus one put per written key, about 2n+3) and
//     --max-request-bytes 1.5 MiB (n rewritten chunk values): 16 chunks.
//   - FoundationDB: 10 MB per transaction (128 chunks of up to 60 KB before the append: 7.7 MB) and
//     100 KB per value (a chunk stays under 84 KB); 1024 slices, as before the per-chunk bound.
//   - Badger: a transaction is capped at about 15% of the 64 MiB memtable (ErrTxnTooBig): 64 chunks.
//   - TiKV: txn-total-size-limit 100 MB, txn-entry-size-limit 6 MB: 256 chunks.
//   - memkv (tests): no limit beyond the defaults.
//
// Any other client gets the etcd bound.
func (m *kvMeta) writeMultiLimits() WriteMultiLimits {
	l := defaultWriteMultiLimits
	switch m.client.name() {
	case "memkv":
	case "tikv":
		l.Chunks = 256
	case "badger":
		l.Chunks = 64
	case "fdb":
		l.Slices, l.Chunks = 1024, 128
	default:
		l.Chunks = 16
	}
	return l
}

// doWriteMulti is doWrite for a batch, in one transaction that reads the inode and every chunk it touches
// and writes each chunk (old value plus the new slices, in order) and the inode. A compaction that sets
// one of the chunks meanwhile makes the whole batch retry. Every transaction, not only a resend, first
// looks for the batch (writeMultiResent), as the client may retry a commit whose outcome it did not
// learn: a batch with any of its slices there already returns 0 and changes nothing.
func (m *kvMeta) doWriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime, since time.Time, counts map[uint32]int, delta *dirStat, attr *Attr) syscall.Errno {
	groups := groupSliceWrites(writes)
	end := sliceWritesEnd(writes)
	keys := make([][]byte, 0, 1+len(groups))
	keys = append(keys, m.inodeKey(inode))
	for _, g := range groups {
		keys = append(keys, m.chunkKey(inode, g.indx))
	}
	vals := make([][]byte, len(writes))
	for i, w := range writes {
		vals[i] = marshalSlice(w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len)
	}
	return errno(m.txn(ctx, func(tx *kvTxn) error {
		rs := tx.gets(keys...)
		if rs[0] == nil {
			return syscall.ENOENT
		}
		var t Attr
		m.parseAttr(rs[0], &t)
		if t.Typ != TypeFile {
			return syscall.EPERM
		}
		chunks := rs[1:]
		for i, g := range groups {
			if len(chunks[i])%sliceBytes != 0 {
				logger.Errorf("Invalid chunk value for inode %d indx %d: %d", inode, g.indx, len(chunks[i]))
				return syscall.EIO
			}
		}
		if landed, st := writeMultiResent(inode, groups, writes, chunks, &t, since, counts); st != 0 {
			return st
		} else if landed {
			return nil
		}
		*delta = dirStat{}
		*attr = t
		growLength(&attr.Length, end, delta)
		if err := m.checkQuota(ctx, delta.space, 0, attr.Uid, attr.Gid, m.getParents(tx, inode, attr.Parent)...); err != 0 {
			return err
		}
		now := time.Now()
		attr.Mtime = mtime.Unix()
		attr.Mtimensec = uint32(mtime.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		for i, g := range groups {
			val := make([]byte, 0, len(chunks[i])+(g.end-g.start)*sliceBytes)
			val = append(val, chunks[i]...)
			for _, v := range vals[g.start:g.end] {
				val = append(val, v...)
			}
			tx.set(keys[i+1], val)
			counts[g.indx] = len(val) / sliceBytes
		}
		tx.set(m.inodeKey(inode), m.marshal(attr))
		if m.fmt.ChangeLog {
			// genLog keys its entry by the transaction id, so a batch gets one entry that lists every slice
			// as indx:off:id:len:position (Redis and SQL log one WRITE entry per slice).
			entries := make([]string, 0, len(writes))
			for _, g := range groups {
				first := counts[g.indx] - (g.end - g.start)
				for k, w := range writes[g.start:g.end] {
					entries = append(entries, fmt.Sprintf("%d:%d:%d:%d:%d", w.Indx, w.Off, w.Slice.Id, w.Slice.Len, first+k+1))
				}
			}
			m.genLog(tx, now, "WRITEMULTI(%d,%d,%d,%d):%s", inode, attr.Mtime, attr.Mtimensec, len(writes), strings.Join(entries, ","))
		}
		return nil
	}, inode))
}
