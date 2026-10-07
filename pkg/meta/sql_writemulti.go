//go:build !nosqlite || !nomysql || !nopg

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
	"fmt"
	"syscall"
	"time"

	"xorm.io/xorm"
)

// mysqlChunkValueMax is the size of the chunk.slices column on MySQL (and TiDB): `xorm:"blob"` is a BLOB.
const mysqlChunkValueMax = 65535

// writeMultiLimits: on MySQL a chunk value must fit in its BLOB column, 2730 slices. Write keeps a chunk at
// most maxSlices (2500) long, so one call may add 230 slices to a chunk; the other databases have no such
// bound (bytea and SQLite blobs go to 1 GB).
func (m *dbMeta) writeMultiLimits() WriteMultiLimits {
	l := defaultWriteMultiLimits
	if m.Name() == "mysql" {
		l.ChunkSlices = mysqlChunkValueMax/sliceBytes - maxSlices
	}
	return l
}

// slicesLength is the SQL expression of the length in bytes of a chunk row's slices. On SQLite the
// concatenation of upsertSlice makes the value TEXT, whose LENGTH counts characters up to the first NUL.
func (m *dbMeta) slicesLength() string {
	if m.Name() == "sqlite3" {
		return "LENGTH(CAST(slices AS BLOB))"
	}
	return "LENGTH(slices)"
}

// chunkLength is the length in bytes of the value of one chunk row (doWriteMulti).
type chunkLength struct {
	Indx      uint32 `xorm:"indx"`
	SlicesLen int64  `xorm:"slices_len"`
}

// insertWriteRefs inserts one chunk_ref row per slice with multi-row INSERTs of up to 200 rows each (as
// many as mustInsert passes to one Insert, which with pointer beans sends one INSERT per row).
func insertWriteRefs(s *xorm.Session, writes []SliceWrite) error {
	for start := 0; start < len(writes); start += 200 {
		end := min(start+200, len(writes))
		rows := make([]sliceRef, 0, end-start)
		for _, w := range writes[start:end] {
			rows = append(rows, sliceRef{w.Slice.Id, w.Slice.Size, 1})
		}
		n, err := s.Insert(&rows)
		if err != nil {
			return err
		}
		if int(n) != len(rows) {
			return fmt.Errorf("%d of %d chunk_ref rows inserted", n, len(rows))
		}
	}
	return nil
}

// doWriteMulti is doWrite for a batch, in one transaction: the node row is locked first (FOR UPDATE), then
// each chunk row is upserted in ascending indx with the concatenation of its new slices. A compaction
// locks one chunk row and then chunk_ref rows, so the lock order has no cycle; deadlocks and
// serialization failures are retried by txn. A chunk value that is not whole slices after the append (a
// value MySQL truncated to its column size without strict mode, or one that was corrupt already) fails
// the transaction, as tkv's doWriteMulti refuses a corrupt chunk. A resend reads the chunk rows after
// the node row is locked and looks for the batch (writeMultiResent): a send of it that commits late holds
// that lock, so it is either seen or waited for. A retry by txn after a lost connection (MySQL, Postgres)
// does not look: if the commit landed, the retry fails on the chunk_ref rows of the batch's slices, which
// are already there, and the resend that follows finds the batch. (Looking in every retry would make each
// longer, and on SQLite, whose writers fail a transaction whose reads another writer has overtaken, a
// retry after a conflict would keep failing.)
func (m *dbMeta) doWriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime, since time.Time, counts map[uint32]int, delta *dirStat, attr *Attr) syscall.Errno {
	groups := groupSliceWrites(writes)
	end := sliceWritesEnd(writes)
	indxs := make([]uint32, len(groups))
	for i, g := range groups {
		indxs[i] = g.indx
	}
	return errno(m.txn(func(s *xorm.Session) error {
		nodeAttr := node{Inode: inode}
		ok, err := s.ForUpdate().Get(&nodeAttr)
		if err != nil {
			return err
		}
		if !ok {
			return syscall.ENOENT
		}
		if nodeAttr.Type != TypeFile {
			return syscall.EPERM
		}
		if !since.IsZero() {
			var cks []chunk
			if err = s.Where("inode = ?", inode).In("indx", indxs).Find(&cks); err != nil {
				return err
			}
			byIndx := make(map[uint32][]byte, len(cks))
			for _, ck := range cks {
				byIndx[ck.Indx] = ck.Slices
			}
			chunks := make([][]byte, len(groups))
			for i, g := range groups {
				chunks[i] = byIndx[g.indx]
			}
			var t Attr
			m.parseAttr(&nodeAttr, &t)
			if landed, st := writeMultiResent(inode, groups, writes, chunks, &t, since, counts); st != 0 {
				return st
			} else if landed {
				return nil
			}
		}
		*delta = dirStat{}
		growLength(&nodeAttr.Length, end, delta)
		if err := m.checkQuota(ctx, delta.space, 0, nodeAttr.Uid, nodeAttr.Gid, m.getParents(s, inode, nodeAttr.Parent)...); err != 0 {
			return err
		}
		now := time.Now().UnixNano()
		nodeAttr.setMtime(mtime.UnixNano())
		nodeAttr.setCtime(now)
		m.parseAttr(&nodeAttr, attr)

		for _, g := range groups {
			buf := make([]byte, 0, (g.end-g.start)*sliceBytes)
			for _, w := range writes[g.start:g.end] {
				buf = append(buf, marshalSlice(w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len)...)
			}
			var insert bool
			if err = m.upsertSlice(s, inode, g.indx, buf, &insert); err != nil {
				return err
			}
		}
		if err = insertWriteRefs(s, writes); err != nil {
			return err
		}
		if _, err = s.Cols("length", "mtime", "ctime", "mtimensec", "ctimensec").Update(&nodeAttr, &node{Inode: inode}); err != nil {
			return err
		}
		// The lengths of the chunk values only, not the values, which may be tens of KB each.
		var lens []chunkLength
		if err = s.Table(&chunk{}).Select("indx, "+m.slicesLength()+" AS slices_len").Where("inode = ?", inode).In("indx", indxs).Find(&lens); err != nil {
			return err
		}
		if len(lens) != len(groups) {
			return fmt.Errorf("WriteMulti of inode %d: %d of %d chunk rows found after the upsert", inode, len(lens), len(groups))
		}
		n := make(map[uint32]int, len(lens))
		for _, l := range lens {
			n[l.Indx] = int(l.SlicesLen / sliceBytes)
			if l.SlicesLen%sliceBytes != 0 {
				logger.Errorf("Invalid chunk value for inode %d indx %d: %d bytes", inode, l.Indx, l.SlicesLen)
				return syscall.EIO
			}
		}
		for _, g := range groups {
			if n[g.indx] < g.end-g.start {
				logger.Errorf("WriteMulti of inode %d: chunk %d holds %d slices after %d were appended", inode, g.indx, n[g.indx], g.end-g.start)
				return syscall.EIO
			}
		}
		for _, g := range groups {
			for k, w := range writes[g.start:g.end] {
				// the position of this slice in its chunk, counted from 1, as doWrite logs it
				pos := n[g.indx] - (g.end - g.start) + k + 1
				m.genLog(ctx, s, now, "WRITE(%d,%d,%d,%d,%d,%d,%d):%d", inode, w.Indx, w.Off, w.Slice.Id, w.Slice.Len, attr.Mtime, attr.Mtimensec, pos)
			}
		}
		for indx, c := range n {
			counts[indx] = c
		}
		return nil
	}, inode))
}
