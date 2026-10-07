//go:build !noredis

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
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func (m *redisMeta) writeMultiLimits() WriteMultiLimits {
	return defaultWriteMultiLimits
}

// doWriteMulti is doWrite for a batch: under WATCH of the inode key, one MULTI/EXEC with one RPUSH per
// chunk (ascending indx, the chunk's slices in order as separate values), one SET of the inode and one
// INCRBY of the used space. As in doWrite the chunk keys are not watched: a compaction watches its chunk
// key, so an append makes the compaction retry, never the write. A resend reads the chunks under the same
// WATCH first (writeMultiResent): any send of the batch that lands sets the inode key, so it cannot land
// between that check and the EXEC without making the EXEC fail and the check run again.
func (m *redisMeta) doWriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime, since time.Time, counts map[uint32]int, delta *dirStat, attr *Attr) syscall.Errno {
	groups := groupSliceWrites(writes)
	end := sliceWritesEnd(writes)
	return errno(m.txn(ctx, func(tx *redis.Tx) error {
		a, err := tx.Get(ctx, m.inodeKey(inode)).Bytes()
		if err != nil {
			return err
		}
		var t Attr
		m.parseAttr(a, &t)
		if t.Typ != TypeFile {
			return syscall.EPERM
		}
		if !since.IsZero() {
			chunks := make([][]byte, len(groups))
			for i, g := range groups {
				vals, err := tx.LRange(ctx, m.chunkKey(inode, g.indx), 0, -1).Result()
				if err != nil {
					return err
				}
				for _, v := range vals {
					if len(v) == sliceBytes { // a corrupt value would shift the ones after it
						chunks[i] = append(chunks[i], v...)
					}
				}
			}
			if landed, st := writeMultiResent(inode, groups, writes, chunks, &t, since, counts); st != 0 {
				return st
			} else if landed {
				return nil
			}
		}
		*delta = dirStat{}
		*attr = t
		growLength(&attr.Length, end, delta)
		if err := m.checkQuota(ctx, delta.space, 0, attr.Uid, attr.Gid, m.getParents(ctx, tx, inode, attr.Parent)...); err != 0 {
			return err
		}
		now := time.Now()
		attr.Mtime = mtime.Unix()
		attr.Mtimensec = uint32(mtime.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())

		rpush := make([]*redis.IntCmd, len(groups))
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for i, g := range groups {
				vals := make([]interface{}, 0, g.end-g.start)
				for _, w := range writes[g.start:g.end] {
					vals = append(vals, marshalSlice(w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len))
				}
				rpush[i] = pipe.RPush(ctx, m.chunkKey(inode, g.indx), vals...)
			}
			pipe.Set(ctx, m.inodeKey(inode), m.marshal(attr), 0)
			if delta.space > 0 {
				pipe.IncrBy(ctx, m.usedSpaceKey(), delta.space)
			}
			// One WRITE entry per slice, so a changelog reader sees the same entries as for Write. As in
			// doWrite, the slice count is not known before EXEC; it is logged as 0.
			for _, w := range writes {
				m.genLog(ctx, pipe, now, "WRITE(%d,%d,%d,%d,%d,%d,%d):%d", inode, w.Indx, w.Off, w.Slice.Id, w.Slice.Len, attr.Mtime, attr.Mtimensec, 0)
			}
			return nil
		})
		if err == nil {
			for i, g := range groups {
				counts[g.indx] = int(rpush[i].Val())
			}
		}
		return err
	}, m.inodeKey(inode)))
}
