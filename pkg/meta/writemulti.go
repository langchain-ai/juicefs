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
	"encoding/binary"
	"sort"
	"syscall"
	"time"
)

// WriteMultier is the part of Meta that puts many slices of a file in one transaction (JFS_COMMIT_MODE=epoch).
type WriteMultier interface {
	// WriteMulti puts every slice on top of its chunk in one transaction: all of them land or none does.
	// The slices of one chunk are appended in the order given; the order across chunks does not matter.
	// The length becomes at least the end of the furthest slice, and mtime is set once for the batch.
	// Every slice must have a distinct non-zero id (from NewSlice), Len > 0, fit in its chunk
	// (Off+Len <= ChunkSize) and in its object (Slice.Off+Len <= Size); EINVAL otherwise.
	// A batch over WriteMultiLimits returns E2BIG; an engine without WriteMulti returns ENOTSUP.
	//
	// EINVAL, E2BIG, ENOTSUP, EROFS, ENOENT, EPERM, EDQUOT and ENOSPC come from checks made before the
	// commit: the batch did not land. After any other error (EIO, ETIMEDOUT, ...), which can follow a
	// commit whose reply was lost, WriteMulti sends the batch again for a while, each time first looking
	// for it in the same transaction, so a batch that landed returns 0. An error it still returns (EIO,
	// EINTR, ...) leaves the batch there (whole) or not; the caller must not delete its objects, which the
	// metadata may refer to.
	WriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime time.Time) syscall.Errno
	// WriteMultiLimits returns the bounds of one WriteMulti call (the zero value without WriteMulti).
	WriteMultiLimits() WriteMultiLimits
}

// writeMultiEngine is the part of engine behind WriteMulti.
type writeMultiEngine interface {
	// doWriteMulti appends every slice of writes to its chunk in one transaction, all or none. writes is
	// grouped by chunk in ascending indx (sortSliceWrites), in the caller's order inside a chunk; baseMeta
	// validated it and checked writeMultiLimits. On success counts[indx] is each chunk's slice count after
	// the append, delta the length/space growth and attr the new attributes. A non-zero since makes it a
	// resend of a batch first sent then (resendWriteMulti): the transaction first makes the check of
	// writeMultiResent, and when it finds the batch there it writes nothing and leaves delta and attr as
	// they are.
	doWriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime, since time.Time, counts map[uint32]int, delta *dirStat, attr *Attr) syscall.Errno
	// writeMultiLimits bounds one doWriteMulti transaction by what the engine can hold (the zero value when
	// the engine does not support it). ChunkSlices is the engine's own bound, if any; baseMeta lowers it to
	// what one compaction absorbs.
	writeMultiLimits() WriteMultiLimits
}

// SliceWrite is one slice of a WriteMulti call: Slice goes on top of chunk Indx at offset Off, as in Write.
type SliceWrite struct {
	Indx  uint32
	Off   uint32
	Slice Slice
}

// WriteMultiLimits bounds one WriteMulti call. The zero value means the engine has no WriteMulti.
type WriteMultiLimits struct {
	Slices int // slices in the call
	Chunks int // distinct chunks in the call
	// ChunkSlices is the most slices the call may add to any one chunk. It is at most one less than the
	// slices one compaction takes, so that the compaction that runs when a chunk reaches its slice limit
	// can absorb one call, and on MySQL it keeps a chunk value within its BLOB column (65535 bytes).
	ChunkSlices int
}

// compactionAfterWrite decides whether a chunk whose slice count went from before to after in one
// transaction needs a compaction: in the background, or blocking (before the write returns, at
// maxSlices). A count past writeCompactionDebtThreshold always compacts; otherwise the chunk compacts each
// time its count reaches a value = interval-1 (mod interval), the configured JFS_WRITE_COMPACTION_INTERVAL.
// For a single slice (before = after-1) that is Write's shouldStartWriteCompaction(after); a batch can add
// several slices to one chunk and step over interval-1, so this checks for the crossing instead.
func (m *baseMeta) compactionAfterWrite(before, after int) (background, blocking bool) {
	interval := m.writeCompactionInterval
	if after <= writeCompactionDebtThreshold && (after+1)/interval <= (before+1)/interval {
		return false, false
	}
	if after < maxSlices {
		return true, false
	}
	return false, true
}

// compactAfterWrite starts the compaction compactionAfterWrite asks for after a WriteMulti. The caller holds
// the open-file lock of the inode (if any), so a blocking compaction holds back the next write of the file,
// as in Write.
func (m *baseMeta) compactAfterWrite(inode Ino, indx uint32, before, after, tier int) {
	background, blocking := m.compactionAfterWrite(before, after)
	if background {
		go m.compactChunk(inode, indx, false, false, tier)
	} else if blocking {
		m.compactBelowMaxSlices(inode, indx, after, tier)
	}
}

// compactBelowMaxSlices runs the blocking compaction of a chunk that holds n >= maxSlices slices, and runs
// it again as long as the chunk still holds maxSlices or more and the round before made it shorter. One
// compaction takes at most maxCompactSlices slices, and fewer if skipSome keeps the first ones. After a
// Write (one slice more) one round is enough, and Write runs one; a WriteMulti adds up to ChunkSlices,
// and a chunk that compactions could not shorten for a while may be far past maxSlices. The count falls
// strictly in every round, so the loop ends; it stops early when a compaction fails or other writers add
// as many slices.
func (m *baseMeta) compactBelowMaxSlices(inode Ino, indx uint32, n, tier int) {
	for n >= maxSlices {
		m.compactChunk(inode, indx, true, false, tier)
		ss, st := m.en.doRead(Background(), inode, indx)
		if st != 0 {
			logger.Warnf("Read chunk %d of inode %d after its compaction: %s", indx, inode, st)
			return
		}
		if len(ss) >= n {
			logger.Debugf("Compaction of chunk %d of inode %d left %d slices (%d before)", indx, inode, len(ss), n)
			return
		}
		n = len(ss)
	}
}

// Limits of one WriteMulti call. writeMultiMaxSlices bounds the size of the transaction on engines whose
// requests grow with the slice count (24 bytes per slice); writeMultiMaxChunks bounds the chunks, each of
// which is one more key (and on TKV one more rewritten value) in the transaction. TKV clients narrow the
// chunk limit to their transaction limits (kvMeta.writeMultiLimits), and MySQL the slices per chunk to its
// column size (dbMeta.writeMultiLimits).
const (
	writeMultiMaxSlices = 4096
	writeMultiMaxChunks = 1024
)

// defaultWriteMultiLimits is for an engine whose only bound on a chunk is compaction.
var defaultWriteMultiLimits = WriteMultiLimits{Slices: writeMultiMaxSlices, Chunks: writeMultiMaxChunks, ChunkSlices: writeMultiMaxSlices}

// Resends of a batch whose commit failed with an error that can follow a commit that landed (resendWriteMulti): how
// many, and the wait before the first one, doubled after each up to writeMultiResendMaxBackoff (about 26 s in all,
// plus the time the engine's client takes to fail). Variables, so tests can shorten them.
var (
	writeMultiResendTries      = 10
	writeMultiResendBackoff    = 100 * time.Millisecond
	writeMultiResendMaxBackoff = 5 * time.Second
)

// errWriteMultiUnknown is what a resent doWriteMulti returns when none of the batch's slices is in its chunks but the
// inode changed since the first send (writeMultiResent). WriteMulti returns EIO for it.
const errWriteMultiUnknown = syscall.ENOTRECOVERABLE

// writeMultiRefused reports whether a WriteMulti error comes from a check made before the commit, so the batch did not
// land (see Meta.WriteMulti). Any other error can follow a commit that landed and whose reply was lost.
func writeMultiRefused(st syscall.Errno) bool {
	switch st {
	case syscall.EINVAL, syscall.E2BIG, syscall.ENOTSUP, syscall.EROFS, syscall.ENOENT, syscall.EPERM, syscall.EDQUOT, syscall.ENOSPC:
		return true
	}
	return false
}

// resendWriteMulti sends a batch again, after a backoff, while its last send failed with an error that can follow a
// commit that landed (a connection lost before the reply, a timeout), up to writeMultiResendTries times. Such a failure
// would otherwise be final, and the epoch writer fails the whole file on it. A resend is safe: its transaction first
// looks for the batch (writeMultiResent), in the same transaction as the write, so the first send cannot land in
// between unseen, even late; it writes nothing when the batch is there, or when it cannot tell (the inode changed
// since the first send), which ends the resends with EIO.
func (m *baseMeta) resendWriteMulti(ctx Context, inode Ino, n int, st syscall.Errno, resend func() syscall.Errno) syscall.Errno {
	backoff := writeMultiResendBackoff
	for try := 0; st != 0 && st != errWriteMultiUnknown && !writeMultiRefused(st) && try < writeMultiResendTries && !ctx.Canceled(); try++ {
		logger.Warnf("WriteMulti of inode %d (%d slices): %s, whether it landed is not known; sending it again in %s", inode, n, st, backoff)
		time.Sleep(backoff)
		backoff = min(2*backoff, writeMultiResendMaxBackoff)
		st = resend()
	}
	if st == errWriteMultiUnknown {
		logger.Errorf("WriteMulti of inode %d (%d slices): none of its slices is there, but the inode changed since it was sent: "+
			"it may have landed and been compacted since; not sending it again", inode, n)
		st = syscall.EIO
	}
	return st
}

// writeMultiResent is the check a resent doWriteMulti makes in its transaction before it writes, given chunks, the
// value of the chunk of each of the batch's groups (marshalSlice records), and attr, the inode's attributes. The
// batch's slice ids are new and it lands whole, so any of them in its chunks means it landed: it returns true, with
// counts set to the chunks' slice counts (a compaction since may have absorbed the others). With none there, the batch
// did not land if the inode's ctime is older than since, the first send, as a landed send sets it later (0);
// otherwise it may have landed and been compacted away since, or the inode changed for another reason, and it returns
// errWriteMultiUnknown. A zero since skips the ctime rule (tkv checks for the batch in every transaction, see its
// doWriteMulti). It reads the ids in place, as tkv runs it in every transaction of a batch.
func writeMultiResent(inode Ino, groups []sliceWriteGroup, writes []SliceWrite, chunks [][]byte, attr *Attr, since time.Time, counts map[uint32]int) (bool, syscall.Errno) {
	ids := make(map[uint64]struct{}, len(writes))
	for _, w := range writes {
		ids[w.Slice.Id] = struct{}{}
	}
	var found int
	for _, c := range chunks {
		for off := 0; off+sliceBytes <= len(c); off += sliceBytes {
			if _, ok := ids[binary.BigEndian.Uint64(c[off+4:off+12])]; ok { // the id, after the 4-byte pos (marshalSlice)
				found++
			}
		}
	}
	if found > 0 {
		logger.Warnf("WriteMulti of inode %d: %d of its %d slices are there already: it landed", inode, found, len(writes))
		for i, g := range groups {
			counts[g.indx] = len(chunks[i]) / sliceBytes
		}
		return true, 0
	}
	if !since.IsZero() && !time.Unix(attr.Ctime, int64(attr.Ctimensec)).Before(since) {
		return false, errWriteMultiUnknown
	}
	return false, 0
}

// sliceWriteGroup is the run writes[start:end] of a sorted batch, which goes to chunk indx.
type sliceWriteGroup struct {
	indx       uint32
	start, end int
}

// sortSliceWrites validates a WriteMulti batch and returns a copy of it grouped by chunk in ascending
// indx, keeping the given order inside each chunk, plus the number of slices per chunk.
func sortSliceWrites(writes []SliceWrite) ([]SliceWrite, map[uint32]int, syscall.Errno) {
	ids := make(map[uint64]struct{}, len(writes))
	added := make(map[uint32]int)
	for _, w := range writes {
		s := w.Slice
		if s.Id == 0 || s.Len == 0 || uint64(w.Off)+uint64(s.Len) > ChunkSize || uint64(s.Off)+uint64(s.Len) > uint64(s.Size) {
			logger.Warnf("WriteMulti: invalid slice %+v at chunk %d offset %d", s, w.Indx, w.Off)
			return nil, nil, syscall.EINVAL
		}
		if _, dup := ids[s.Id]; dup {
			logger.Warnf("WriteMulti: slice %d is given twice", s.Id)
			return nil, nil, syscall.EINVAL
		}
		ids[s.Id] = struct{}{}
		added[w.Indx]++
	}
	sorted := make([]SliceWrite, len(writes))
	copy(sorted, writes)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Indx < sorted[j].Indx })
	return sorted, added, 0
}

// groupSliceWrites splits a batch sorted by sortSliceWrites into its per-chunk runs, in ascending indx.
func groupSliceWrites(writes []SliceWrite) []sliceWriteGroup {
	var groups []sliceWriteGroup
	for i, w := range writes {
		if n := len(groups); n > 0 && groups[n-1].indx == w.Indx {
			groups[n-1].end = i + 1
		} else {
			groups = append(groups, sliceWriteGroup{indx: w.Indx, start: i, end: i + 1})
		}
	}
	return groups
}

// sliceWritesEnd is the file offset where the furthest slice of the batch ends.
func sliceWritesEnd(writes []SliceWrite) uint64 {
	var end uint64
	for _, w := range writes {
		if e := uint64(w.Indx)*ChunkSize + uint64(w.Off) + uint64(w.Slice.Len); e > end {
			end = e
		}
	}
	return end
}

// growLength extends *length to end if end is past it and records the growth in delta. Applied once with
// the furthest end of a batch it gives the sum of what applying each slice in turn would give.
func growLength(length *uint64, end uint64, delta *dirStat) {
	if end > *length {
		delta.length = int64(end - *length)
		delta.space = align4K(end) - align4K(*length)
		*length = end
	}
}

// WriteMultiLimits is the engine's, with ChunkSlices lowered to one less than the slices one compaction
// takes (maxCompactSlices, 100 with etcd): the blocking compaction at maxSlices then absorbs a whole call
// in one round, so a chunk stays near maxSlices as with Write. Otherwise a chunk that receives many slices
// per call would grow past maxSlices by the difference in every call.
func (m *baseMeta) WriteMultiLimits() WriteMultiLimits {
	l := m.en.writeMultiLimits()
	if l.Slices <= 0 || l.Chunks <= 0 || l.ChunkSlices <= 0 {
		return WriteMultiLimits{}
	}
	l.ChunkSlices = min(l.ChunkSlices, maxCompactSlices-1)
	return l
}

func (m *baseMeta) WriteMulti(ctx Context, inode Ino, writes []SliceWrite, mtime time.Time) syscall.Errno {
	limits := m.WriteMultiLimits()
	if limits.Slices <= 0 {
		return syscall.ENOTSUP
	}
	if len(writes) == 0 {
		return 0
	}
	sorted, added, st := sortSliceWrites(writes)
	if st != 0 {
		return st
	}
	var most int // the most slices of the batch in one chunk
	for _, n := range added {
		most = max(most, n)
	}
	if len(sorted) > limits.Slices || len(added) > limits.Chunks || most > limits.ChunkSlices {
		logger.Warnf("WriteMulti of inode %d: %d slices in %d chunks, up to %d in one, is over the limit of %d slices in %d chunks, %d in one",
			inode, len(sorted), len(added), most, limits.Slices, limits.Chunks, limits.ChunkSlices)
		return syscall.E2BIG
	}
	sent := time.Now()
	if len(sorted) == 1 {
		// A single slice is a Write: the same transaction, changelog entry and compaction as in chunk mode. Only a
		// failure whose outcome is unknown goes on, to be resent as a batch of one.
		w := sorted[0]
		if st = m.Write(ctx, inode, w.Indx, w.Off, w.Slice, mtime); st == 0 || writeMultiRefused(st) {
			return st
		}
	}
	defer m.timeit("WriteMulti", time.Now())
	f := m.of.find(inode)
	if f != nil {
		f.Lock()
		defer f.Unlock()
	}
	defer func() {
		for indx := range added {
			m.of.InvalidateChunk(inode, indx)
		}
	}()
	counts := make(map[uint32]int, len(added))
	var delta dirStat
	var attr Attr
	if len(sorted) > 1 {
		st = m.en.doWriteMulti(ctx, inode, sorted, mtime, time.Time{}, counts, &delta, &attr)
	}
	st = m.resendWriteMulti(ctx, inode, len(sorted), st, func() syscall.Errno {
		return m.en.doWriteMulti(ctx, inode, sorted, mtime, sent, counts, &delta, &attr)
	})
	if st == 0 {
		m.updateParentStat(ctx, inode, attr.Parent, delta.length, delta.space)
		m.updateUserGroupStat(ctx, attr.Uid, attr.Gid, delta.space, 0)
		for _, g := range groupSliceWrites(sorted) {
			n := counts[g.indx]
			m.compactAfterWrite(inode, g.indx, n-added[g.indx], n, int(attr.Tier))
		}
	}
	return st
}
