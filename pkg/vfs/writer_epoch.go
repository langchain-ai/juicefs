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
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Epoch commits (see commitEpochs). The age and the slice limit can be changed per client with
	// JFS_EPOCH_MAX_AGE_MS and JFS_EPOCH_MAX_SLICES, read once when the client starts (commitConfigFromEnv).
	defaultEpochMaxAge    = time.Second * 5 // an epoch closes this long after its first slice was created
	defaultEpochMaxSlices = 512             // slices in one epoch, which commits in one WriteMulti
	epochIdle             = time.Second     // an epoch closes once its file had no write for this long
	epochBufferedSlices   = 800             // the flusher closes the open epoch of a file with more slices not uploaded

	// Per-file limits on pending slices in epoch mode (dataWriter.maxBufferedSlices and maxPendingSlices; see
	// fileWriter.waitSliceCap). A slice holds its write buffers until its upload finishes: upstream's limit of 1000
	// slices per file, which now counts those only. An uploaded slice waiting for its epoch, or for an older epoch,
	// holds no buffers (the store frees each block once it is uploaded), only its bookkeeping (about 1-3 KiB), so many
	// more of them may queue behind one slow upload before writes are held back.
	defaultMaxBufferedSlices = 1000
	defaultMaxPendingSlices  = 10000
)

// sliceEpoch is the part of a sliceWriter only epoch mode uses.
type sliceEpoch struct {
	ep        *epoch    // the epoch the slice was created in, and commits with
	lastWrite time.Time // the last write into it (lastMod is also set by UpdateMtime)
}

// fileEpoch is the part of a fileWriter only epoch mode uses (see commitEpochs); protected by the file. Epoch mode also
// uses the file's commitcond (commitEpochs waits for an epoch, and for its slices to be done), readcond (a read waits
// for its epoch) and capcond (waitSliceCap).
type fileEpoch struct {
	open         *epoch   // the epoch new slices go into
	queue        []*epoch // closed epochs not yet handled by commitEpochs, oldest first
	nextEpoch    uint64   // id of the open epoch
	committing   bool     // commitEpochs is running (it holds a file ref)
	writes       uint64   // Writes that put data into the file's slices
	pendingCount int      // slices of the open and queued epochs: every slice in f.chunks
	donePending  int      // how many of them are done (uploaded, or given up): they hold no write buffers
	capwaiting   uint16   // writes held back by waitSliceCap
	// The oldest epoch with a slice whose upload failed (0: none), and that error, as soon as the slice is done
	// (markDone): its slices and those of newer epochs, which commitEpochs will drop, skip their uploads. f.err is set
	// only once commitEpochs gets to that epoch, after the older ones commit.
	failedEpoch uint64
	failedErr   syscall.Errno
	bufferStall bool // a write closed the open epoch for the buffer stall, and no write saw the buffer below its size since
}

// writerEpoch is the part of a dataWriter only epoch mode uses (commitConfig); dataWriter.epochs selects the mode.
type writerEpoch struct {
	epochMaxAge       time.Duration
	epochMaxSlices    int
	maxBufferedSlices int // per file; see waitSliceCap
	maxPendingSlices  int
	sequentialOnce    sync.Once
	bufferUsed        func() int64 // tests: what epoch mode's write throttle reads instead of usedBufferSize
	// Times a defensive branch of the epoch writer ran, one that its invariants make unreachable (each also logs an
	// error). Tests fail when it is not zero (epochState, and the end of each test of the writer).
	invariantBreaks atomic.Int64
}

// initCommitMode sets the commit mode of a new writer from JFS_COMMIT_MODE and the epoch settings (commitConfigFromEnv).
func (w *dataWriter) initCommitMode() {
	cc, err := commitConfigFromEnv()
	if err != nil {
		logger.Fatalf("Invalid writer commit config: %s", err)
	}
	w.epochs = cc.epochs
	w.epochMaxAge = cc.maxAge
	w.epochMaxSlices = cc.maxSlices
	w.maxBufferedSlices = defaultMaxBufferedSlices
	w.maxPendingSlices = defaultMaxPendingSlices
	if w.epochs {
		logger.Infof("Writer commits in epochs (JFS_COMMIT_MODE=epoch): each closes %s after its first slice, or after %d slices",
			w.epochMaxAge, w.epochMaxSlices)
	}
}

// initEpochs sets up a new fileWriter for epoch mode (Open).
func (f *fileWriter) initEpochs() {
	if f.w.epochs {
		f.capcond = utils.NewCond(f)
		f.nextEpoch = 1
		f.open = newEpoch(f.nextEpoch)
	}
}

// chunksToFreeze is what flush freezes the slices of: all of the file's chunks in chunk mode. In epoch mode it closes
// the open epoch instead, which freezes its slices, and returns none: flushwaiting holds back new writes, so this closes
// the last epoch the flush waits for, and commitEpochs empties f.chunks once every epoch is handled.
// protected by file
func (f *fileWriter) chunksToFreeze() map[uint32]*chunkWriter {
	if f.w.epochs {
		f.closeEpoch("flush")
		return nil
	}
	return f.chunks
}

// Epoch commits: the writer groups the slices of a file into epochs and commits each epoch in one metadata transaction
// (meta.WriteMulti), oldest first, so what the metadata engine holds after a client crash is the file's content after
// all the writes before some epoch boundary. These show what that costs and what it buys.
var (
	writerEpochs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_epochs_total",
		Help: "Epochs handled by the writer: outcome committed, failed (an upload or the commit failed), or abandoned " +
			"(an older epoch of the same file failed).",
	}, []string{"outcome"})
	writerEpochClosed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_epoch_closed_total",
		Help: "Epochs closed, by reason: age (the epoch's maximum age), idle (no write to the file for a second), flush " +
			"(fsync, close, truncate, ...), read (a read overlapped it), cap (a slice or engine limit), buffer (the write " +
			"buffer is full), error (an older epoch failed).",
	}, []string{"reason"})
	writerEpochCommits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_epoch_commits_total",
		Help: "Epoch commits, by mode: multi (one WriteMulti transaction) or sequential (one Write per slice, in order, " +
			"when the metadata engine has no WriteMulti: a crash can then leave part of an epoch).",
	}, []string{"mode"})
	writerEpochSlices = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_slices",
		Help:    "Slices in an epoch when it closes.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	})
	writerEpochChunks = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_chunks",
		Help:    "Chunks an epoch has slices in when it closes.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})
	writerEpochBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_bytes",
		Help:    "Bytes in the slices of an epoch when it closes.",
		Buckets: prometheus.ExponentialBuckets(4096, 2, 22),
	})
	writerEpochAge = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_age_seconds",
		Help:    "Time from the first slice of an epoch to its close.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	writerEpochUploadWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_upload_wait_seconds",
		Help:    "Time from the close of an epoch to the end of the last upload of its slices.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18),
	})
	writerEpochOrderWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_order_wait_seconds",
		Help:    "Time an uploaded epoch waited for the older epochs of its file to be handled before its commit.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18),
	})
	writerEpochCommitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_commit_seconds",
		Help:    "Time the commit of an epoch took in the metadata engine (WriteMulti, or the Writes of a sequential commit).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	})
	writerEpochDurableLag = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_epoch_durable_lag_seconds",
		Help:    "Time from the first slice of an epoch to its commit: how long its first write could be lost in a crash.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	writerSlicesFrozenByEpoch = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_slices_frozen_by_epoch_total",
		Help: "Slices still taking writes when their epoch closed, by the reason of the close: writes that would have " +
			"extended them start new slices.",
	}, []string{"reason"})
	// Observed by flushRangeEpoch only, so with JFS_READ_FLUSH_RANGE=true: with it off, a read in epoch mode waits in
	// Flush, which this does not count.
	writerReadEpochWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "writer_read_epoch_wait_seconds",
		Help:    "Time a read waited for the epoch holding the newest pending write it overlaps to commit.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18),
	})
	writerSlicesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "writer_slices_created_total",
		Help: "Slices created by the writer in epoch mode.",
	})
	writerSlicesAbandoned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "writer_slices_abandoned_total",
		Help: "Slices dropped without a commit because their epoch, or an older epoch of the same file, failed.",
	})
	writerSliceCapWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "writer_slice_cap_wait_seconds",
		Help: "Time a write was held back because its file had too many pending slices: reason buffered = slices still " +
			"holding their buffers (not yet uploaded), gated = all pending slices, uploaded ones waiting for their epoch included.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18),
	}, []string{"reason"})
)

var (
	epochOutcomes     = []string{"committed", "failed", "abandoned"}
	epochCloseReasons = []string{"age", "idle", "flush", "read", "cap", "buffer", "error"}
	epochCommitModes  = []string{"multi", "sequential"}
	sliceCapReasons   = []string{"buffered", "gated"}
)

// InitWriterMetrics registers the metrics of an epoch writer, with every label value, so a series exists before its
// first event. A writer in chunk mode has none of them: it exposes the metrics it did before epochs existed.
func InitWriterMetrics(writer DataWriter, registerer prometheus.Registerer) {
	dw, ok := writer.(*dataWriter)
	if registerer == nil || !ok || !dw.epochs {
		return
	}
	for _, o := range epochOutcomes {
		writerEpochs.WithLabelValues(o)
	}
	for _, r := range epochCloseReasons {
		writerEpochClosed.WithLabelValues(r)
		writerSlicesFrozenByEpoch.WithLabelValues(r)
	}
	for _, m := range epochCommitModes {
		writerEpochCommits.WithLabelValues(m)
	}
	for _, r := range sliceCapReasons {
		writerSliceCapWait.WithLabelValues(r)
	}
	for _, c := range []prometheus.Collector{writerEpochs, writerEpochClosed, writerEpochCommits, writerEpochSlices,
		writerEpochChunks, writerEpochBytes, writerEpochAge, writerEpochUploadWait, writerEpochOrderWait,
		writerEpochCommitDuration, writerEpochDurableLag, writerSlicesFrozenByEpoch, writerReadEpochWait,
		writerSlicesCreated, writerSlicesAbandoned, writerSliceCapWait} {
		_ = registerer.Register(c) // as InitMemoryBufferMetrics: another client of the process may have registered it
	}
}

// commitConfig is how a client commits the slices it writes. It comes from the environment, read once when the client
// starts (NewDataWriter), so one binary runs either arm of an A/B.
type commitConfig struct {
	epochs    bool          // JFS_COMMIT_MODE: "epoch" groups slices into epochs; "chunk" (the default) commits per chunk
	maxAge    time.Duration // JFS_EPOCH_MAX_AGE_MS: an epoch closes this long after its first slice
	maxSlices int           // JFS_EPOCH_MAX_SLICES: the most slices in one epoch
}

// commitConfigFromEnv reads the commit config. An unset or empty variable takes its default; any other value must be a
// valid one: the mode exactly (no case folding, no surrounding spaces), the numbers as strconv.Atoi reads them (a leading
// + and leading zeros pass; spaces do not). An invalid value is an error, not a default: a typo must not run the other
// arm of an A/B, or its parameters, without anyone noticing (NewDataWriter refuses to start). The epoch settings are
// checked in chunk mode too, which ignores them.
func commitConfigFromEnv() (commitConfig, error) {
	c := commitConfig{maxAge: defaultEpochMaxAge, maxSlices: defaultEpochMaxSlices}
	switch mode := os.Getenv("JFS_COMMIT_MODE"); mode {
	case "", "chunk":
	case "epoch":
		c.epochs = true
	default:
		return c, fmt.Errorf("JFS_COMMIT_MODE must be chunk or epoch, not %q", mode)
	}
	ms, ok, err := envInt("JFS_EPOCH_MAX_AGE_MS", 1, 3600*1000)
	if err != nil {
		return c, err
	} else if ok {
		c.maxAge = time.Duration(ms) * time.Millisecond
	}
	// Two at least: a write that crosses a chunk boundary adds two slices, and must fit one epoch (writeEpoch).
	n, ok, err := envInt("JFS_EPOCH_MAX_SLICES", 2, 1<<20)
	if err != nil {
		return c, err
	} else if ok {
		c.maxSlices = n
	}
	return c, nil
}

// CheckCommitConfig returns the error NewDataWriter would stop the client with, if the commit config in the environment
// is invalid (commitConfigFromEnv). A client checks it before it starts its work: mount before it daemonizes, so the
// error reaches the terminal rather than only the log, and fs.NewFileSystem (the gateway, WebDAV and the Java SDK)
// returns it rather than end the process.
func CheckCommitConfig() error {
	_, err := commitConfigFromEnv()
	return err
}

// envInt parses the integer environment variable name, which must be in [lo, hi]. ok is false when it is unset or
// empty; err is set when it is not a valid value.
func envInt(name string, lo, hi int) (int, bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < lo || n > hi {
		return 0, false, fmt.Errorf("%s must be an integer from %d to %d, not %q", name, lo, hi, value)
	}
	return n, true, nil
}

// dropped returns the error that dooms the slice's epoch (epoch mode), if any: the file's error, set once an epoch
// failed and every newer one is dropped, or the error of a failed slice of the same epoch or an older one (failedEpoch),
// whose epoch fails when commitEpochs gets to it. An epoch older than failedEpoch still commits.
// protected by file
func (s *sliceWriter) dropped() syscall.Errno {
	f := s.chunk.file
	if f.err != 0 {
		return f.err
	}
	if f.failedEpoch != 0 && s.ep != nil && s.ep.id >= f.failedEpoch {
		return f.failedErr
	}
	return 0
}

// dropIfFailed skips the upload of a slice whose epoch will not commit (epoch mode, see dropped): commitEpochs drops
// it rather than commit it, so neither its upload nor its slice id (which a metadata outage would have it wait for,
// buffers held) is of any use.
func (s *sliceWriter) dropIfFailed() bool {
	f := s.chunk.file
	f.Lock()
	ferr := s.dropped()
	if ferr != 0 {
		s.err = ferr
	}
	f.Unlock()
	if ferr == 0 {
		return false
	}
	logger.Debugf("flush inode: %v chunk: %d dropped after an earlier error: %s", f.inode, s.id, ferr)
	s.abort()
	return true
}

// abort drops the slice's data: it frees the buffers and removes the blocks uploaded already. In epoch mode it waits
// for the uploads in flight first (FlushTo starts them while the slice takes writes), so none lands after the removal
// and leaves an object nothing refers to; chunk mode keeps upstream's Abort. Not under the file lock.
func (s *sliceWriter) abort() {
	if s.chunk.file.w.epochs {
		s.writer.AbortAfterUploads()
	} else {
		s.writer.Abort()
	}
}

// epoch is a group of slices of one file that commit together, in one metadata transaction (commitEpochs). Every slice
// is created in the file's open epoch and commits with it; closeEpoch freezes all of them, so the slices of the epochs
// up to k hold exactly the writes made before epoch k closed.
type epoch struct {
	id        uint64
	slices    []*sliceWriter // creation order; fixed once closed
	chunks    map[uint32]int // slices per chunk
	notDone   int            // slices not yet done (markDone)
	started   time.Time      // the first slice was created: the age clock
	lastWrite time.Time      // the last write into the epoch: the idle clock
	closed    time.Time
	lastDone  time.Time // the last slice was done
	writes    uint64    // the file's writes when the epoch closed: its slices and the older ones hold exactly those
	reason    string    // why it closed (writer_epoch_closed_total)
	committed bool      // handled by commitEpochs: committed, failed or abandoned
}

func newEpoch(id uint64) *epoch {
	return &epoch{id: id, chunks: make(map[uint32]int)}
}

// fits reports whether a write of [off, off+size) can go into the epoch within the limits. A write adds at most one
// slice to each chunk it spans (writeChunk). An empty epoch takes any write: one larger than an epoch is split
// (writeEpoch).
func (e *epoch) fits(off, size uint64, l meta.WriteMultiLimits) bool {
	if len(e.slices) == 0 || size == 0 {
		return true
	}
	first, last := off/meta.ChunkSize, (off+size-1)/meta.ChunkSize
	if uint64(len(e.slices))+last-first+1 > uint64(l.Slices) {
		return false
	}
	chunks := len(e.chunks)
	for i := first; i <= last; i++ {
		if n, ok := e.chunks[uint32(i)]; !ok {
			chunks++
		} else if n >= l.ChunkSlices {
			return false
		}
	}
	return chunks <= l.Chunks
}

func (e *epoch) hasUnfrozen() bool {
	for _, s := range e.slices {
		if !s.freezed {
			return true
		}
	}
	return false
}

// epochLimits bounds one epoch: the client's slice limit, and what one WriteMulti of the metadata engine takes. An
// engine without WriteMulti commits an epoch one slice at a time (commitEpoch), bounded by the slice limit only.
func (w *dataWriter) epochLimits() meta.WriteMultiLimits {
	l := w.m.WriteMultiLimits()
	if l.Slices <= 0 {
		return meta.WriteMultiLimits{Slices: w.epochMaxSlices, Chunks: math.MaxInt, ChunkSlices: math.MaxInt}
	}
	return meta.WriteMultiLimits{Slices: min(l.Slices, w.epochMaxSlices), Chunks: max(l.Chunks, 1), ChunkSlices: max(l.ChunkSlices, 1)}
}

// closeEpoch ends the open epoch and installs the next one, in one critical section: every slice of the epoch still
// taking writes is frozen (its upload starts), the epoch is queued for commitEpochs, and later writes go to new slices
// of the next epoch. Nothing happens if the open epoch has no slice.
// protected by file
func (f *fileWriter) closeEpoch(reason string) {
	e := f.open
	if len(e.slices) == 0 {
		return
	}
	now := time.Now()
	var frozen int
	var bytes uint64
	for _, s := range e.slices {
		if !s.freezed {
			s.freezed = true
			go s.flushData()
			frozen++
		}
		bytes += uint64(s.slen)
	}
	e.closed, e.writes, e.reason = now, f.writes, reason
	f.queue = append(f.queue, e)
	f.nextEpoch++
	f.open = newEpoch(f.nextEpoch)
	if !f.committing { // unreachable: commitEpochs runs while a slice is pending (writeChunkEpoch)
		f.w.invariantBroken("write inode:%d: epoch %d closed with no committer running", f.inode, e.id)
		f.startCommitter()
	}
	f.commitcond.Broadcast()

	writerEpochClosed.WithLabelValues(reason).Inc()
	writerSlicesFrozenByEpoch.WithLabelValues(reason).Add(float64(frozen))
	writerEpochSlices.Observe(float64(len(e.slices)))
	writerEpochChunks.Observe(float64(len(e.chunks)))
	writerEpochBytes.Observe(float64(bytes))
	writerEpochAge.Observe(now.Sub(e.started).Seconds())
}

// startCommitter starts commitEpochs with a ref of the file, so the file's writer lives while it has pending slices.
// protected by file
func (f *fileWriter) startCommitter() {
	f.committing = true
	f.w.Lock()
	f.refs++
	f.w.Unlock()
	go f.commitEpochs()
}

// commitEpochs commits the closed epochs of the file, oldest first, each once all of its slices are uploaded: one
// meta.WriteMulti with the epoch's slices in creation order, so the transaction appends each chunk's slices in the
// order they were created. It runs while the file has pending slices (writeChunkEpoch starts it), and replaces the
// per-chunk commitThread of chunk mode.
//
// After a failure (an upload or the commit of an epoch), the file's error is sticky and no later epoch commits: each is
// dropped (abandoned) once its slices are done, and the objects its slices uploaded are removed, since no metadata
// refers to them. The objects of a failed commit are removed only when the engine reports that the batch did not land.
func (f *fileWriter) commitEpochs() {
	defer f.w.free(f)
	f.Lock()
	var lastHandled time.Time
	for len(f.queue) > 0 || len(f.open.slices) > 0 {
		if len(f.queue) == 0 {
			f.commitcond.WaitWithTimeout(time.Second) // closeEpoch broadcasts; the timeout is a backstop
			continue
		}
		e := f.queue[0]
		for e.notDone > 0 {
			if f.commitcond.WaitWithTimeout(time.Millisecond * 100) { // markDone broadcasts; the timeout is a backstop
				f.freezeStragglers(e)
			}
		}
		ready := e.closed
		if e.lastDone.After(ready) {
			ready = e.lastDone
		}
		writerEpochUploadWait.Observe(ready.Sub(e.closed).Seconds())
		var orderWait time.Duration
		if lastHandled.After(ready) {
			orderWait = lastHandled.Sub(ready)
		}
		writerEpochOrderWait.Observe(orderWait.Seconds())

		abandon := f.err != 0
		var uploadErr syscall.Errno
		var slices []*sliceWriter // the slices with data, in creation order
		var writes []meta.SliceWrite
		var mtimes []time.Time
		var mtime time.Time
		for _, s := range e.slices {
			if s.err != 0 && uploadErr == 0 {
				uploadErr = s.err
			}
			if s.slen == 0 {
				continue // nothing was written into it
			}
			slices = append(slices, s)
			writes = append(writes, meta.SliceWrite{Indx: s.chunk.indx, Off: s.off, Slice: meta.Slice{Id: s.id, Size: s.length, Off: s.soff, Len: s.slen}})
			mtimes = append(mtimes, s.lastMod)
			if s.lastMod.After(mtime) {
				mtime = s.lastMod
			}
		}
		f.Unlock()

		var st syscall.Errno
		var landed, sent int
		var sequential bool
		if !abandon && uploadErr == 0 && len(writes) > 0 {
			st, landed, sent, sequential = f.commitEpoch(writes, mtimes, mtime)
		}

		f.Lock()
		outcome := "committed"
		var remove []*sliceWriter // uploaded slices no metadata refers to
		switch {
		case abandon:
			outcome = "abandoned"
			remove = slices
		case uploadErr != 0:
			outcome = "failed"
			f.fail(e, uploadErr)
			remove = slices
		case st != 0:
			outcome = "failed"
			f.fail(e, st)
			if commitDidNotLand(st, !sequential) {
				remove = slices[landed:]
			} else {
				remove = slices[sent:] // the batch may have landed; a sequential commit sent none after the failed slice
			}
		}
		writerEpochs.WithLabelValues(outcome).Inc()
		if outcome == "committed" {
			writerEpochDurableLag.Observe(time.Since(e.started).Seconds())
		} else {
			writerSlicesAbandoned.Add(float64(len(e.slices) - landed))
		}
		var objects []sliceObject
		for _, s := range remove {
			if s.err == 0 && s.id > 0 && s.length > 0 {
				objects = append(objects, sliceObject{s.id, int(s.length)})
			}
		}
		f.popEpoch(e)
		lastHandled = time.Now()
		if len(objects) > 0 {
			go f.w.removeSlices(f.inode, objects)
		}
	}
	f.committing = false
	f.Unlock()
}

// freezeStragglers freezes the slices of the closed epoch e that still take writes. closeEpoch froze all of them, and
// nothing unfreezes a slice, so there are none: one would keep its epoch, and every newer one, from committing.
// protected by file
func (f *fileWriter) freezeStragglers(e *epoch) {
	for _, s := range e.slices {
		if !s.freezed {
			f.w.invariantBroken("write inode:%d: slice %d of closed epoch %d is unfrozen", f.inode, s.id, e.id)
			s.freezed = true
			go s.flushData()
		}
	}
}

// commitEpoch commits the slices of one epoch, in creation order: one WriteMulti, or one Write per slice when the
// metadata engine has no WriteMulti (ENOTSUP) or does not take the batch (E2BIG, which the epoch limits should
// prevent). A sequential commit keeps the order but not the atomicity: a crash in the middle leaves part of the epoch.
// It returns the error, the number of slices that landed, the number sent (a failed commit may have landed), and
// whether the commit was sequential.
func (f *fileWriter) commitEpoch(writes []meta.SliceWrite, mtimes []time.Time, mtime time.Time) (st syscall.Errno, landed, sent int, sequential bool) {
	start := time.Now()
	ctx := meta.Background()
	mode := "multi"
	st = f.w.m.WriteMulti(ctx, f.inode, writes, mtime)
	sent = len(writes)
	if st == syscall.ENOTSUP || st == syscall.E2BIG {
		mode, sequential = "sequential", true
		f.w.warnSequential(f.inode, len(writes), st)
		sent = 0
		for i, w := range writes {
			sent++
			if st = f.w.m.Write(ctx, f.inode, w.Indx, w.Off, w.Slice, mtimes[i]); st != 0 {
				break
			}
			landed++
		}
	} else if st == 0 {
		landed = len(writes)
	}
	writerEpochCommits.WithLabelValues(mode).Inc()
	writerEpochCommitDuration.Observe(time.Since(start).Seconds())
	// The reader may hold old bytes of these ranges (read-ahead): drop them before a waiting read is woken, and after a
	// failure too, as the batch may have landed.
	for _, w := range writes[:sent] {
		f.w.reader.Invalidate(f.inode, uint64(w.Indx)*meta.ChunkSize+uint64(w.Off), uint64(w.Slice.Len))
	}
	return st, landed, sent, sequential
}

// commitDidNotLand reports whether a failed commit certainly left nothing in the metadata: the errors that WriteMulti
// (multi) or Write return from their checks, made before the transaction. Other errors, such as EIO after a lost
// connection, may follow a commit that landed.
func commitDidNotLand(st syscall.Errno, multi bool) bool {
	switch st {
	case syscall.ENOENT, syscall.ENOSPC, syscall.EDQUOT:
		return true
	case syscall.EINVAL, syscall.E2BIG, syscall.ENOTSUP, syscall.EROFS, syscall.EPERM:
		return multi
	}
	return false
}

// fail records the failure of epoch e (an upload, or its commit) as the file's sticky error, and closes the open epoch:
// every newer epoch is dropped (commitEpochs), and closing it now lets its slices finish, and free their buffers, without
// waiting for the flusher. Waiting reads return the error.
// protected by file
func (f *fileWriter) fail(e *epoch, err syscall.Errno) {
	if err != syscall.ENOENT && err != syscall.ENOSPC && err != syscall.EDQUOT {
		logger.Warnf("write inode:%d error: %s", f.inode, err)
		err = syscall.EIO
	}
	f.err = err
	logger.Errorf("write inode:%d epoch %d (%d slices): %s", f.inode, e.id, len(e.slices), err)
	f.closeEpoch("error")
	if f.readwaiting > 0 {
		f.readcond.Broadcast()
	}
	if f.capwaiting > 0 {
		f.capcond.Broadcast()
	}
}

// popEpoch removes the epoch commitEpochs has just handled, which is the oldest one, and its slices, which are the
// oldest pending ones of their chunks, and wakes whoever waits for it.
// protected by file
func (f *fileWriter) popEpoch(e *epoch) {
	for _, s := range e.slices {
		s.committed = true
		c := s.chunk
		if len(c.slices) > 0 && c.slices[0] == s {
			c.slices[0] = nil
			c.slices = c.slices[1:]
		} else {
			// Epochs are handled in order and a chunk's slices are in creation order, so this is unreachable; drop the
			// slice wherever it is so the file cannot get stuck behind it.
			for i, cs := range c.slices {
				if cs == s {
					f.w.invariantBroken("write inode:%d: slice %d of chunk %d handled ahead of %d older ones", f.inode, s.id, c.indx, i)
					c.slices = append(c.slices[:i], c.slices[i+1:]...)
					break
				}
			}
		}
		if len(c.slices) == 0 {
			f.freeChunk(c)
		}
		f.pendingCount--
		if s.done {
			f.donePending--
		}
	}
	e.committed = true
	if len(f.queue) > 0 && f.queue[0] == e {
		f.queue[0] = nil
		f.queue = f.queue[1:]
	}
	if f.readwaiting > 0 {
		f.readcond.Broadcast()
	}
	if f.capwaiting > 0 {
		f.capcond.Broadcast()
	}
}

// sliceObject is the id and length of an uploaded slice, for store.Remove.
type sliceObject struct {
	id     uint64
	length int
}

// removeSlices deletes the objects of slices that were uploaded but not committed (commitEpochs): no metadata refers to
// them, so nothing else would.
func (w *dataWriter) removeSlices(inode Ino, objects []sliceObject) {
	for _, o := range objects {
		if err := w.store.Remove(o.id, o.length); err != nil {
			logger.Warnf("remove dropped slice %d of inode %d: %s", o.id, inode, err)
		}
	}
}

// invariantBroken logs a state the epoch writer's invariants rule out, which the defensive branch that found it then
// repairs, and counts it (invariantBreaks), so tests catch it.
func (w *dataWriter) invariantBroken(format string, args ...interface{}) {
	w.invariantBreaks.Add(1)
	logger.Errorf(format, args...)
}

// warnSequential logs, once per client, that epochs commit slice by slice.
func (w *dataWriter) warnSequential(inode Ino, n int, st syscall.Errno) {
	if st == syscall.E2BIG {
		logger.Errorf("WriteMulti of %d slices of inode %d: %s, over the engine's limits; committing them one by one", n, inode, st)
		return
	}
	w.sequentialOnce.Do(func() {
		logger.Warnf("the metadata engine has no WriteMulti (%s): epochs commit one slice at a time, in order, and a crash "+
			"can leave part of an epoch", st)
	})
}

// writeChunkEpoch is writeChunk in epoch mode: a new slice goes into the open epoch, and commits with it. There is no
// dependency between slices of different chunks: an older slice is in the same epoch, which commits atomically, or in an
// older one, which commits first.
// protected by file
func (f *fileWriter) writeChunkEpoch(ctx meta.Context, indx uint32, off uint32, data []byte) syscall.Errno {
	c := f.findChunk(indx)
	s := c.findWritableSlice(off, uint32(len(data)))
	e := f.open
	if s != nil && s.ep != e {
		// Unreachable: only unfrozen slices take writes, and closeEpoch freezes every slice of the epoch it closes. A
		// write into it would land in a closed epoch; a new slice is always correct (it shadows the older ones).
		f.w.invariantBroken("write inode:%d: slice %d of chunk %d is unfrozen in closed epoch %d", f.inode, s.id, indx, s.ep.id)
		s.freezed = true
		go s.flushData()
		s = nil
	}
	now := time.Now()
	if s == nil {
		s = &sliceWriter{
			chunk:      c,
			off:        off,
			writer:     f.w.store.NewWriter(0, f.tierID),
			notify:     utils.NewCond(&f.Mutex),
			started:    now,
			sliceEpoch: sliceEpoch{ep: e},
		}
		go s.prepareID(meta.Background(), false)
		c.slices = append(c.slices, s)
		if len(e.slices) == 0 {
			e.started = now
		}
		e.slices = append(e.slices, s)
		e.chunks[indx]++
		e.notDone++
		f.pendingCount++
		writerSlicesCreated.Inc()
		if !f.committing {
			f.startCommitter()
		}
	}
	e.lastWrite = now
	return s.write(ctx, off-s.off, data)
}

// overSliceCap reports which limit on pending slices the file is at, if any (see waitSliceCap).
// protected by file
func (f *fileWriter) overSliceCap() string {
	switch {
	case f.pendingCount-f.donePending >= f.w.maxBufferedSlices:
		return "buffered"
	case f.pendingCount >= f.w.maxPendingSlices:
		return "gated"
	}
	return ""
}

// openEpochHoldsCap reports whether the open epoch alone keeps the file at the limit on pending slices r: buffered, when
// its unfrozen slices alone reach maxBufferedSlices (they start to upload only once frozen), or gated, when its slices
// alone reach maxPendingSlices (they commit only once it closes). Otherwise the slices over the limit are ones of older
// epochs, or ones already frozen, and their uploads and commits take the count down without closing the open epoch.
// protected by file
func (f *fileWriter) openEpochHoldsCap(r string) bool {
	switch r {
	case "buffered":
		unfrozen := 0
		for _, s := range f.open.slices {
			if !s.freezed {
				unfrozen++
			}
		}
		return unfrozen >= f.w.maxBufferedSlices
	case "gated":
		return len(f.open.slices) >= f.w.maxPendingSlices
	}
	return false
}

// waitSliceCap holds a write back, in epoch mode, while the file has too many pending slices: maxBufferedSlices that
// still hold their buffers, or maxPendingSlices in all. The second limit is reached when one slow upload holds back the
// commits of every newer epoch of the file while writes go on. The wait closes the open epoch only when the open epoch
// alone holds the count (openEpochHoldsCap): otherwise the wait would last until the file is idle for a second. A close
// when older or already frozen slices hold the count would only cut the open epoch's slices short, and every write
// held at the limit would start new slices, more uploads for a store that is already behind. The time spent here goes
// to writer_slice_cap_wait_seconds, by the limit that held the write.
func (f *fileWriter) waitSliceCap() {
	f.Lock()
	defer f.Unlock()
	var reason string
	var start time.Time
	for r := f.overSliceCap(); r != "" && f.err == 0; r = f.overSliceCap() {
		if reason == "" {
			reason, start = r, time.Now()
		}
		if f.openEpochHoldsCap(r) { // this empties the open epoch: it closes again only once writes fill it to the limit
			f.closeEpoch("cap")
		}
		f.capwaiting++
		f.capcond.WaitWithTimeout(time.Millisecond * 100) // markDone and popEpoch broadcast; the timeout is a backstop
		f.capwaiting--
	}
	if reason != "" {
		writerSliceCapWait.WithLabelValues(reason).Observe(time.Since(start).Seconds())
	}
}

// epochBufferUsed is what epoch mode's write throttle reads: usedBufferSize, or a test's value (bufferUsed).
func (w *dataWriter) epochBufferUsed() int64 {
	if w.bufferUsed != nil {
		return w.bufferUsed()
	}
	return w.usedBufferSize()
}

// throttleEpochWrite is chunk mode's write throttle, in epoch mode: a write slows down while the write buffers of the
// client are over bufferSize, and waits while they are over twice that. The unfrozen slices of the file keep their
// last, partial block until their epoch closes, so the first write of the file that meets a hard stall closes the open
// epoch, to have them upload and free their buffers; the others of the same stall do not (bufferStall), as the usage
// is the whole client's, and another file's backlog would otherwise cut this file's epochs at every write. It reports
// whether the buffers were below bufferSize, which ends the stall.
func (f *fileWriter) throttleEpochWrite() bool {
	if f.w.epochBufferUsed() <= f.w.bufferSize {
		return true
	}
	time.Sleep(time.Millisecond * 10) // slow down
	if f.w.epochBufferUsed() > f.w.bufferSize*2 {
		f.Lock()
		if !f.bufferStall && len(f.open.slices) > 0 {
			f.bufferStall = true
			f.closeEpoch("buffer")
		}
		f.Unlock()
		for f.w.epochBufferUsed() > f.w.bufferSize*2 {
			time.Sleep(time.Millisecond * 100)
		}
	}
	return false
}

// writeEpoch is Write in epoch mode. The epoch limits are checked before any of the data goes in, so a write that
// crosses a chunk boundary lands in one epoch: a boundary is never inside a Write (unless the write alone is larger than
// an epoch may be, see below).
func (f *fileWriter) writeEpoch(ctx meta.Context, off uint64, data []byte) syscall.Errno {
	f.waitSliceCap()
	belowThrottle := f.throttleEpochWrite()

	s := time.Now()
	f.Lock()
	defer f.Unlock()
	if belowThrottle {
		f.bufferStall = false // the stall is over: the next one may close the open epoch again
	}
	size := uint64(len(data))
	f.writewaiting++
	for f.flushwaiting > 0 {
		if f.writecond.WaitWithTimeout(time.Second) && ctx.Canceled() {
			f.writewaiting--
			logger.Warnf("write %d interrupted after %d", f.inode, time.Since(s))
			return syscall.EINTR
		}
	}
	f.writewaiting--
	if f.err != 0 {
		// An epoch failed: every newer one is dropped, so this write would be too.
		return f.err
	}
	if size == 0 {
		return 0
	}
	limits := f.w.epochLimits()
	if !f.open.fits(off, size, limits) {
		f.closeEpoch("cap")
	}
	f.writes++

	indx := uint32(off / meta.ChunkSize)
	pos := uint32(off % meta.ChunkSize)
	for len(data) > 0 {
		n := uint32(len(data))
		if pos+n > meta.ChunkSize {
			n = meta.ChunkSize - pos
		}
		// Only a write that does not fit even an empty epoch (more chunks than the limits allow) gets here with an epoch
		// it does not fit: it is split at chunk boundaries.
		if !f.open.fits(uint64(indx)*meta.ChunkSize+uint64(pos), uint64(n), limits) {
			f.closeEpoch("cap")
		}
		if st := f.writeChunk(ctx, indx, pos, data[:n]); st != 0 {
			return st
		}
		data = data[n:]
		indx++
		pos = (pos + n) % meta.ChunkSize
	}
	if off+size > f.length {
		f.length = off + size
	}
	return f.err
}

// flushRangeEpoch is flushRange in epoch mode. Epochs commit in order, each atomically, so a read that overlaps pending
// writes waits for the newest epoch holding one of them: if that is the open epoch, the read closes it first. A read
// that overlaps only closed epochs waits for the newest of those and leaves the open epoch open; one that overlaps no
// pending write waits for nothing. Like flushRange, it does not hold back writes to the file.
//
// It returns the file's error, if any, even for a read that overlaps nothing pending: after a failed epoch every newer
// one is dropped, and which ranges they covered is not kept, so the read fails (VFS.Read) rather than return old
// bytes. The error lasts as long as the file's writer, until its last handle closes.
func (f *fileWriter) flushRangeEpoch(ctx meta.Context, off, size uint64) syscall.Errno {
	if size == 0 {
		return 0
	}
	start := time.Now()
	end := off + size
	f.Lock()
	defer f.Unlock()
	if f.err != 0 {
		return f.err
	}

	var target *epoch
	for indx := off / meta.ChunkSize; indx <= (end-1)/meta.ChunkSize; indx++ {
		c := f.chunks[uint32(indx)]
		if c == nil {
			continue
		}
		base := indx * meta.ChunkSize
		for i := len(c.slices) - 1; i >= 0; i-- { // newest first: the first overlap is the chunk's newest
			s := c.slices[i]
			if soff := base + uint64(s.off); soff < end && off < soff+uint64(s.slen) {
				if target == nil || s.ep.id > target.id {
					target = s.ep
				}
				break
			}
		}
	}
	if target == nil {
		return 0
	}
	if target == f.open {
		f.closeEpoch("read")
	}

	var err syscall.Errno
	wait := f.flushTimeout()
	deadline := start.Add(wait)
	f.readwaiting++
	for !target.committed && f.err == 0 && err == 0 {
		if f.readcond.WaitWithTimeout(time.Second*3) && ctx.Canceled() && time.Since(start) > f.w.conf.Chunk.PutTimeout*2 {
			logger.Warnf("flush range %d (%d,%d) interrupted after %s", f.inode, off, size, time.Since(start))
			err = syscall.EINTR
		} else if time.Now().After(deadline) {
			logger.Errorf("flush range %d (%d,%d) timeout after waited %s", f.inode, off, size, wait)
			err = syscall.EIO
		}
	}
	f.readwaiting--
	writerReadEpochWait.Observe(time.Since(start).Seconds())
	if err == 0 {
		err = f.err
	}
	return err
}

// flushEpochsInBackground is the background flusher's pass over a file in epoch mode. It closes the open epoch when it
// is epochMaxAge old, counted from its first slice (not from its oldest unfrozen one, so auto-frozen slices cannot keep
// it open without bound), when the file had no write for a second, or when the file has more than epochBufferedSlices
// slices not uploaded. Within the open epoch it freezes each slice idle for a second, as chunk mode does: its upload
// starts early, and it still commits with its epoch.
func (f *fileWriter) flushEpochsInBackground(now time.Time) {
	f.Lock()
	defer f.Unlock()
	if e := f.open; len(e.slices) > 0 {
		switch {
		case now.Sub(e.started) >= f.w.epochMaxAge:
			f.closeEpoch("age")
		case now.Sub(e.lastWrite) >= epochIdle:
			f.closeEpoch("idle")
		case f.pendingCount-f.donePending > epochBufferedSlices && e.hasUnfrozen():
			f.closeEpoch("cap")
		}
	}
	for _, s := range f.open.slices {
		if !s.freezed && now.Sub(s.lastWrite) > time.Second && now.Sub(s.started) > time.Second {
			s.freezed = true
			go s.flushData()
		}
	}
}
