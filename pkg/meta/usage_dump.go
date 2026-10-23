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
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// MaxUsageRecordSize includes the terminating newline of a usage record.
const MaxUsageRecordSize = 1 << 20

const (
	maxUsagePath  = 64 << 10
	maxUsageDepth = 1024
)

// UsageDumpOption identifies one complete active-tree traversal.
type UsageDumpOption struct {
	ExportID  string
	StartedAt time.Time
	Threads   int
}

type usageHeader struct {
	Type      string    `json:"type"`
	Format    string    `json:"format"`
	Version   int       `json:"version"`
	SourceID  string    `json:"source_id"`
	ExportID  string    `json:"export_id"`
	StartedAt time.Time `json:"started_at"`
}

type usageEntry struct {
	Type  string `json:"type"`
	Path  string `json:"path"`
	Inode Ino    `json:"inode"`
	Kind  string `json:"kind"`
}

type usageSlice struct {
	Type string `json:"type"`
	ID   uint64 `json:"id"`
	Size uint32 `json:"size"`
}

type usageEnd struct {
	Type        string    `json:"type"`
	Entries     uint64    `json:"entries"`
	Slices      uint64    `json:"slices"`
	SHA256      string    `json:"sha256"`
	CompletedAt time.Time `json:"completed_at"`
}

type usageWriter struct {
	w       *bufio.Writer
	hash    hash.Hash
	entries uint64
	slices  uint64
}

func (w *usageWriter) record(v interface{}, checksum bool) error {
	line, err := json.Marshal(v)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if len(line) > MaxUsageRecordSize {
		return fmt.Errorf("usage record exceeds %d bytes", MaxUsageRecordSize)
	}
	if _, err = w.w.Write(line); err != nil {
		return err
	}
	if checksum {
		_, _ = w.hash.Write(line)
	}
	return nil
}

func (m *baseMeta) DumpUsage(ctx Context, dst io.Writer, opt *UsageDumpOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var options UsageDumpOption
	if opt != nil {
		options = *opt
	}
	if options.ExportID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		options.ExportID = id.String()
	}
	exportID, err := uuid.Parse(options.ExportID)
	if err != nil {
		return fmt.Errorf("invalid usage export ID: %w", err)
	}
	if exportID == uuid.Nil {
		return fmt.Errorf("usage export ID must not be nil")
	}
	format := m.getFormat()
	if format == nil {
		return fmt.Errorf("volume format must be loaded before usage export")
	}
	sourceID, err := uuid.Parse(format.UUID)
	if err != nil {
		return fmt.Errorf("invalid volume UUID: %w", err)
	}
	if sourceID == uuid.Nil {
		return fmt.Errorf("volume UUID must not be nil")
	}
	if options.StartedAt.IsZero() {
		options.StartedAt = time.Now()
	}
	if options.Threads == 0 {
		options.Threads = 2
	}
	if options.Threads < 1 || options.Threads > 64 {
		return fmt.Errorf("usage export threads must be between 1 and 64")
	}
	w := &usageWriter{w: bufio.NewWriterSize(dst, 64<<10), hash: sha256.New()}
	if err := w.record(usageHeader{
		Type: "header", Format: "juicefs-usage", Version: 1,
		SourceID: sourceID.String(), ExportID: exportID.String(), StartedAt: options.StartedAt.UTC(),
	}, true); err != nil {
		return err
	}
	if err := m.dumpUsageEntry(ctx, w, RootInode, "/", nil, make(map[Ino]bool), options.Threads); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.record(usageEnd{
		Type: "end", Entries: w.entries, Slices: w.slices,
		SHA256: hex.EncodeToString(w.hash.Sum(nil)), CompletedAt: time.Now().UTC(),
	}, false); err != nil {
		return err
	}
	return errors.Join(w.w.Flush(), ctx.Err())
}

// usageReadBatch keeps metadata requests concurrent without retaining a whole
// file's slice lists. Results are consumed in their original order.
func usageReadBatch(ctx Context, count, threads int, read func(int) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	errs := make([]error, count)
	var wg sync.WaitGroup
	for worker := 0; worker < min(count, threads); worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < count; i += threads {
				if err := ctx.Err(); err != nil {
					errs[i] = err
					return
				}
				errs[i] = read(i)
				if errs[i] != nil {
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (m *baseMeta) dumpUsageEntry(ctx Context, w *usageWriter, inode Ino, path string, attr *Attr, ancestors map[Ino]bool, threads int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encodedPath := escape(path)
	if len(encodedPath) > maxUsagePath || len(ancestors) > maxUsageDepth {
		return fmt.Errorf("usage export exceeds path or directory depth limit")
	}
	if attr == nil {
		attr = &Attr{}
		if st := m.en.doGetAttr(ctx, inode, attr); st != 0 {
			if st == syscall.ENOENT && inode != RootInode {
				return nil // Removed during the live traversal.
			}
			return fmt.Errorf("usage getattr inode %d: %w", inode, st)
		}
	}
	if attr.Typ < TypeFile || attr.Typ > TypeSocket {
		return fmt.Errorf("invalid inode type %d", attr.Typ)
	}
	if err := w.record(usageEntry{Type: "entry", Path: encodedPath, Inode: inode, Kind: typeToString(attr.Typ)}, true); err != nil {
		return err
	}
	w.entries++
	if attr.Typ == TypeFile {
		return m.dumpUsageSlices(ctx, w, inode, attr.Length, threads)
	}
	if attr.Typ != TypeDirectory {
		return nil
	}
	if ancestors[inode] {
		return fmt.Errorf("directory cycle at inode %d", inode)
	}
	ancestors[inode] = true
	defer delete(ancestors, inode)
	var entries []*Entry
	if st := m.en.doReaddir(ctx, inode, 0, &entries, -1); st != 0 {
		if st == syscall.ENOENT && inode != RootInode {
			return nil
		}
		return fmt.Errorf("usage readdir inode %d: %w", inode, st)
	}
	sort.SliceStable(entries, func(i, j int) bool { return bytes.Compare(entries[i].Name, entries[j].Name) < 0 })
	// Redis scans may repeat entries while the directory changes. As in a
	// recovery dump's entry map, keep the last observed value for each name.
	unique := entries[:0]
	for _, entry := range entries {
		if len(unique) > 0 && bytes.Equal(unique[len(unique)-1].Name, entry.Name) {
			unique[len(unique)-1] = entry
		} else {
			unique = append(unique, entry)
		}
	}
	clear(entries[len(unique):])
	entries = unique
	for offset := 0; offset < len(entries); {
		batch := entries[offset:min(offset+threads*16, len(entries))]
		attrs := make([]Attr, len(batch))
		if err := usageReadBatch(ctx, len(batch), threads, func(i int) error {
			if batch[i].Inode >= TrashInode {
				return nil
			}
			st := m.en.doGetAttr(ctx, batch[i].Inode, &attrs[i])
			if st == syscall.ENOENT {
				return nil
			}
			if st != 0 {
				return fmt.Errorf("usage getattr inode %d: %w", batch[i].Inode, st)
			}
			return nil
		}); err != nil {
			return err
		}
		for i, entry := range batch {
			if entry.Inode >= TrashInode || attrs[i].Typ == 0 {
				continue
			}
			name := string(entry.Name)
			if checkInodeName(name) != 0 || name == "." || name == ".." {
				return fmt.Errorf("invalid directory entry in usage export")
			}
			childPath := path
			if path != "/" {
				childPath += "/"
			}
			childPath += name
			if err := m.dumpUsageEntry(ctx, w, entry.Inode, childPath, &attrs[i], ancestors, threads); err != nil {
				return err
			}
		}
		offset += len(batch)
	}
	return nil
}

func (m *baseMeta) dumpUsageSlices(ctx Context, w *usageWriter, inode Ino, length uint64, threads int) error {
	if length > uint64(1<<32)*ChunkSize {
		return fmt.Errorf("invalid file length in usage export")
	}
	chunks := (length + ChunkSize - 1) / ChunkSize
	for index := uint64(0); index < chunks; {
		count := min(uint64(threads*16), chunks-index)
		batch := make([][]*slice, count)
		if err := usageReadBatch(ctx, int(count), threads, func(i int) error {
			ss, st := m.en.doRead(ctx, inode, uint32(index+uint64(i)))
			if st != 0 {
				return fmt.Errorf("usage read inode %d chunk %d: %w", inode, index+uint64(i), st)
			}
			if ss == nil {
				return fmt.Errorf("corrupt slice list in usage export")
			}
			batch[i] = ss
			return nil
		}); err != nil {
			return err
		}
		for _, ss := range batch {
			for _, s := range ss {
				if err := ctx.Err(); err != nil {
					return err
				}
				if s.id == 0 {
					continue // Hole records do not refer to an object.
				}
				if err := w.record(usageSlice{Type: "slice", ID: s.id, Size: s.size}, true); err != nil {
					return err
				}
				w.slices++
			}
		}
		index += count
	}
	return nil
}
