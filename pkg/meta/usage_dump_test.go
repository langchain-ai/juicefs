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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

const usageTestUUID = "6fded32e-4e2b-4eec-9dbe-8fb4f14d218d"

func newUsageTestMeta(t *testing.T, driver string) Meta {
	t.Helper()
	conf := testConfig()
	conf.NoBGJob = true
	var m Meta
	var err error
	switch driver {
	case "memkv":
		m, err = newKVMeta(driver, "usage-export-test", conf)
	case "sqlite3":
		m, err = newSQLMeta(driver, filepath.Join(t.TempDir(), "usage.db"), conf)
	case "redis":
		m, err = newRedisMeta(driver, "127.0.0.1:6379/11", conf)
	default:
		t.Fatal("unexpected engine")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	if err := m.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := m.Init(&Format{Name: "usage-test", UUID: usageTestUUID, TrashDays: 1}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Load(true); err != nil {
		t.Fatal(err)
	}
	return m
}

type usageTestRecord struct {
	Type  string `json:"type"`
	Path  string `json:"path,omitempty"`
	Inode Ino    `json:"inode,omitempty"`
	Kind  string `json:"kind,omitempty"`
	ID    uint64 `json:"id,omitempty"`
	Size  uint32 `json:"size,omitempty"`
}

func readUsageTestRecords(t *testing.T, data []byte) []usageTestRecord {
	t.Helper()
	lines := bytes.SplitAfter(data, []byte{'\n'})
	if len(lines) < 3 || len(lines[len(lines)-1]) != 0 {
		t.Fatal("missing final newline")
	}
	lines = lines[:len(lines)-1]
	var header usageHeader
	if err := json.Unmarshal(lines[0], &header); err != nil {
		t.Fatal(err)
	}
	if header.Type != "header" || header.Version != 1 || header.Format != "juicefs-usage" || header.SourceID != usageTestUUID || header.StartedAt.IsZero() {
		t.Fatalf("bad usage header: %+v", header)
	}
	hash := sha256.New()
	var records []usageTestRecord
	var entries, slices uint64
	for i, line := range lines[:len(lines)-1] {
		if len(line) > MaxUsageRecordSize {
			t.Fatal("oversized record")
		}
		_, _ = hash.Write(line)
		if i == 0 {
			continue
		}
		var record usageTestRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		switch record.Type {
		case "entry":
			entries++
		case "slice":
			slices++
		default:
			t.Fatalf("unexpected record type %q", record.Type)
		}
		records = append(records, record)
	}
	var end usageEnd
	if err := json.Unmarshal(lines[len(lines)-1], &end); err != nil {
		t.Fatal(err)
	}
	if end.Type != "end" || end.Entries != entries || end.Slices != slices || end.SHA256 != hex.EncodeToString(hash.Sum(nil)) || end.CompletedAt.Before(header.StartedAt) {
		t.Fatalf("invalid completion record: %+v", end)
	}
	return records
}

func testUsageDump(t *testing.T, m Meta) {
	t.Helper()
	ctx := Background()
	var dir, file, ignored Ino
	var attr Attr
	if st := m.Mkdir(ctx, RootInode, "a", 0755, 0, 0, &dir, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Mkdir(ctx, RootInode, "empty", 0755, 0, 0, &ignored, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Create(ctx, RootInode, "empty-file", 0644, 0, 0, &ignored, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Create(ctx, RootInode, "truncated-file", 0644, 0, 0, &ignored, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Write(ctx, ignored, 0, 0, Slice{Id: 88, Size: 128, Len: 128}, time.Now()); st != 0 {
		t.Fatal(st)
	}
	if st := m.Truncate(ctx, ignored, 0, 0, &attr, false); st != 0 {
		t.Fatal(st)
	}
	name := "file %20 \xff\"\n"
	if st := m.Create(ctx, dir, name, 0644, 0, 0, &file, &attr); st != 0 {
		t.Fatal(st)
	}
	for _, write := range []struct {
		index uint32
		id    uint64
		size  uint32
	}{{0, 9007199254740993, 4096}, {0, 9007199254740994, 4096}, {2, 17, 1024}} {
		if st := m.Write(ctx, file, write.index, 0, Slice{Id: write.id, Size: write.size, Len: write.size}, time.Now()); st != 0 {
			t.Fatal(st)
		}
	}
	if st := m.Link(ctx, file, RootInode, "z-hardlink", &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Symlink(ctx, RootInode, "link", "target-must-not-be-exported", &ignored, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.SetXattr(ctx, RootInode, "test-annotation", []byte("value-must-not-be-exported"), 0); st != 0 {
		t.Fatal(st)
	}
	if st := m.Create(ctx, RootInode, "removed", 0644, 0, 0, &ignored, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Write(ctx, ignored, 0, 0, Slice{Id: 99, Size: 128, Len: 128}, time.Now()); st != 0 {
		t.Fatal(st)
	}
	if st := m.Close(ctx, ignored); st != 0 {
		t.Fatal(st)
	}
	if st := m.Unlink(ctx, RootInode, "removed"); st != 0 {
		t.Fatal(st)
	}
	var before Attr
	if st := m.GetAttr(ctx, file, &before); st != 0 {
		t.Fatal(st)
	}
	var output bytes.Buffer
	if err := m.DumpUsage(ctx, &output, nil); err != nil {
		t.Fatal(err)
	}
	records := readUsageTestRecords(t, output.Bytes())
	for _, forbidden := range []string{"target-must-not-be-exported", "value-must-not-be-exported", "/removed", "\"id\":99", "\"id\":88", "\"Trash\""} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("export includes excluded metadata %q", forbidden)
		}
	}
	var after Attr
	if st := m.GetAttr(ctx, file, &after); st != 0 {
		t.Fatal(st)
	}
	if before.Atime != after.Atime || before.Atimensec != after.Atimensec {
		t.Fatal("export changed file access time")
	}

	var recovery bytes.Buffer
	if err := m.DumpMeta(&recovery, 0, 1, false, false, false); err != nil {
		t.Fatal(err)
	}
	var full DumpedMeta
	if err := json.Unmarshal(recovery.Bytes(), &full); err != nil {
		t.Fatal(err)
	}
	if full.Trash == nil {
		t.Fatal("recovery export lost Trash")
	}
	var expected []usageTestRecord
	var visit func(string, *DumpedEntry)
	visit = func(path string, entry *DumpedEntry) {
		expected = append(expected, usageTestRecord{Type: "entry", Path: path, Inode: entry.Attr.Inode, Kind: entry.Attr.Type})
		for _, chunk := range entry.Chunks {
			for _, slice := range chunk.Slices {
				if slice.Id != 0 {
					expected = append(expected, usageTestRecord{Type: "slice", ID: slice.Id, Size: slice.Size})
				}
			}
		}
		var names []string
		for name := range entry.Entries {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool { return bytes.Compare(unescape(names[i]), unescape(names[j])) < 0 })
		for _, name := range names {
			visit(strings.TrimSuffix(path, "/")+"/"+name, entry.Entries[name])
		}
	}
	visit("/", full.FSTree)
	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("usage references differ from recovery tree:\nactual: %+v\nexpected: %+v", records, expected)
	}
	if st := m.Chroot(ctx, "a"); st != 0 {
		t.Fatal(st)
	}
	output.Reset()
	if err := m.DumpUsage(ctx, &output, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readUsageTestRecords(t, output.Bytes()), expected) {
		t.Fatal("subdirectory mount changed full-volume export")
	}
}

func TestUsageDumpMemKV(t *testing.T)  { testUsageDump(t, newUsageTestMeta(t, "memkv")) }
func TestUsageDumpSQLite(t *testing.T) { testUsageDump(t, newUsageTestMeta(t, "sqlite3")) }
func TestUsageDumpRedis(t *testing.T)  { testUsageDump(t, newUsageTestMeta(t, "redis")) }

type usageFailWriter struct{}

func (usageFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type usageReadFault struct {
	engine
	getAttr func(Context, Ino, *Attr) syscall.Errno
	read    func(Context, Ino, uint32) ([]*slice, syscall.Errno)
}

func (e usageReadFault) doGetAttr(ctx Context, inode Ino, attr *Attr) syscall.Errno {
	if e.getAttr != nil {
		return e.getAttr(ctx, inode, attr)
	}
	return e.engine.doGetAttr(ctx, inode, attr)
}

func (e usageReadFault) doRead(ctx Context, inode Ino, index uint32) ([]*slice, syscall.Errno) {
	return e.read(ctx, inode, index)
}

func TestUsageDumpFailures(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	ctx := Background()
	ctx.Cancel()
	var output bytes.Buffer
	if err := m.DumpUsage(ctx, &output, nil); !errors.Is(err, context.Canceled) || output.Len() != 0 {
		t.Fatalf("canceled export: %v, output length %d", err, output.Len())
	}
	if err := m.DumpUsage(Background(), usageFailWriter{}, nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer failure: %v", err)
	}
	base := m.getBase()
	original := base.en
	base.en = usageReadFault{engine: original, getAttr: func(ctx Context, inode Ino, attr *Attr) syscall.Errno {
		st := original.doGetAttr(ctx, inode, attr)
		ctx.Cancel()
		return st
	}}
	output.Reset()
	if err := m.DumpUsage(Background(), &output, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-export cancellation: %v", err)
	}
	if bytes.Contains(output.Bytes(), []byte(`"type":"end"`)) {
		t.Fatal("canceled export has a completion record")
	}
	base.en = original
	for _, opt := range []*UsageDumpOption{{Threads: -1}, {Threads: 65}, {ExportID: "invalid"}, {ExportID: "00000000-0000-0000-0000-000000000000"}} {
		if err := m.DumpUsage(Background(), io.Discard, opt); err == nil {
			t.Fatalf("accepted invalid options %+v", opt)
		}
	}
}

func TestUsageDumpZeroSizeAndCorruptSlices(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "file", 0644, 0, 0, &inode, &attr); st != 0 {
		t.Fatal(st)
	}
	if st := m.Write(ctx, inode, 0, 0, Slice{Id: 42, Size: 100, Len: 100}, time.Now()); st != 0 {
		t.Fatal(st)
	}
	base := m.getBase()
	original := base.en
	base.en = usageReadFault{engine: original, read: func(Context, Ino, uint32) ([]*slice, syscall.Errno) {
		return []*slice{{id: 0}, {id: 42, size: 0}, {id: 42, size: 100}}, 0
	}}
	var output bytes.Buffer
	if err := m.DumpUsage(ctx, &output, nil); err != nil {
		t.Fatal(err)
	}
	records := readUsageTestRecords(t, output.Bytes())
	expected := []usageTestRecord{{Type: "slice", ID: 42, Size: 0}, {Type: "slice", ID: 42, Size: 100}}
	if !reflect.DeepEqual(records[len(records)-2:], expected) {
		t.Fatalf("zero-sized first observation not preserved: %+v", records)
	}
	base.en = usageReadFault{engine: original, read: func(Context, Ino, uint32) ([]*slice, syscall.Errno) { return nil, 0 }}
	output.Reset()
	if err := m.DumpUsage(ctx, &output, nil); err == nil || bytes.Contains(output.Bytes(), []byte(`"type":"end"`)) {
		t.Fatalf("corrupt slices accepted: %v", err)
	}
}

func TestUsageDumpEscapedPathLimit(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	ctx := Background()
	parent := RootInode
	for range 90 {
		var child Ino
		var attr Attr
		if st := m.Mkdir(ctx, parent, strings.Repeat("%", 255), 0755, 0, 0, &child, &attr); st != 0 {
			t.Fatal(st)
		}
		parent = child
	}
	var output bytes.Buffer
	if err := m.DumpUsage(ctx, &output, nil); err == nil {
		t.Fatal("accepted an oversized escaped path")
	}
	if bytes.Contains(output.Bytes(), []byte(`"type":"end"`)) {
		t.Fatal("over-limit export has a completion record")
	}
}

func TestUsageDumpIdentity(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	format := m.GetFormat()
	format.UUID = strings.ToUpper(usageTestUUID)
	m.getBase().Lock()
	m.getBase().fmt = &format
	m.getBase().Unlock()
	var output bytes.Buffer
	if err := m.DumpUsage(Background(), &output, &UsageDumpOption{ExportID: strings.ReplaceAll(usageTestUUID, "-", "")}); err != nil {
		t.Fatal(err)
	}
	readUsageTestRecords(t, output.Bytes())
	var header usageHeader
	if err := json.NewDecoder(&output).Decode(&header); err != nil {
		t.Fatal(err)
	}
	if header.SourceID != usageTestUUID || header.ExportID != usageTestUUID {
		t.Fatalf("export identity is not canonical: %+v", header)
	}
	for _, id := range []string{"", "invalid", "00000000-0000-0000-0000-000000000000"} {
		format := m.GetFormat()
		format.UUID = id
		m.getBase().Lock()
		m.getBase().fmt = &format
		m.getBase().Unlock()
		output.Reset()
		if err := m.DumpUsage(Background(), &output, nil); err == nil || output.Len() != 0 {
			t.Fatalf("accepted invalid volume identity %q: %v", id, err)
		}
	}
}

func TestUsageDumpSparseFileOrder(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "sparse", 0644, 0, 0, &inode, &attr); st != 0 {
		t.Fatal(st)
	}
	for _, write := range []struct {
		index uint32
		id    uint64
	}{{65, 3}, {0, 1}, {33, 2}} {
		if st := m.Write(ctx, inode, write.index, 0, Slice{Id: write.id, Size: 1024, Len: 1024}, time.Now()); st != 0 {
			t.Fatal(st)
		}
	}
	var output bytes.Buffer
	if err := m.DumpUsage(ctx, &output, &UsageDumpOption{Threads: 4}); err != nil {
		t.Fatal(err)
	}
	records := readUsageTestRecords(t, output.Bytes())
	expected := []usageTestRecord{
		{Type: "entry", Path: "/", Inode: RootInode, Kind: "directory"},
		{Type: "entry", Path: "/sparse", Inode: inode, Kind: "regular"},
		{Type: "slice", ID: 1, Size: 1024},
		{Type: "slice", ID: 2, Size: 1024},
		{Type: "slice", ID: 3, Size: 1024},
	}
	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("sparse file references out of order: %+v", records)
	}
}
