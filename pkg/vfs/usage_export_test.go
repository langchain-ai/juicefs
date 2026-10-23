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

package vfs

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

type usageDumpTestMeta struct {
	meta.Meta
	dump func(meta.Context, io.Writer, *meta.UsageDumpOption) error
}

func (m usageDumpTestMeta) DumpUsage(ctx meta.Context, w io.Writer, opt *meta.UsageDumpOption) error {
	return m.dump(ctx, w, opt)
}

type usageUploadFailure struct{ object.ObjectStorage }

func (s usageUploadFailure) Put(ctx context.Context, key string, r io.Reader, getters ...object.AttrGetter) error {
	if err := s.ObjectStorage.Put(ctx, key, strings.NewReader("partial")); err != nil {
		return err
	}
	return io.ErrClosedPipe
}

func TestUsageExportPublication(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	blob, err := object.CreateStorage("mem", "usage-test", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	const content = "{\"type\":\"header\"}\n{\"type\":\"end\"}\n"
	m := usageDumpTestMeta{dump: func(ctx meta.Context, w io.Writer, opt *meta.UsageDumpOption) error {
		if opt.ExportID == "" || opt.StartedAt.IsZero() {
			t.Fatal("missing export identity")
		}
		_, err := io.WriteString(w, content)
		return err
	}}
	key, err := exportUsage(meta.Background(), m, blob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := usageExportTime(key); err != nil {
		t.Fatalf("invalid generated key: %v", err)
	}
	r, err := blob.Get(t.Context(), key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	gr, err := gzip.NewReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()
	data, err := io.ReadAll(gr)
	if err != nil || string(data) != content {
		t.Fatalf("invalid completed gzip: %q %v", data, err)
	}
	if files, _ := filepath.Glob(filepath.Join(tmp, "juicefs-usage-*")); len(files) != 0 {
		t.Fatalf("temporary files retained: %v", files)
	}

	failed := usageDumpTestMeta{dump: func(meta.Context, io.Writer, *meta.UsageDumpOption) error { return io.ErrUnexpectedEOF }}
	if _, err := exportUsage(meta.Background(), failed, blob); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("dump failure: %v", err)
	}
	if _, err := exportUsage(meta.Background(), m, usageUploadFailure{blob}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("upload failure: %v", err)
	}
	objects, err := object.ListAll(t.Context(), blob, "usage/", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for obj := range objects {
		if obj == nil {
			t.Fatal("list failed")
		}
		if !obj.IsDir() {
			keys = append(keys, obj.Key())
		}
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("failed or partial export published: %v", keys)
	}
	if files, _ := filepath.Glob(filepath.Join(tmp, "juicefs-usage-*")); len(files) != 0 {
		t.Fatalf("failed export retained temporary files: %v", files)
	}
}

func TestUsageExportCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ExportUsage(ctx, nil, nil, time.Hour)
	ExportUsage(t.Context(), nil, nil, 0)

	f, err := os.CreateTemp(t.TempDir(), "context-read")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := &usageReadSeeker{ctx: ctx, file: f}
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("read ignored cancellation: %v", err)
	}
	if _, err := r.Seek(0, io.SeekStart); !errors.Is(err, context.Canceled) {
		t.Fatalf("seek ignored cancellation: %v", err)
	}
}

func TestUsageExportRetention(t *testing.T) {
	root := t.TempDir()
	blob, err := object.CreateStorage("file", root+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-72 * time.Hour)
	name := func(ts time.Time, id string) string {
		return "usage/usage-" + ts.Format(usageTimeFormat) + "-" + id + ".ndjson.gz"
	}
	oldKey := name(old, "00000000-0000-4000-8000-000000000001")
	recentKey := name(now, "00000000-0000-4000-8000-000000000002")
	lateKey := name(old, "00000000-0000-4000-8000-000000000003")
	invalidKey := "usage/usage-old-unrelated.ndjson.gz"
	backupKey := "meta/dump-" + old.Format("2006-01-02-150405") + ".json.gz"
	for _, key := range []string{oldKey, recentKey, lateKey, invalidKey, backupKey} {
		if err := blob.Put(t.Context(), key, bytes.NewReader(nil)); err != nil {
			t.Fatal(err)
		}
		if key != lateKey && key != recentKey {
			if err := os.Chtimes(filepath.Join(root, key), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := cleanupUsageExports(t.Context(), blob, now); err != nil {
		t.Fatal(err)
	}
	if _, err := blob.Head(t.Context(), oldKey); !os.IsNotExist(err) {
		t.Fatalf("expired usage export was not deleted: %v", err)
	}
	for _, key := range []string{recentKey, lateKey, invalidKey, backupKey} {
		if _, err := blob.Head(t.Context(), key); err != nil {
			t.Fatalf("retention deleted protected object %q: %v", key, err)
		}
	}
}
