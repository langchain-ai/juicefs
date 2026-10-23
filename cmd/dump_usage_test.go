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

package cmd

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

type usageFileTestMeta struct {
	meta.Meta
	err error
}

func (m usageFileTestMeta) DumpUsage(ctx meta.Context, w io.Writer, opt *meta.UsageDumpOption) error {
	if _, err := io.WriteString(w, "complete usage export\n"); err != nil {
		return err
	}
	return m.err
}

func TestDumpUsageFileAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "usage.ndjson.gz")
	if err := os.WriteFile(dst, []byte("previous export"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := dumpUsageFile(meta.Background(), usageFileTestMeta{err: io.ErrUnexpectedEOF}, dst, 2); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("failed dump: %v", err)
	}
	if data, err := os.ReadFile(dst); err != nil || string(data) != "previous export" {
		t.Fatalf("failed export replaced existing file: %q %v", data, err)
	}
	if err := dumpUsageFile(meta.Background(), usageFileTestMeta{}, dst, 2); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil || string(data) != "complete usage export\n" {
		t.Fatalf("invalid gzip: %q %v", data, err)
	}
	info, err := os.Stat(dst)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("export permissions: %v %v", info, err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("temporary files retained: %v %v", files, err)
	}
}
