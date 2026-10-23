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
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

func cmdDumpUsage() *cli.Command {
	return &cli.Command{
		Name:      "dump-usage",
		Action:    dumpUsage,
		Category:  "ADMIN",
		Usage:     "Export active-tree paths and physical slice references as NDJSON",
		ArgsUsage: "META-URL [FILE]",
		Description: `
Export the active volume tree for physical storage attribution. Each path and
slice is a separate record. Trash, detached files, credentials, extended
attributes and other recovery metadata are excluded. A checksum and record
counts terminate successful exports. This format cannot be used by juicefs load.

Write to STDOUT when FILE is omitted; a .gz suffix enables gzip compression.

Examples:
$ juicefs dump-usage redis://localhost usage.ndjson.gz
$ juicefs dump-usage sqlite3://meta.db usage.ndjson`,
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "threads", Value: 2, Usage: "concurrent metadata reads (1-64)"},
		},
	}
}

func dumpUsage(c *cli.Context) error {
	setup0(c, 1, 2)
	metaURI := c.Args().Get(0)
	removePassword(metaURI)
	conf := meta.DefaultConf()
	conf.ReadOnly = true
	conf.NoBGJob = true
	m := meta.NewClient(metaURI, conf)
	defer func() { _ = m.Shutdown() }()
	if _, err := m.Load(true); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	mctx := meta.WrapContext(ctx)
	defer mctx.Cancel()
	return dumpUsageFile(mctx, m, c.Args().Get(1), c.Int("threads"))
}

func dumpUsageFile(ctx meta.Context, m meta.Meta, dst string, threads int) error {
	opt := &meta.UsageDumpOption{Threads: threads}
	if dst == "" {
		return m.DumpUsage(ctx, os.Stdout, opt)
	}
	f, err := os.CreateTemp(filepath.Dir(dst), ".juicefs-usage-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	var w io.Writer = f
	var zw *gzip.Writer
	if strings.HasSuffix(dst, ".gz") {
		zw, err = gzip.NewWriterLevel(f, gzip.BestSpeed)
		if err != nil {
			return err
		}
		w = zw
	}
	err = m.DumpUsage(ctx, w, opt)
	if zw != nil {
		err = errors.Join(err, zw.Close())
	}
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(f.Name(), dst)
}
