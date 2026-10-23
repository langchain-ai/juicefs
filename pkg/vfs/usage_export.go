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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

const usageTimeFormat = "2006-01-02-150405.000000000"

// ExportUsage periodically publishes compact active-tree usage independently of
// recovery backups. An interval of zero disables the task.
func ExportUsage(ctx context.Context, m meta.Meta, blob object.ObjectStorage, interval time.Duration) {
	if interval <= 0 {
		return
	}
	mctx := meta.WrapContext(ctx)
	defer mctx.Cancel()
	for {
		timer := time.NewTimer(utils.JitterIt(min(interval/10, time.Minute)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		ran, err := meta.RunUsageExport(mctx, m, interval, func(ctx meta.Context) error {
			key, err := exportUsage(ctx, m, blob)
			if err == nil {
				logger.Infof("usage export completed: %s", key)
			}
			return err
		})
		if err != nil {
			logger.Warnf("usage export failed: %s", err)
		} else if ran {
			if err := cleanupUsageExports(ctx, blob, time.Now()); err != nil {
				logger.Warnf("cleanup usage exports: %s", err)
			}
		}
	}
}

type usageReadSeeker struct {
	ctx  context.Context
	file *os.File
}

func (r *usageReadSeeker) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.file.Read(p)
}

func (r *usageReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.file.Seek(offset, whence)
}

func exportUsage(ctx meta.Context, m meta.Meta, blob object.ObjectStorage) (string, error) {
	started := time.Now().UTC()
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	key := "usage/usage-" + started.Format(usageTimeFormat) + "-" + id.String() + ".ndjson.gz"
	f, err := os.CreateTemp("", "juicefs-usage-*.ndjson.gz")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	zw, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		return "", err
	}
	err = m.DumpUsage(ctx, zw, &meta.UsageDumpOption{ExportID: id.String(), StartedAt: started})
	err = errors.Join(err, zw.Close())
	if err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	readFile, err := os.Open(f.Name())
	if err != nil {
		return "", err
	}
	defer readFile.Close()
	if err = ctx.Err(); err != nil {
		return "", err
	}
	err = blob.Put(ctx, key, &usageReadSeeker{ctx: ctx, file: readFile})
	err = errors.Join(err, ctx.Err())
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return "", errors.Join(err, blob.Delete(cleanupCtx, key))
	}
	return key, nil
}

func usageExportTime(key string) (time.Time, error) {
	name := strings.TrimPrefix(key, "usage/usage-")
	if name == key || !strings.HasSuffix(name, ".ndjson.gz") {
		return time.Time{}, fmt.Errorf("not a usage export")
	}
	name = strings.TrimSuffix(name, ".ndjson.gz")
	if len(name) != len(usageTimeFormat)+1+36 || name[len(usageTimeFormat)] != '-' {
		return time.Time{}, fmt.Errorf("invalid usage export name")
	}
	if _, err := uuid.Parse(name[len(usageTimeFormat)+1:]); err != nil {
		return time.Time{}, err
	}
	return time.Parse(usageTimeFormat, name[:len(usageTimeFormat)])
}

func cleanupUsageExports(ctx context.Context, blob object.ObjectStorage, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	objects, err := object.ListAll(ctx, blob, "usage/usage-", "", false, false)
	if err != nil {
		return err
	}
	deleted := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case obj, ok := <-objects:
			if !ok {
				return nil
			}
			if obj == nil {
				return fmt.Errorf("list usage exports failed")
			}
			ts, err := usageExportTime(obj.Key())
			if err != nil || obj.IsDir() || !ts.Before(now.Add(-48*time.Hour)) || obj.Mtime().After(now.Add(-48*time.Hour)) {
				continue
			}
			if err := blob.Delete(ctx, obj.Key()); err != nil {
				return err
			}
			deleted++
			if deleted >= 1000 {
				return nil
			}
		}
	}
}
