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
	"context"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type usageLeaseTestEngine struct {
	engine
	set func(string, int64, int64) (bool, error)
}

func (e usageLeaseTestEngine) setIfSmall(name string, value, diff int64) (bool, error) {
	return e.set(name, value, diff)
}

func testUsageExportSchedule(t *testing.T, m Meta) {
	t.Helper()
	ctx := Background()
	if st := m.SetXattr(ctx, 0, "lastBackup", []byte(time.Now().UTC().Format(time.RFC3339)), 0); st != 0 {
		t.Fatal(st)
	}
	if _, err := m.getBase().en.incrCounter(totalInodes, 2_000_000); err != nil {
		t.Fatal(err)
	}
	calls := 0
	export := func(Context) error { calls++; return nil }
	if ran, err := RunUsageExport(ctx, m, 0, export); err != nil || ran || calls != 0 {
		t.Fatalf("disabled export ran: %v %v", ran, err)
	}
	if ran, err := RunUsageExport(ctx, m, time.Hour, export); err != nil || !ran || calls != 1 {
		t.Fatalf("independent high-inode export did not run: %v %v", ran, err)
	}
	if ran, err := RunUsageExport(ctx, m, time.Hour, export); err != nil || ran || calls != 1 {
		t.Fatalf("completed export was repeated: %v %v", ran, err)
	}
	m.getBase().conf.ReadOnly = true
	if ran, err := RunUsageExport(ctx, m, time.Hour, export); !errors.Is(err, syscall.EROFS) || ran || calls != 1 {
		t.Fatalf("read-only export was accepted: %v %v", ran, err)
	}
}

func TestUsageExportScheduleMemKV(t *testing.T) {
	testUsageExportSchedule(t, newUsageTestMeta(t, "memkv"))
}
func TestUsageExportScheduleSQLite(t *testing.T) {
	testUsageExportSchedule(t, newUsageTestMeta(t, "sqlite3"))
}
func TestUsageExportScheduleRedis(t *testing.T) {
	testUsageExportSchedule(t, newUsageTestMeta(t, "redis"))
}

func TestUsageExportRenewsLongScan(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	base := m.getBase()
	original := base.en
	var calls atomic.Int64
	renewals := make(chan struct{}, 16)
	base.en = usageLeaseTestEngine{engine: original, set: func(name string, value, diff int64) (bool, error) {
		changed, err := original.setIfSmall(name, value, diff)
		if name == usageExportLeaseKey && calls.Add(1) > 1 && changed && err == nil {
			renewals <- struct{}{}
		}
		return changed, err
	}}
	const lease = 150 * time.Millisecond
	ctx := WrapContext(t.Context())
	ran, err := runUsageExport(ctx, m, time.Hour, lease, func(ctx Context) error {
		for range 4 {
			select {
			case <-renewals:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		otherRan, err := runUsageExport(Background(), m, time.Hour, lease, func(Context) error {
			t.Error("two usage exports overlapped")
			return nil
		})
		if err != nil || otherRan {
			t.Fatalf("contender: %v %v", otherRan, err)
		}
		return nil
	})
	if err != nil || !ran {
		t.Fatalf("long export: %v %v", ran, err)
	}
}

func TestUsageExportLeaseLoss(t *testing.T) {
	for _, mode := range []string{"error", "takeover", "blocked-renewal"} {
		t.Run(mode, func(t *testing.T) {
			m := newUsageTestMeta(t, "memkv")
			base := m.getBase()
			original := base.en
			var calls atomic.Int64
			unblock := make(chan struct{})
			exited := make(chan struct{})
			base.en = usageLeaseTestEngine{engine: original, set: func(name string, value, diff int64) (bool, error) {
				if calls.Add(1) == 1 {
					return original.setIfSmall(name, value, diff)
				}
				switch mode {
				case "error":
					return false, io.ErrClosedPipe
				case "takeover":
					return false, nil
				default:
					defer close(exited)
					<-unblock
					return false, nil
				}
			}}
			ctx := WrapContext(t.Context())
			ran, err := runUsageExport(ctx, m, time.Hour, 90*time.Millisecond, func(ctx Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
			if mode == "blocked-renewal" {
				close(unblock)
				<-exited
			}
			if !ran || err == nil {
				t.Fatalf("lease loss accepted: %v %v", ran, err)
			}
			var value []byte
			if st := m.GetXattr(Background(), 0, lastUsageExportKey, &value); st != ENOATTR {
				t.Fatalf("failed export was marked successful: %v", st)
			}
		})
	}
}

func TestUsageExportFailureCanRetry(t *testing.T) {
	m := newUsageTestMeta(t, "memkv")
	ctx := Background()
	const lease = 30 * time.Millisecond
	if ran, err := runUsageExport(ctx, m, time.Hour, lease, func(Context) error { return io.ErrClosedPipe }); !ran || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("failed callback: %v %v", ran, err)
	}
	time.Sleep(2 * lease)
	if ran, err := runUsageExport(ctx, m, time.Hour, lease, func(Context) error { return nil }); !ran || err != nil {
		t.Fatalf("retry failed: %v %v", ran, err)
	}
	ctx.Cancel()
	if _, err := RunUsageExport(ctx, m, time.Hour, func(Context) error { t.Fatal("ran after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled schedule: %v", err)
	}
}
