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
	"fmt"
	"syscall"
	"time"
)

const (
	usageExportLeaseKey = "usageExportLease"
	lastUsageExportKey  = "lastUsageExport"
)

var errUsageExportLeaseLost = errors.New("usage export lease expired or was lost")

// RunUsageExport runs export when its independent interval is due. A renewable
// metadata lease coordinates clients; export must honor cancellation. Success is
// recorded only after export has finished publishing its complete artifact.
func RunUsageExport(ctx Context, m Meta, interval time.Duration, export func(Context) error) (bool, error) {
	return runUsageExport(ctx, m, interval, 2*time.Minute, export)
}

func usageExportDue(ctx Context, m Meta, now time.Time, interval time.Duration) (bool, error) {
	var value []byte
	if st := m.GetXattr(ctx, 0, lastUsageExportKey, &value); st != 0 && st != ENOATTR {
		return false, fmt.Errorf("read last usage export: %w", st)
	}
	if len(value) == 0 {
		return true, nil
	}
	last, err := time.Parse(time.RFC3339Nano, string(value))
	if err != nil {
		return false, fmt.Errorf("invalid last usage export timestamp: %w", err)
	}
	return now.Sub(last) >= interval, nil
}

func runUsageExport(ctx Context, m Meta, interval, leaseDuration time.Duration, export func(Context) error) (bool, error) {
	if interval <= 0 {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if m.getBase().conf.ReadOnly {
		return false, syscall.EROFS
	}
	due, err := usageExportDue(ctx, m, time.Now(), interval)
	if err != nil || !due {
		return false, err
	}
	now := time.Now()
	expires := now.Add(leaseDuration)
	en := m.getBase().en
	claimed, err := en.setIfSmall(usageExportLeaseKey, expires.UnixNano(), int64(leaseDuration))
	if err != nil || !claimed {
		return false, err
	}
	leaseContext, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	leaseCtx := WrapContext(leaseContext)
	defer leaseCtx.Cancel()
	deadline := time.AfterFunc(time.Until(expires), func() { cancel(errUsageExportLeaseLost) })
	defer deadline.Stop()
	if due, err = usageExportDue(leaseCtx, m, time.Now(), interval); err != nil || !due {
		return false, err
	}
	if err := leaseCtx.Err(); err != nil {
		return false, context.Cause(leaseContext)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(leaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				if !now.Before(expires) {
					cancel(errUsageExportLeaseLost)
					return
				}
				next := now.Add(leaseDuration)
				// A newer claimant has an expiry greater than ours and cannot be
				// overwritten. Do not read timestamp counters via getCounter: KV
				// engines encode setIfSmall values differently from increments.
				// Compare wall-clock values exactly; Sub can use monotonic time.
				renewed, err := en.setIfSmall(usageExportLeaseKey, next.UnixNano(), next.UnixNano()-expires.UnixNano())
				if err != nil {
					cancel(fmt.Errorf("renew usage export lease: %w", err))
					return
				}
				if !renewed || !time.Now().Before(expires) || !deadline.Stop() {
					cancel(errUsageExportLeaseLost)
					return
				}
				expires = next
				deadline.Reset(time.Until(expires))
			}
		}
	}()

	err = export(leaseCtx)
	close(stop)
	select {
	case <-done:
	case <-leaseCtx.Done():
		return true, errors.Join(err, context.Cause(leaseContext))
	}
	if leaseCtx.Err() != nil {
		return true, errors.Join(err, context.Cause(leaseContext))
	}
	if err != nil {
		return true, err
	}
	if st := m.SetXattr(leaseCtx, 0, lastUsageExportKey, []byte(time.Now().UTC().Format(time.RFC3339Nano)), XattrCreateOrReplace); st != 0 {
		return true, fmt.Errorf("record completed usage export: %w", st)
	}
	return true, context.Cause(leaseContext)
}
