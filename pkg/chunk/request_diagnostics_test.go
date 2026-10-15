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

package chunk

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/juicedata/juicefs/pkg/compress"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juju/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRequestDiagnosticsLocalWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem, err := object.CreateStorage("mem", "", "", "", "")
		require.NoError(t, err)
		require.NoError(t, mem.Put(context.Background(), "test-object", bytes.NewBufferString("data")))
		store := &cachedStore{storage: mem, compressor: compress.NewCompressor("none"), currentDownload: make(chan struct{}, 1), conf: Config{GetTimeout: time.Minute}}
		store.initMetrics()
		// The bandwidth bucket is empty, so four bytes require one additional second.
		store.downLimit = ratelimit.NewBucketWithRate(4, 4)
		require.Equal(t, int64(4), store.downLimit.TakeAvailable(4))
		store.currentDownload <- struct{}{}
		page := NewPage(make([]byte, 4))
		defer page.Release()
		done := make(chan error, 1)
		go func() { done <- store.load(context.Background(), "test-object", page, false, false) }()
		synctest.Wait()
		time.Sleep(2 * time.Second)
		// Empty the refilled bucket immediately before releasing the slot.
		require.Equal(t, int64(4), store.downLimit.TakeAvailable(4))
		<-store.currentDownload
		require.NoError(t, <-done)
		require.Equal(t, "data", string(page.Data))
		reg := prometheus.NewRegistry()
		reg.MustRegister(store.objectWaitSeconds, store.objectReqsHistogram, store.objectPhaseSeconds)
		families, err := reg.Gather()
		require.NoError(t, err)
		waits := map[string]*dto.Histogram{}
		var requestSeconds float64
		for _, family := range families {
			for _, metric := range family.Metric {
				if family.GetName() == "object_request_wait_seconds" {
					for _, label := range metric.Label {
						if label.GetName() == "reason" {
							waits[label.GetValue()] = metric.Histogram
						}
					}
				}
				if family.GetName() == "object_request_durations_histogram_seconds" {
					requestSeconds = metric.Histogram.GetSampleSum()
				}
				require.NotEqual(t, "object_request_phase_seconds", family.GetName(), "unsupported backends must not report zero SDK phases")
			}
		}
		require.Equal(t, 2.0, waits["slot"].GetSampleSum())
		require.Equal(t, 1.0, waits["bandwidth"].GetSampleSum())
		require.Zero(t, requestSeconds, "local waits must remain outside the existing request timer")
	})
}
