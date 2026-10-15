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
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juju/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
)

func (store *cachedStore) initRequestDiagnostics() {
	store.objectWaitSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "object_request_wait_seconds",
		Help:    "Local slot or bandwidth wait durations, outside object request latency.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"method", "reason"})
	store.objectPhaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "object_request_phase_seconds",
		Help:    "Observed SDK phase durations summed per object operation; phases can overlap.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 20),
	}, []string{"method", "phase"})
	store.objectRetryableErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_retryable_errors_total",
		Help: "SDK retryable-error decisions, not completed retries.",
	}, []string{"method", "reason"})
	store.objectTerminalErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_terminal_errors_total",
		Help: "Terminal GCS operation errors by category, including errors that were not retried.",
	}, []string{"method", "reason"})
	store.objectConnectionAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_connection_attempts_total",
		Help: "HTTP connection acquisitions, including reuse and SDK protocol requests.",
	}, []string{"method"})
	store.objectReusedConnections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_reused_connections_total",
		Help: "HTTP connection acquisitions that reused an existing connection.",
	}, []string{"method"})
}

func (store *cachedStore) acquireSlot(method string, slots chan struct{}) {
	start := time.Now()
	slots <- struct{}{}
	store.objectWaitSeconds.WithLabelValues(method, "slot").Observe(time.Since(start).Seconds())
}

func (store *cachedStore) waitForBandwidth(method string, limit *ratelimit.Bucket, size int) {
	if limit == nil {
		return
	}
	start := time.Now()
	limit.Wait(int64(size))
	store.objectWaitSeconds.WithLabelValues(method, "bandwidth").Observe(time.Since(start).Seconds())
}

func (store *cachedStore) observeRequestTrace(method string, trace *object.RequestTrace) object.RequestTraceSnapshot {
	snapshot := trace.Snapshot()
	if !snapshot.Enabled {
		return snapshot
	}
	for phase, seconds := range snapshot.PhaseSeconds {
		store.objectPhaseSeconds.WithLabelValues(method, phase).Observe(seconds)
	}
	for reason, count := range snapshot.RetryableErrors {
		store.objectRetryableErrors.WithLabelValues(method, reason).Add(float64(count))
	}
	if snapshot.TerminalError != "" {
		store.objectTerminalErrors.WithLabelValues(method, snapshot.TerminalError).Inc()
	}
	store.objectConnectionAttempts.WithLabelValues(method).Add(float64(snapshot.ConnectionAttempts))
	store.objectReusedConnections.WithLabelValues(method).Add(float64(snapshot.ReusedConnections))
	return snapshot
}
