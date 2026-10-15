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

package object

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http/httptrace"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequestTracePhasesAndPending(t *testing.T) {
	now := time.Unix(1, 0)
	trace := &RequestTrace{now: func() time.Time { return now }}
	inherited := 0
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GetConn: func(string) { inherited++ }})
	hooks := httptrace.ContextClientTrace(trace.context(ctx))
	hooks.GetConn("private-host")
	hooks.DNSStart(httptrace.DNSStartInfo{Host: "private-host"})
	now = now.Add(time.Second)
	hooks.DNSDone(httptrace.DNSDoneInfo{})
	hooks.ConnectStart("tcp", "private-address")
	now = now.Add(2 * time.Second)
	hooks.ConnectDone("tcp", "private-address", nil)
	hooks.TLSHandshakeStart()
	now = now.Add(time.Second)
	hooks.TLSHandshakeDone(tls.ConnectionState{}, nil)
	hooks.GotConn(httptrace.GotConnInfo{})
	now = now.Add(3 * time.Second)
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	now = now.Add(5 * time.Second)
	pending := trace.Snapshot()
	require.Equal(t, 5.0, pending.PendingPhaseSeconds["first_byte"])
	hooks.GotFirstResponseByte()
	snap := trace.Snapshot()
	require.Equal(t, map[string]float64{"connection_wait": 4, "dns": 1, "tcp_connect": 2, "tls": 1, "request_write": 3, "first_byte": 5}, snap.PhaseSeconds)
	require.Empty(t, snap.PendingPhaseSeconds)
	require.Equal(t, uint64(1), snap.ConnectionAttempts)
	require.Equal(t, 1, inherited)
	require.NotContains(t, snap.String(), "private-")
	// A reused connection has no DNS/TCP/TLS callbacks. Their totals stay intact.
	hooks.GetConn("private-host")
	hooks.GotConn(httptrace.GotConnInfo{Reused: true})
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	now = now.Add(time.Second)
	hooks.GotFirstResponseByte()
	require.Equal(t, uint64(1), trace.Snapshot().ReusedConnections)
	require.Equal(t, 6.0, trace.Snapshot().PhaseSeconds["first_byte"])
	require.Equal(t, 5.0, snap.PhaseSeconds["first_byte"], "snapshot must not change after late callbacks")
}

func TestRequestTraceMissingAndEarlyCallbacks(t *testing.T) {
	var trace RequestTrace
	require.False(t, trace.Snapshot().Enabled)
	hooks := httptrace.ContextClientTrace(trace.context(context.Background()))
	hooks.GotFirstResponseByte()
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	require.Empty(t, trace.Snapshot().PhaseSeconds)
	require.Empty(t, trace.Snapshot().PendingPhaseSeconds)
	hooks.GetConn("host")
	hooks.GotConn(httptrace.GotConnInfo{})
	hooks.GotFirstResponseByte() // server responds before the request body is written
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	require.NotContains(t, trace.Snapshot().PhaseSeconds, "first_byte")
	require.Empty(t, trace.Snapshot().PendingPhaseSeconds)
}

func TestRequestTraceBoundedAndConcurrent(t *testing.T) {
	var trace RequestTrace
	hooks := httptrace.ContextClientTrace(trace.context(context.Background()))
	for i := 0; i < 100; i++ {
		hooks.ConnectStart("tcp", fmt.Sprintf("private-%d", i))
	}
	snap := trace.Snapshot()
	require.Equal(t, uint64(100-maxTrackedDials), snap.DroppedDials)
	require.NotContains(t, snap.String(), "private-")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				trace.retryableError("unbounded-external-error")
				_ = trace.Snapshot().String()
			}
		})
	}
	wg.Wait()
	require.Equal(t, map[string]uint64{"other": 400}, trace.Snapshot().RetryableErrors)
	for i := 0; i < 100; i++ {
		hooks.ConnectDone("tcp", fmt.Sprintf("private-%d", i), nil)
	}
	require.Empty(t, trace.Snapshot().PendingPhaseSeconds)
}

func TestRequestTraceFailureStopsBeforeBackoff(t *testing.T) {
	now := time.Unix(1, 0)
	trace := &RequestTrace{now: func() time.Time { return now }}
	hooks := httptrace.ContextClientTrace(trace.context(context.Background()))
	hooks.GetConn("host")
	hooks.ConnectStart("tcp", "address")
	now = now.Add(time.Second)
	hooks.ConnectDone("tcp", "address", context.DeadlineExceeded)
	trace.finishHTTPRequest()
	now = now.Add(30 * time.Second) // retry backoff is outside the failed connection phase
	snap := trace.Snapshot()
	require.Equal(t, 1.0, snap.PhaseSeconds["connection_wait"])
	require.Empty(t, snap.PendingPhaseSeconds)
	hooks.GetConn("host")
	hooks.GotConn(httptrace.GotConnInfo{})
	hooks.WroteRequest(httptrace.WroteRequestInfo{Err: context.Canceled})
	require.NotContains(t, trace.Snapshot().PendingPhaseSeconds, "first_byte")
}

func TestRequestTraceOmitsOverlappingCallbacks(t *testing.T) {
	now := time.Unix(1, 0)
	trace := &RequestTrace{now: func() time.Time { return now }}
	hooks := httptrace.ContextClientTrace(trace.context(context.Background()))
	hooks.GetConn("host")
	hooks.DNSStart(httptrace.DNSStartInfo{})
	hooks.TLSHandshakeStart()
	hooks.GotConn(httptrace.GotConnInfo{})
	now = now.Add(time.Second)
	trace.finishHTTPRequest()
	// A dial can outlive an HTTP request after an idle connection wins. Its late
	// callbacks must not close a subsequent request's DNS/TLS/write measurements.
	hooks.GetConn("host")
	hooks.DNSStart(httptrace.DNSStartInfo{})
	hooks.TLSHandshakeStart()
	hooks.GotConn(httptrace.GotConnInfo{})
	now = now.Add(2 * time.Second)
	snap := trace.Snapshot()
	for _, phase := range []string{"dns", "tls", "request_write", "first_byte"} {
		require.Contains(t, snap.OmittedPhases, phase)
		require.NotContains(t, snap.PhaseSeconds, phase)
		require.NotContains(t, snap.PendingPhaseSeconds, phase)
	}
	hooks.DNSDone(httptrace.DNSDoneInfo{})
	hooks.DNSDone(httptrace.DNSDoneInfo{})
	hooks.TLSHandshakeDone(tls.ConnectionState{}, nil)
	hooks.TLSHandshakeDone(tls.ConnectionState{}, nil)
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	hooks.GotFirstResponseByte()
	require.Equal(t, snap.OmittedPhases, trace.Snapshot().OmittedPhases)
	require.Empty(t, trace.Snapshot().PendingPhaseSeconds)
}

func TestRequestTraceOmitsLateDialAcrossRequests(t *testing.T) {
	var trace RequestTrace
	hooks := httptrace.ContextClientTrace(trace.context(context.Background()))
	hooks.GetConn("host")
	hooks.ConnectStart("tcp", "address")
	hooks.GotConn(httptrace.GotConnInfo{Reused: true})
	hooks.WroteRequest(httptrace.WroteRequestInfo{})
	hooks.GotFirstResponseByte()
	trace.finishHTTPRequest()
	hooks.GetConn("host")
	hooks.ConnectStart("tcp", "address")
	hooks.ConnectDone("tcp", "address", nil)
	snap := trace.Snapshot()
	require.Contains(t, snap.OmittedPhases, "tcp_connect")
	require.NotContains(t, snap.PhaseSeconds, "tcp_connect")
	require.NotContains(t, snap.PendingPhaseSeconds, "tcp_connect")
}
