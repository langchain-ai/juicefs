//go:build !nogs

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newDiagnosticGS(t *testing.T, server *httptest.Server) *gs {
	t.Helper()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	// Shorten test backoff without changing the SDK's default classification or policy.
	client.SetRetry(storage.WithBackoff(gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond}))
	g := &gs{clients: []*storage.Client{client}, bucket: "test-bucket"}
	require.NoError(t, g.InitTiers(nil))
	return g
}

func TestGSRequestDiagnosticsReadRetry(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusForbidden} {
		for _, traced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/trace=%v", code, traced), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if calls.Add(1) == 1 && code != http.StatusOK {
						w.WriteHeader(code)
						return
					}
					w.Header().Set("Content-Length", "7")
					_, _ = w.Write([]byte("payload"))
				}))
				defer server.Close()
				g := newDiagnosticGS(t, server)
				var trace *RequestTrace
				if traced {
					trace = &RequestTrace{}
				}
				reader, err := g.Get(context.Background(), "test-object", 0, -1, WithRequestTrace(trace))
				if code == http.StatusForbidden {
					var apiErr *googleapi.Error
					require.ErrorAs(t, err, &apiErr)
					require.Equal(t, code, apiErr.Code)
					require.Equal(t, int32(1), calls.Load())
					require.Empty(t, trace.Snapshot().RetryableErrors)
					if traced {
						require.Equal(t, "http_4xx_other", trace.Snapshot().TerminalError)
					}
					return
				}
				require.NoError(t, err)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, "payload", string(data))
				expected := int32(1)
				if code != http.StatusOK {
					expected = 2
				}
				require.Equal(t, expected, calls.Load())
				if traced {
					snap := trace.Snapshot()
					require.Equal(t, uint64(expected), snap.ConnectionAttempts)
					require.Contains(t, snap.PhaseSeconds, "body_read")
					require.Contains(t, snap.PhaseSeconds, "first_byte")
					if expected == 2 {
						require.Equal(t, uint64(1), snap.RetryableErrors[fmt.Sprintf("http_%d", code)])
					}
				}
			})
		}
	}
}

func TestGSRequestDiagnosticsCancellation(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	defer server.Close()
	g := newDiagnosticGS(t, server)
	wrote := make(chan struct{})
	parent := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { close(wrote) }})
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var trace RequestTrace
	done := make(chan error, 1)
	go func() { _, err := g.Get(ctx, "test-object", 0, -1, WithRequestTrace(&trace)); done <- err }()
	<-entered
	<-wrote
	require.Contains(t, trace.Snapshot().PendingPhaseSeconds, "first_byte")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, "canceled", trace.Snapshot().TerminalError)
	require.Empty(t, trace.Snapshot().PendingPhaseSeconds)
	require.Equal(t, uint64(1), trace.Snapshot().ConnectionAttempts)
}

func TestGSRequestDiagnosticsPut(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		for _, traced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/trace=%v", code, traced), func(t *testing.T) {
				var calls atomic.Int32
				var body []byte
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, _ = io.ReadAll(r.Body)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(code)
					if code == http.StatusOK {
						_, _ = w.Write([]byte(`{"name":"test-object","bucket":"test-bucket","size":"7","generation":"1"}`))
					}
				}))
				defer server.Close()
				g := newDiagnosticGS(t, server)
				var trace *RequestTrace
				if traced {
					trace = &RequestTrace{}
				}
				err := g.Put(context.Background(), "test-object", bytes.NewBufferString("payload"), WithRequestTrace(trace))
				if code == http.StatusOK {
					require.NoError(t, err)
					require.Contains(t, string(body), "payload")
				} else {
					require.Error(t, err)
				}
				require.Equal(t, int32(1), calls.Load(), "unconditional writes keep the SDK's default idempotency policy")
				if traced {
					require.Equal(t, uint64(1), trace.Snapshot().ConnectionAttempts)
					if code != http.StatusOK {
						require.Equal(t, "http_503", trace.Snapshot().TerminalError)
					}
				}
			})
		}
	}
}

func TestGSErrorCategories(t *testing.T) {
	for _, tc := range []struct {
		err      error
		category string
	}{
		{&googleapi.Error{Code: 429}, "http_429"},
		{fmt.Errorf("wrapped: %w", &googleapi.Error{Code: 503}), "http_503"},
		{&googleapi.Error{Code: 501}, "http_5xx_other"},
		{context.DeadlineExceeded, "deadline"},
		{status.Error(codes.Unavailable, "private-message"), "grpc_unavailable"},
		{status.Error(codes.ResourceExhausted, "private-message"), "grpc_resource_exhausted"},
		{errors.New("private-message"), "other"},
	} {
		require.Equal(t, tc.category, gsErrorCategory(tc.err))
	}
}

func TestGSRequestDiagnosticsResumablePut(t *testing.T) {
	for _, failChunk := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", failChunk), func(t *testing.T) {
			payload := bytes.Repeat([]byte("x"), 6<<20)
			var uploaded bytes.Buffer
			var requests, chunks atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path != "/upload-session" {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Location", server.URL+"/upload-session")
					return
				}
				if chunks.Add(1) == 1 && failChunk {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_, _ = io.Copy(&uploaded, r.Body)
				if uploaded.Len() < len(payload) {
					w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", uploaded.Len()-1))
					w.Header().Set("X-Http-Status-Code-Override", "308")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"name":"test-object","bucket":"test-bucket","size":%q,"generation":"1"}`, strconv.Itoa(len(payload)))
			}))
			defer server.Close()
			g := newDiagnosticGS(t, server)
			var trace RequestTrace
			require.NoError(t, g.Put(context.Background(), "test-object", bytes.NewReader(payload), WithRequestTrace(&trace)))
			require.Equal(t, payload, uploaded.Bytes())
			expected := int32(3) // initiation plus two data chunks
			if failChunk {
				expected++
			}
			require.Equal(t, expected, requests.Load())
			require.Equal(t, uint64(expected), trace.Snapshot().ConnectionAttempts)
			// The SDK's unconditional resumable-upload path uses a lower-level retry
			// classifier and does not forward the object-level WithErrorFunc observer.
			require.Empty(t, trace.Snapshot().RetryableErrors)
			require.Empty(t, trace.Snapshot().TerminalError)
		})
	}
}
