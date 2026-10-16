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
	"context"
	"errors"
	"io"
	"net"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func traceGSRequest(ctx context.Context, obj *storage.ObjectHandle, trace *RequestTrace) (context.Context, *storage.ObjectHandle) {
	if trace == nil {
		return ctx, obj
	}
	return trace.context(ctx), obj.Retryer(storage.WithErrorFunc(func(err error) bool {
		// Preserve the SDK's default classification, idempotency policy and
		// backoff. This counts retryable-error decisions, not retries completed.
		trace.finishHTTPRequest()
		retry := storage.ShouldRetry(err)
		if retry {
			trace.retryableError(gsErrorCategory(err))
		}
		return retry
	}))
}

func gsErrorCategory(err error) string {
	var apiError *googleapi.Error
	if errors.As(err, &apiError) {
		switch apiError.Code {
		case 408:
			return "http_408"
		case 429:
			return "http_429"
		case 500:
			return "http_500"
		case 502:
			return "http_502"
		case 503:
			return "http_503"
		case 504:
			return "http_504"
		default:
			if apiError.Code >= 400 && apiError.Code < 500 {
				return "http_4xx_other"
			}
			if apiError.Code >= 500 && apiError.Code < 600 {
				return "http_5xx_other"
			}
		}
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable:
			return "grpc_unavailable"
		case codes.ResourceExhausted:
			return "grpc_resource_exhausted"
		case codes.Internal:
			return "grpc_internal"
		case codes.DeadlineExceeded:
			return "deadline"
		}
	}
	var netError net.Error
	if errors.As(err, &netError) {
		if netError.Timeout() {
			return "deadline"
		}
		return "connection"
	}
	return "other"
}

func finishGSRequest(trace *RequestTrace, err error) {
	if trace == nil {
		return
	}
	trace.finishHTTPRequest()
	if err != nil && !errors.Is(err, io.EOF) {
		trace.setTerminalError(gsErrorCategory(err))
	}
}

// tracedResponseBody measures time spent inside the SDK reader, which may itself
// reopen the HTTP request. This can overlap the HTTP phases and retry delays.
type tracedResponseBody struct {
	io.ReadCloser
	trace *RequestTrace
}

func (r *tracedResponseBody) Read(p []byte) (int, error) {
	r.trace.mu.Lock()
	r.trace.body = r.trace.clock()
	r.trace.mu.Unlock()
	n, err := r.ReadCloser.Read(p)
	r.trace.mu.Lock()
	r.trace.finishPhase(6, r.trace.body)
	r.trace.body = time.Time{}
	r.trace.mu.Unlock()
	if err != nil {
		finishGSRequest(r.trace, err)
	}
	return n, err
}
