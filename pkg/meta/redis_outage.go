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
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultMetaOutageTolerance covers a Redis restart or failover of a few tens of
// seconds. Past it, transactions fail as they did before and callers see EIO.
const defaultMetaOutageTolerance = 60 * time.Second

// metaOutageTolerance bounds how long a Redis transaction keeps retrying while the
// server is unreachable, so a short outage delays file operations instead of
// failing them. A failed slice commit poisons the file's writer until every
// handle closes, which a long-lived writer such as a VM disk image never does.
// JFS_META_OUTAGE_TOLERANCE overrides it; 0 disables the retries.
var metaOutageTolerance = parseMetaOutageTolerance(os.Getenv("JFS_META_OUTAGE_TOLERANCE"))

func parseMetaOutageTolerance(value string) time.Duration {
	if value == "" {
		return defaultMetaOutageTolerance
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		logger.Warnf("Ignoring invalid JFS_META_OUTAGE_TOLERANCE %q; using %s", value, defaultMetaOutageTolerance)
		return defaultMetaOutageTolerance
	}
	return d
}

// txIdempotentKey marks a transaction whose function detects, on a rerun, that an
// earlier attempt was applied. Only such a transaction may retry an error that
// leaves it unknown whether EXEC ran.
type txIdempotentKey struct{}

// isUnsentRedisError reports whether err means the command never reached Redis,
// so retrying it cannot apply anything twice.
func isUnsentRedisError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, redis.ErrPoolTimeout) ||
		errors.Is(err, redis.ErrPoolExhausted) ||
		errors.Is(err, redis.ErrClosed)
}

// isRedisConnectionError reports whether err is a connection failure, sent or not.
// A sent command may or may not have been applied.
func isRedisConnectionError(err error) bool {
	if isUnsentRedisError(err) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// metaOutageBackoff grows from 20ms to 1s, so a recovered server is noticed within
// a second.
func metaOutageBackoff(attempt int) time.Duration {
	if attempt > 6 {
		return time.Second
	}
	return 20 * time.Millisecond << attempt
}

// readThroughOutage reruns a read while the metadata engine is unreachable, for up
// to metaOutageTolerance. A read has no side effects, so any connection error is
// safe to retry. Block reads and attribute refreshes go through here: an error from
// either reaches a guest disk as an I/O error, which ext4 answers by going
// read-only.
func readThroughOutage[T any](ctx Context, op string, read func() (T, error)) (T, error) {
	start := time.Now()
	for attempt := 0; ; attempt++ {
		v, err := read()
		if err == nil || !isRedisConnectionError(err) || time.Since(start) >= metaOutageTolerance || ctx.Canceled() {
			if err == nil && attempt > 0 {
				logger.Warnf("%s succeeded after the metadata engine was unreachable for %s (%d retries)", op, time.Since(start), attempt)
			}
			return v, err
		}
		if attempt == 0 {
			logger.Warnf("Metadata engine unreachable during %s, retrying for up to %s: %s", op, metaOutageTolerance, err)
		}
		time.Sleep(metaOutageBackoff(attempt))
	}
}
