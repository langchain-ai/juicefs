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

package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WithTimeout returns while f is still running, so f's result must not reach the caller through a
// shared variable: a concurrent write to the returned error interface tears it, and a torn
// interface faults the process instead of raising a recoverable panic.
func TestWithTimeoutDoesNotRaceWithAbandonedFunc(t *testing.T) {
	err := WithTimeout(context.Background(), func(ctx context.Context) error {
		time.Sleep(60 * time.Millisecond)
		return errors.New("late result from an abandoned goroutine")
	}, 10*time.Millisecond)
	require.ErrorIs(t, err, ErrFuncTimeout)

	// Give the abandoned goroutine time to publish its result; it must not reach err.
	time.Sleep(200 * time.Millisecond)
	require.ErrorIs(t, err, ErrFuncTimeout)
}

func TestWithTimeoutCancelBeatsAbandonedFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := WithTimeout(ctx, func(ctx context.Context) error {
		time.Sleep(60 * time.Millisecond)
		return errors.New("late result from an abandoned goroutine")
	}, time.Hour)
	require.ErrorIs(t, err, context.Canceled)

	time.Sleep(200 * time.Millisecond)
}

func TestWithTimeoutReturnsFuncResult(t *testing.T) {
	want := errors.New("from f")
	err := WithTimeout(context.Background(), func(ctx context.Context) error {
		return want
	}, time.Hour)
	require.ErrorIs(t, err, want)

	require.NoError(t, WithTimeout(context.Background(), func(ctx context.Context) error {
		return nil
	}, time.Hour))
}
