package utils

import (
	"context"
	"errors"
	"testing"
	"time"
)

// WithTimeout returns while f is still running, so f's result must not reach the caller through a
// shared variable: a concurrent write to the returned error interface tears it, and a torn
// interface faults the process instead of panicking.
func TestWithTimeoutDoesNotRaceWithAbandonedFunc(t *testing.T) {
	err := WithTimeout(context.Background(), func(ctx context.Context) error {
		time.Sleep(60 * time.Millisecond)
		return errors.New("late result from an abandoned goroutine")
	}, 10*time.Millisecond)

	if !errors.Is(err, ErrFuncTimeout) {
		t.Fatalf("err = %v, want ErrFuncTimeout", err)
	}
	// Give the abandoned goroutine time to publish its result; it must not reach err.
	time.Sleep(200 * time.Millisecond)
	if !errors.Is(err, ErrFuncTimeout) {
		t.Fatalf("err mutated after return: %v", err)
	}
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

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	time.Sleep(200 * time.Millisecond)
}

func TestWithTimeoutReturnsFuncResult(t *testing.T) {
	want := errors.New("from f")
	if err := WithTimeout(context.Background(), func(ctx context.Context) error {
		return want
	}, time.Hour); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if err := WithTimeout(context.Background(), func(ctx context.Context) error {
		return nil
	}, time.Hour); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}
