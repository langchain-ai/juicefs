package chunk

import (
	"errors"
	"sync"
	"testing"
)

// A failed fetch returns a nil page. Execute then acquires a reference once per piggybacked
// caller, so a single duplicate is enough to call Acquire on nil. refs is the first field of Page,
// which puts that atomic add at address 0x0 — the runtime cannot raise a nil-dereference panic
// from an atomic, so the process dies with "fatal error: fault / unexpected fault address 0x0"
// instead of a recoverable panic.
func TestControllerExecuteFailedFetchWithDuplicate(t *testing.T) {
	con := NewController()

	wantErr := errors.New("fetch canceled")
	entered := make(chan struct{})
	finish := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p, err := con.Execute("block", func() (*Page, error) {
			close(entered)
			<-finish
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Errorf("owner error = %v, want %v", err, wantErr)
		}
		if p != nil {
			t.Errorf("owner page = %v, want nil", p)
		}
	}()

	<-entered

	// Piggyback on the in-flight fetch so dups > 0 by the time it fails.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p, err := con.Execute("block", func() (*Page, error) {
			t.Error("duplicate ran its own fetch instead of piggybacking")
			return nil, nil
		})
		if !errors.Is(err, wantErr) {
			t.Errorf("duplicate error = %v, want %v", err, wantErr)
		}
		if p != nil {
			t.Errorf("duplicate page = %v, want nil", p)
		}
	}()

	// Give the duplicate time to register itself before the fetch fails.
	for {
		con.Lock()
		dups := 0
		if c, ok := con.rs["block"]; ok {
			dups = c.dups
		}
		con.Unlock()
		if dups > 0 {
			break
		}
	}

	close(finish)
	wg.Wait()
}
