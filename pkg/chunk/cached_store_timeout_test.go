package chunk

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// slowGetStorage delays Get past GetTimeout so WithTimeout abandons load's closure while it is
// still running.
type slowGetStorage struct {
	object.ObjectStorage
	delay time.Duration
}

func (s *slowGetStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

// A GET that outlives GetTimeout leaves load's closure running after load has returned. The
// closure must not share err, n, reqID or sc with load: writing the error interface while
// errors.Is reads it tears the interface, and the runtime faults on the torn type word rather
// than raising a recoverable panic.
func TestLoadDoesNotRaceWithTimedOutGet(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(ctx, "chunks/0/0/1_0_1048576", io.LimitReader(fillReader{}, 1<<20)); err != nil {
		t.Fatal(err)
	}

	conf := defaultConf
	conf.CacheDir = filepath.Join(os.TempDir(), "raceCache")
	conf.GetTimeout = 20 * time.Millisecond
	_ = os.RemoveAll(conf.CacheDir)
	defer os.RemoveAll(conf.CacheDir)

	store := NewCachedStore(&slowGetStorage{mem, 120 * time.Millisecond}, conf, nil).(*cachedStore)

	page := NewOffPage(1 << 20)
	defer page.Release()
	if err := store.load(ctx, "chunks/0/0/1_0_1048576", page, false, false); err == nil {
		t.Fatal("expected load to time out")
	}
	// Let the abandoned closure publish its result.
	time.Sleep(300 * time.Millisecond)
}

type fillReader struct{}

func (fillReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
