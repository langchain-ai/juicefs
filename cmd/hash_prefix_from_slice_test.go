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

package cmd

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	hpVolume    = "hpvol"
	hpBlockSize = 64 << 10 // formatted with --block-size 64, so small files span several blocks
)

func hpFormat(t *testing.T, metaURL, bucket string, extra ...string) {
	args := append([]string{"", "format", "--bucket", bucket, "--block-size", "64", "--trash-days", "0"}, extra...)
	require.NoError(t, Main(append(args, metaURL, hpVolume)))
}

func hpConfig(metaURL string, args ...string) error {
	return Main(append([]string{"", "config", metaURL, "--yes"}, args...))
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func TestConfigHashPrefixFromSlice(t *testing.T) {
	dir := t.TempDir()
	metaURL := "sqlite3://" + filepath.Join(dir, "meta.db")
	hpFormat(t, metaURL, filepath.Join(dir, "bucket"))
	m := meta.NewClient(metaURL, nil)
	_, err := m.Load(true)
	require.NoError(t, err)
	next, err := meta.NextSliceID(m)
	require.NoError(t, err)

	min := next + hashPrefixMinMargin
	require.ErrorContains(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(min-1)), "use at least "+u64(min))

	// A connected client that does not know the threshold blocks it.
	cur := version.Version()
	version.SetVersion("1.4.1+2026-10-24.c307bcb9")
	old := meta.NewClient(metaURL, nil)
	_, err = old.Load(true)
	require.NoError(t, err)
	require.NoError(t, old.NewSession(true))
	version.SetVersion(cur)
	require.ErrorContains(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(2*min)), "clients older than 1.4.2-A are connected")
	require.ErrorContains(t, hpConfig(metaURL, "--force", "--hash-prefix-from-slice", u64(2*min)), "clients older than 1.4.2-A are connected")
	require.NoError(t, old.CloseSession())

	// Each connected client adds two id batches to the margin.
	c := meta.NewClient(metaURL, nil)
	_, err = c.Load(true)
	require.NoError(t, err)
	require.NoError(t, c.NewSession(true))
	defer c.CloseSession() //nolint:errcheck
	min = next + hashPrefixMinMargin + 2*meta.SliceIDBatch
	require.ErrorContains(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(min-1)), "use at least "+u64(min))
	require.NoError(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(min)))
	format, err := m.Load(false)
	require.NoError(t, err)
	require.Equal(t, min, format.HashPrefixFromSlice)
	require.Equal(t, hashPrefixFromSliceMinClient, format.MinClientVersion)

	// Setting it again to the same value changes nothing; nothing else is accepted, not even with --force.
	require.NoError(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(min)))
	for _, v := range []uint64{min + 1, min - 1, 0} {
		require.ErrorContains(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(v)), "cannot be changed")
		require.ErrorContains(t, hpConfig(metaURL, "--force", "--hash-prefix-from-slice", u64(v)), "cannot be changed")
	}
	format, err = m.Load(false)
	require.NoError(t, err)
	require.Equal(t, min, format.HashPrefixFromSlice)

	// Clients without the feature can no longer mount the volume.
	version.SetVersion("1.4.1+2026-10-24.c307bcb9")
	_, err = meta.NewClient(metaURL, nil).Load(true)
	version.SetVersion(cur)
	require.ErrorContains(t, err, "allowed minimum version: 1.4.2-A")
	_, err = meta.NewClient(metaURL, nil).Load(true)
	require.NoError(t, err)
}

// TestConfigHashPrefixFromSliceRecheck lets clients take slice ids while config waits at its confirmation prompt, and
// checks that config refuses to write a threshold that has become too close.
func TestConfigHashPrefixFromSliceRecheck(t *testing.T) {
	dir := t.TempDir()
	metaURL := "sqlite3://" + filepath.Join(dir, "meta.db")
	hpFormat(t, metaURL, filepath.Join(dir, "bucket"))
	m := meta.NewClient(metaURL, nil)
	_, err := m.Load(true)
	require.NoError(t, err)
	next, err := meta.NextSliceID(m)
	require.NoError(t, err)
	from := next + hashPrefixMinMargin

	inR, inW, err := os.Pipe()
	require.NoError(t, err)
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	stdin, stdout := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = inR, outW
	defer func() { os.Stdin, os.Stdout = stdin, stdout }()
	done := make(chan error, 1)
	go func() { done <- Main([]string{"", "config", metaURL, "--hash-prefix-from-slice", u64(from)}) }()

	// The first check passed once config asks for confirmation.
	var out []byte
	buf := make([]byte, 512)
	for !bytes.Contains(out, []byte("Proceed anyway?")) {
		n, err := outR.Read(buf)
		require.NoError(t, err)
		out = append(out, buf[:n]...)
	}
	for id := uint64(0); id < next+meta.SliceIDBatch; {
		require.Zero(t, m.NewSlice(hpCtx, &id))
	}
	_, err = inW.WriteString("y\n")
	require.NoError(t, err)
	require.ErrorContains(t, <-done, "is too close to the next slice id")

	format, err := m.Load(false)
	require.NoError(t, err)
	require.Zero(t, format.HashPrefixFromSlice)
	require.Equal(t, "1.1.0-A", format.MinClientVersion)
}

func TestConfigHashPrefixFromSliceOnHashPrefixVolume(t *testing.T) {
	dir := t.TempDir()
	metaURL := "sqlite3://" + filepath.Join(dir, "meta.db")
	hpFormat(t, metaURL, filepath.Join(dir, "bucket"), "--hash-prefix")
	require.ErrorContains(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(1<<40)), "formatted with --hash-prefix")
}

// hpClient is an in-process client wired like `juicefs mount`: a chunk store with the compaction and delete callbacks,
// and the hash-prefix threshold followed on format reloads. It has no FUSE mount; files go through pkg/fs.
type hpClient struct {
	m       meta.Meta
	fs      *fs.FileSystem
	learned chan struct{} // closed once the store has a hash-prefix threshold
}

func openHPClient(t *testing.T, metaURL string, tune func(*chunk.Config)) *hpClient {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.Heartbeat = time.Second // reload the format every second instead of every 12
	m := meta.NewClient(metaURL, metaConf)
	format, err := m.Load(true)
	require.NoError(t, err)
	blob, err := createStorage(*format)
	require.NoError(t, err)
	chunkConf := getDefaultChunkConf(format)
	if tune != nil {
		tune(chunkConf)
		chunkConf.SelfCheck(format.UUID)
	}
	store := chunk.NewCachedStore(blob, *chunkConf, nil)
	registerMetaMsg(m, store, chunkConf)
	require.NoError(t, m.NewSession(true))
	c := &hpClient{m: m, learned: make(chan struct{})}
	var once sync.Once
	learned := func(f *meta.Format) {
		if f.HashPrefixFromSlice != 0 {
			once.Do(func() { close(c.learned) })
		}
	}
	learned(format)
	m.OnReload(func(f *meta.Format) {
		store.SetHashPrefixFromSlice(f.HashPrefixFromSlice)
	})
	m.OnReload(learned) // after the store's callback, so the store has it once this fires
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Version:         version.Version(),
		Chunk:           chunkConf,
		AttrTimeout:     time.Second,
		DirEntryTimeout: time.Second,
	}
	c.fs, err = fs.NewFileSystem(vfsConf, m, store, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.fs.Close() })
	return c
}

var hpCtx = meta.NewContext(1, 0, []uint32{0})

func (c *hpClient) write(t *testing.T, path string, off int64, data []byte) {
	t.Helper()
	f, st := c.fs.Open(hpCtx, path, meta.MODE_MASK_W)
	if st == syscall.ENOENT {
		f, st = c.fs.Create(hpCtx, path, 0644, 022)
	}
	require.Zero(t, st, "open %s", path)
	n, st := f.Pwrite(hpCtx, data, off)
	require.Zero(t, st, "write %s", path)
	require.Equal(t, len(data), n)
	require.Zero(t, f.Close(hpCtx), "close %s", path)
}

func (c *hpClient) read(t *testing.T, path string) []byte {
	t.Helper()
	fi, st := c.fs.Stat(hpCtx, path)
	require.Zero(t, st, "stat %s", path)
	f, st := c.fs.Open(hpCtx, path, meta.MODE_MASK_R)
	require.Zero(t, st, "open %s", path)
	defer f.Close(hpCtx) //nolint:errcheck
	buf := make([]byte, fi.Size())
	for off := 0; off < len(buf); {
		n, err := f.Pread(hpCtx, buf[off:], int64(off))
		require.NoError(t, err, "read %s at %d", path, off)
		require.NotZero(t, n, "read %s at %d", path, off)
		off += n
	}
	return buf
}

func (c *hpClient) inode(t *testing.T, path string) meta.Ino {
	t.Helper()
	fi, st := c.fs.Stat(hpCtx, path)
	require.Zero(t, st, "stat %s", path)
	return fi.Inode()
}

// slices returns the slices that path reads from, once each.
func (c *hpClient) slices(t *testing.T, path string) []meta.Slice {
	t.Helper()
	fi, st := c.fs.Stat(hpCtx, path)
	require.Zero(t, st, "stat %s", path)
	seen := make(map[uint64]bool)
	var out []meta.Slice
	for indx := uint64(0); indx*meta.ChunkSize < uint64(fi.Size()); indx++ {
		var ss []meta.Slice
		require.Zero(t, c.m.Read(hpCtx, fi.Inode(), uint32(indx), &ss))
		for _, s := range ss {
			if s.Id > 0 && !seen[s.Id] {
				seen[s.Id] = true
				out = append(out, s)
			}
		}
	}
	return out
}

func (c *hpClient) compact(t *testing.T, path string) {
	t.Helper()
	require.Zero(t, c.m.Compact(hpCtx, c.inode(t, path), 1, func() {}, func() {}))
}

// hpBlockKeys spells out the object keys of a slice's blocks, independently of chunk.BlockKey.
func hpBlockKeys(s meta.Slice, hashed bool) []string {
	var keys []string
	for indx := 0; indx*hpBlockSize < int(s.Size); indx++ {
		size := min(hpBlockSize, int(s.Size)-indx*hpBlockSize)
		if hashed {
			keys = append(keys, fmt.Sprintf("chunks/%02X/%d/%d_%d_%d", s.Id%256, s.Id/1000000, s.Id, indx, size))
		} else {
			keys = append(keys, fmt.Sprintf("chunks/%d/%d/%d_%d_%d", s.Id/1000000, s.Id/1000, s.Id, indx, size))
		}
	}
	return keys
}

// requireLayout checks that the blocks of each slice are in the bucket under the layout its id calls for, and not
// under the other one.
func requireLayout(t *testing.T, objDir string, slices []meta.Slice, from uint64) {
	t.Helper()
	for _, s := range slices {
		hashed := s.Id >= from
		for _, key := range hpBlockKeys(s, hashed) {
			require.FileExists(t, filepath.Join(objDir, key), "slice %d (threshold %d)", s.Id, from)
		}
		for _, key := range hpBlockKeys(s, !hashed) {
			require.NoFileExists(t, filepath.Join(objDir, key), "slice %d (threshold %d)", s.Id, from)
		}
	}
}

func requireGone(t *testing.T, objDir string, slices []meta.Slice) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, s := range slices {
			for _, hashed := range []bool{false, true} {
				for _, key := range hpBlockKeys(s, hashed) {
					if _, err := os.Stat(filepath.Join(objDir, key)); err == nil {
						return false
					}
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "slices %v still have objects", slices)
}

func hpRandom(t *testing.T, n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// fatalLogs runs fn, which is expected to end in logger.Fatal, and returns the error and fatal messages it logged.
func fatalLogs(t *testing.T, fn func()) (msgs []string) {
	t.Helper()
	type fatalExit struct{}
	var mu sync.Mutex
	hooks := logrus.LevelHooks{}
	for k, v := range logger.Hooks {
		hooks[k] = v
	}
	logger.AddHook(&logHook{fn: func(e *logrus.Entry) {
		mu.Lock()
		defer mu.Unlock()
		msgs = append(msgs, e.Message)
	}})
	exit := logger.ExitFunc
	logger.ExitFunc = func(int) { panic(fatalExit{}) }
	defer func() {
		logger.ExitFunc = exit
		logger.ReplaceHooks(hooks)
		r := recover()
		if _, ok := r.(fatalExit); !ok {
			if r != nil {
				panic(r)
			}
			t.Fatal("expected a fatal error")
		}
	}()
	fn()
	return
}

type logHook struct{ fn func(*logrus.Entry) }

func (h *logHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel, logrus.FatalLevel}
}
func (h *logHook) Fire(e *logrus.Entry) error {
	h.fn(e)
	return nil
}

func redisTestURL(t *testing.T, db int) string {
	url := fmt.Sprintf("redis://127.0.0.1:6379/%d", db)
	opt, err := redis.ParseURL(url)
	require.NoError(t, err)
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis at %s: %s", url, err)
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())
	return url
}

// TestHashPrefixFromSliceMixedVolume turns hash-prefix-from-slice on for a live volume and runs the data paths over
// slices on both sides of the threshold: reads, overwrites, compaction (which deletes old slices), clones, a client
// restarted on staged writeback blocks, gc leak detection, fsck, and dump/load into another metadata engine.
func TestHashPrefixFromSliceMixedVolume(t *testing.T) {
	t.Run("sqlite3", func(t *testing.T) {
		dir := t.TempDir()
		testHashPrefixFromSliceMixedVolume(t, dir, "sqlite3://"+filepath.Join(dir, "meta.db"),
			"sqlite3://"+filepath.Join(dir, "meta-json.db"), "sqlite3://"+filepath.Join(dir, "meta-binary.db"))
	})
	t.Run("redis", func(t *testing.T) {
		// DBs 6-8 are not used by other tests, some of which leave servers running on theirs.
		testHashPrefixFromSliceMixedVolume(t, t.TempDir(), redisTestURL(t, 6), redisTestURL(t, 7), redisTestURL(t, 8))
	})
}

func testHashPrefixFromSliceMixedVolume(t *testing.T, dir, metaURL, jsonLoadURL, binaryLoadURL string) {
	bucket := filepath.Join(dir, "bucket")
	objDir := filepath.Join(bucket, hpVolume)
	hpFormat(t, metaURL, bucket)
	want := make(map[string][]byte)
	put := func(c *hpClient, path string, off int, data []byte) {
		t.Helper()
		c.write(t, path, int64(off), data)
		cur := want[path]
		if end := off + len(data); end > len(cur) {
			cur = append(cur, make([]byte, end-len(cur))...)
		}
		copy(cur[off:], data)
		want[path] = cur
	}
	// verify reads every file through a new client, so every block comes from the bucket.
	verify := func(metaURL string) {
		t.Helper()
		r := openHPClient(t, metaURL, nil)
		for path, data := range want {
			require.True(t, bytes.Equal(data, r.read(t, path)), "content of %s", path)
		}
	}

	a := openHPClient(t, metaURL, nil)
	put(a, "/old", 0, hpRandom(t, 5*hpBlockSize/2))
	put(a, "/mixed", 0, hpRandom(t, 4*hpBlockSize))
	oldSlices := a.slices(t, "/old")

	// Turn it on with the smallest threshold config accepts (one connected client), and wait for the client to learn
	// it on a format reload.
	next, err := meta.NextSliceID(a.m)
	require.NoError(t, err)
	from := next + hashPrefixMinMargin + 2*meta.SliceIDBatch
	require.NoError(t, hpConfig(metaURL, "--hash-prefix-from-slice", u64(from)))
	select {
	case <-a.learned:
	case <-time.After(15 * time.Second):
		t.Fatal("the client did not reload the format")
	}
	// Use up the ids below the threshold, as a busy volume would before it gets there.
	for id := uint64(0); id < from; {
		require.Zero(t, a.m.NewSlice(hpCtx, &id))
	}

	put(a, "/new", 0, hpRandom(t, 5*hpBlockSize/2))
	put(a, "/mixed", hpBlockSize/2, hpRandom(t, hpBlockSize)) // over blocks 0 and 1 of the first slice
	requireLayout(t, objDir, oldSlices, from)
	requireLayout(t, objDir, a.slices(t, "/old"), from)
	newSlices := a.slices(t, "/new")
	requireLayout(t, objDir, newSlices, from)
	for _, s := range newSlices {
		require.GreaterOrEqual(t, s.Id, from)
	}
	mixed := a.slices(t, "/mixed")
	require.Len(t, mixed, 2)
	require.Less(t, mixed[0].Id, from)
	require.GreaterOrEqual(t, mixed[1].Id, from)
	requireLayout(t, objDir, mixed, from)
	verify(metaURL)

	// Compaction reads both layouts, writes one hashed slice, and deletes the old slices under their own keys.
	a.compact(t, "/mixed")
	compacted := a.slices(t, "/mixed")
	require.Len(t, compacted, 1)
	require.GreaterOrEqual(t, compacted[0].Id, from)
	requireLayout(t, objDir, compacted, from)
	requireGone(t, objDir, mixed)
	verify(metaURL)

	// A clone shares the slices; once it diverges and is compacted, only its own slices are deleted.
	require.Zero(t, a.fs.Clone(hpCtx, "/mixed", "/clone", false))
	want["/clone"] = append([]byte(nil), want["/mixed"]...)
	put(a, "/clone", 2*hpBlockSize, hpRandom(t, hpBlockSize/2))
	var own []meta.Slice
	for _, s := range a.slices(t, "/clone") {
		if s.Id != compacted[0].Id {
			own = append(own, s)
		}
	}
	require.Len(t, own, 1)
	a.compact(t, "/clone")
	requireLayout(t, objDir, a.slices(t, "/clone"), from)
	requireGone(t, objDir, own)
	requireLayout(t, objDir, compacted, from) // still used by /mixed
	verify(metaURL)

	// Writeback: a client stages blocks under the hashed names and stops before uploading them; another client on the
	// same cache directory, as after a restart, uploads them under those names.
	cacheDir := filepath.Join(dir, "cache")
	writeback := func(delay time.Duration) func(*chunk.Config) {
		return func(c *chunk.Config) {
			c.CacheDir = cacheDir
			c.CacheSize = 256 << 20
			c.FreeSpace = 0.01
			c.AutoCreate = true
			c.Writeback = true
			c.UploadDelay = delay
		}
	}
	w := openHPClient(t, metaURL, writeback(time.Hour))
	put(w, "/staged", 0, hpRandom(t, 5*hpBlockSize/2))
	staged := w.slices(t, "/staged")
	stagingDir := filepath.Join(cacheDir, w.m.GetFormat().UUID, "rawstaging")
	for _, s := range staged {
		require.GreaterOrEqual(t, s.Id, from)
		for _, key := range hpBlockKeys(s, true) {
			require.FileExists(t, filepath.Join(stagingDir, key))
			require.NoFileExists(t, filepath.Join(objDir, key))
		}
	}
	openHPClient(t, metaURL, writeback(0))
	require.Eventually(t, func() bool {
		for _, s := range staged {
			for _, key := range hpBlockKeys(s, true) {
				if _, err := os.Stat(filepath.Join(objDir, key)); err != nil {
					return false
				}
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond)
	requireLayout(t, objDir, staged, from)
	verify(metaURL)

	// gc finds leaked objects in both layouts and keeps every referenced one. All objects are aged past gc's
	// skipped time, so it judges each of them.
	leaked := []string{
		hpBlockKeys(meta.Slice{Id: 12345, Size: 4096}, false)[0],
		hpBlockKeys(meta.Slice{Id: from + 50_000_000, Size: 4096}, true)[0],
	}
	for _, key := range leaked {
		p := filepath.Join(objDir, key)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, make([]byte, 4096), 0644))
	}
	past := time.Now().Add(-time.Hour)
	require.NoError(t, filepath.Walk(filepath.Join(objDir, "chunks"), func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		return os.Chtimes(p, past, past)
	}))
	t.Setenv("JFS_GC_SKIPPEDTIME", "60")
	require.NoError(t, Main([]string{"", "gc", metaURL, "--delete"}))
	for _, key := range leaked {
		require.NoFileExists(t, filepath.Join(objDir, key))
	}
	for _, path := range []string{"/old", "/new", "/mixed", "/clone", "/staged"} {
		requireLayout(t, objDir, a.slices(t, path), from)
	}
	verify(metaURL)

	// fsck matches the bucket by block name; with --path it looks every block up under its key.
	require.NoError(t, Main([]string{"", "fsck", metaURL}))
	for _, path := range []string{"/old", "/new", "/mixed", "/clone", "/staged"} {
		require.NoError(t, Main([]string{"", "fsck", metaURL, "--path", path}))
	}

	// dump/load keeps the threshold and the client version gate.
	for _, d := range []struct {
		loadURL string
		args    []string
	}{
		{jsonLoadURL, nil},
		{binaryLoadURL, []string{"--binary"}},
	} {
		dump := filepath.Join(dir, fmt.Sprintf("dump-%d", len(d.args)))
		require.NoError(t, Main(append([]string{"", "dump", metaURL, dump}, d.args...)))
		require.NoError(t, Main(append([]string{"", "load", d.loadURL, dump}, d.args...)))
		format, err := meta.NewClient(d.loadURL, nil).Load(true)
		require.NoError(t, err)
		require.Equal(t, from, format.HashPrefixFromSlice)
		require.Equal(t, hashPrefixFromSliceMinClient, format.MinClientVersion)
		verify(d.loadURL)
	}

	// fsck reports a lost hashed block under its key.
	victim := hpBlockKeys(newSlices[0], true)[0]
	require.NoError(t, os.Remove(filepath.Join(objDir, victim)))
	msgs := fatalLogs(t, func() { _ = Main([]string{"", "fsck", metaURL, "--path", "/new"}) })
	require.Contains(t, fmt.Sprint(msgs), "can't find block "+victim[len("chunks/"):])
}
