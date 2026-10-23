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
	"sync"
	"testing"
)

func TestRedisCleanupZeroRef(t *testing.T) {
	m := newTestRedisMeta(t, 12)
	ctx := Background()
	if err := m.rdb.HSet(ctx, m.sliceRefs(), "k1_4096", "0", "k2_4096", "1", "k3_4096", "-1").Err(); err != nil {
		t.Fatalf("seed sliceRef: %v", err)
	}
	for _, key := range []string{"k1_4096", "k2_4096", "k3_4096", "k4_4096"} {
		m.cleanupZeroRef(key)
	}
	got, err := m.rdb.HGetAll(ctx, m.sliceRefs()).Result()
	if err != nil {
		t.Fatalf("read sliceRef: %v", err)
	}
	want := map[string]string{"k2_4096": "1", "k3_4096": "-1"}
	if len(got) != len(want) || got["k2_4096"] != "1" || got["k3_4096"] != "-1" {
		t.Fatalf("sliceRef after cleanup = %v, want %v", got, want)
	}
}

// Clones, chunk deletions and compactions on other clients keep writing the sliceRef hash;
// the cleanup must still drop every zero field instead of losing to them.
func TestRedisCleanupZeroRefWhileHashIsWritten(t *testing.T) {
	m := newTestRedisMeta(t, 12)
	ctx := Background()
	const n = 200
	keys := make([]string, n)
	for i := range keys {
		keys[i] = m.sliceKey(uint64(i+1), 4096)
		if err := m.rdb.HSet(ctx, m.sliceRefs(), keys[i], "0").Err(); err != nil {
			t.Fatalf("seed sliceRef: %v", err)
		}
	}

	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			field := m.sliceKey(uint64(1_000_000+w), 4096)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = m.rdb.HIncrBy(ctx, m.sliceRefs(), field, 1).Err()
			}
		}(w)
	}
	for _, key := range keys {
		m.cleanupZeroRef(key)
	}
	close(stop)
	writers.Wait()

	for _, key := range keys {
		exists, err := m.rdb.HExists(ctx, m.sliceRefs(), key).Result()
		if err != nil {
			t.Fatalf("HExists %s: %v", key, err)
		}
		if exists {
			t.Fatalf("zero ref %s survived cleanup", key)
		}
	}
}
