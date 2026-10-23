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
	"github.com/redis/go-redis/v9"
)

// scriptCleanupZeroRef deletes a field of the sliceRef hash only while it still holds "0",
// in one step on the server, so a reference added concurrently by a clone is never lost.
var scriptCleanupZeroRef = redis.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) == '0' then
	return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
`)

// cleanupZeroRefAtomic drops a "0" reference count without WATCHing the sliceRef hash.
// Every clone, chunk deletion and compaction on every client of the volume writes that one
// hash, so a WATCH on it aborts on almost every attempt while those run: the transaction
// burns its retries and, since all of them hash to one txLock slot, queues every other
// zero-ref cleanup and the compaction waiting on it behind it. A "0" field means the same as
// no field, so when the script cannot run the caller may fall back or leave it to gc.
func (m *redisMeta) cleanupZeroRefAtomic(key string) error {
	return scriptCleanupZeroRef.Run(Background(), m.rdb, []string{m.sliceRefs()}, key).Err()
}
