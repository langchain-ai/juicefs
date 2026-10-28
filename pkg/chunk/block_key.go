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

package chunk

import (
	"fmt"
	"sync/atomic"
)

// HashedKey reports whether the blocks of slice id are stored under hash-prefixed keys
// ("chunks/<id%256 as %02X>/<id/1e6>/..."), which spread consecutive slices over 256 key ranges of the bucket. That is
// every slice of a volume formatted with --hash-prefix, and the slices with id >= hashPrefixFrom (0 = none) of a volume
// that turned it on later with `juicefs config --hash-prefix-from-slice`. Slices are written once, so a slice's keys
// never change: the older slices keep the sequential "chunks/<id/1e6>/<id/1e3>/..." keys.
func HashedKey(id uint64, hashPrefix bool, hashPrefixFrom uint64) bool {
	return hashPrefix || hashPrefixFrom > 0 && id >= hashPrefixFrom
}

// SliceKeyPrefix returns the object key of the blocks of slice id up to the id, without the "_<indx>_<size>" suffix.
func SliceKeyPrefix(id uint64, hashPrefix bool, hashPrefixFrom uint64) string {
	if HashedKey(id, hashPrefix, hashPrefixFrom) {
		return fmt.Sprintf("chunks/%02X/%v/%v", id%256, id/1000/1000, id)
	}
	return fmt.Sprintf("chunks/%v/%v/%v", id/1000/1000, id/1000, id)
}

// BlockKey returns the object key of block indx, of size bytes, of slice id.
func BlockKey(id uint64, indx, size int, hashPrefix bool, hashPrefixFrom uint64) string {
	if HashedKey(id, hashPrefix, hashPrefixFrom) {
		return fmt.Sprintf("chunks/%02X/%v/%v_%v_%v", id%256, id/1000/1000, id, indx, size)
	}
	return fmt.Sprintf("chunks/%v/%v/%v_%v_%v", id/1000/1000, id/1000, id, indx, size)
}

// keyLayout is the key layout of a store. The store shares it with its disk caches, whose cache and staging file
// names are the object keys, so both see the same threshold when a format reload turns it on.
type keyLayout struct {
	hashPrefix bool
	hashFrom   atomic.Uint64
}

func newKeyLayout(hashPrefix bool, hashFrom uint64) *keyLayout {
	l := &keyLayout{hashPrefix: hashPrefix}
	l.setHashFrom(hashFrom)
	return l
}

func (l *keyLayout) key(id uint64, indx, size int) string {
	return BlockKey(id, indx, size, l.hashPrefix, l.hashFrom.Load())
}

// setHashFrom sets the first slice id that uses hash-prefixed keys. Only the first non-zero value is taken: changing it
// afterwards would move the keys of slices that are already written. A different value can only come from a volume
// format that was overwritten (`juicefs format --force`, or a client too old to know the field rewriting it), so it
// is logged and ignored rather than followed.
func (l *keyLayout) setHashFrom(id uint64) {
	if id == 0 && l.hashFrom.Load() == 0 {
		return
	}
	if l.hashFrom.CompareAndSwap(0, id) {
		logger.Infof("Slices from id %d use hash-prefixed object keys", id)
		return
	}
	if cur := l.hashFrom.Load(); cur != id {
		logger.Errorf("Ignore hash-prefix-from-slice %d in the volume format: this client uses %d, and the object keys of written slices cannot change", id, cur)
	}
}
