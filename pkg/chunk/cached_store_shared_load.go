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
	"context"
	"errors"
)

// sharedLoadRetries bounds how many times a reader loads a block again after the loads it joined
// were canceled by their owners.
const sharedLoadRetries = 3

// executeShared loads a block through the store's singleflight group, as group.Execute does, except
// for a reader whose own context is still live when the load it joined was canceled by its owner:
// that reader loads the block again instead of failing with the owner's context.Canceled.
//
// A joiner gets the owner's error unchanged. The owner's context can be canceled while others wait:
// the vfs reader cancels a read-ahead it no longer needs, and a compaction (whose context is never
// canceled) that joined that download would fail with it.
//
// A reader's own load returns context.Canceled only once its context is canceled (load runs the Get
// under utils.WithTimeout, which returns the parent's ctx.Err()), so a reader that loads again here
// was a joiner: the abandoned Get of the canceled load writes only into its owner's page.
//
// Execute hands every reader a referenced page even when the load fails. The page of each load given
// up on is released here, and the caller releases the one returned, as it does after Execute.
func (store *cachedStore) executeShared(ctx context.Context, key string, fn func() (*Page, error)) (*Page, error) {
	for retry := 0; ; retry++ {
		block, err := store.group.Execute(key, fn)
		if err == nil || !errors.Is(err, context.Canceled) || ctx.Err() != nil || retry == sharedLoadRetries {
			return block, err
		}
		if block != nil {
			block.Release()
		}
		logger.Debugf("load %s again: the load it joined was canceled (%d of %d)", key, retry+1, sharedLoadRetries)
	}
}
