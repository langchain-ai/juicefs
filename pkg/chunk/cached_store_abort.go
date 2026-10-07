/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

// AbortAfterUploads is Abort once the uploads in flight have ended (their retries included): an upload still in flight
// when Abort removes the uploaded blocks would land after that and leave an object nothing refers to. It also removes
// the last block of a slice whose Finish failed under its own key (the block's size is in the key): Abort sets the
// length to the uploaded offset, which Finish rounds up to whole blocks.
func (s *wSlice) AbortAfterUploads() {
	for i := range s.pages {
		for _, b := range s.pages[i] {
			freePage(b)
		}
		s.pages[i] = nil
	}
	for ; s.pendings > 0; s.pendings-- {
		<-s.errors // the upload ended, or gave up; its block is removed below either way
	}
	s.length = min(s.length, s.uploaded)
	if err := s.Remove(); err != nil {
		// Some blocks may never have been uploaded (a failed upload, or one that never started).
		logger.Debugf("remove the blocks of aborted slice %d: %s", s.id, err)
	}
}
