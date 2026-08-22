//go:build linux

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

package vhostfs

import "sync/atomic"

// barrierWord backs the fences below. Go has no standalone fence primitive, so an
// atomic read-modify-write on a dedicated word is used for its ordering effect;
// the value itself is never read.
var barrierWord atomic.Uint64

// storeBarrier orders our prior writes to the rings before the index write that
// publishes them to the guest.
func storeBarrier() { barrierWord.Add(1) }

// loadBarrier orders the guest's index write before our reads of what it points at.
func loadBarrier() { barrierWord.Add(1) }
