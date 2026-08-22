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

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	descSize      = 16
	descFNext     = 0x1
	descFWrite    = 0x2
	descFIndirect = 0x4
)

// descriptor is one entry of the split-virtqueue descriptor table.
type descriptor struct {
	addr  uint64
	len   uint32
	flags uint16
	next  uint16
}

// vring is one split virtqueue: the three rings live in guest memory, and the
// two eventfds carry notifications in each direction.
type vring struct {
	index uint16
	size  uint16

	descTable []byte
	avail     []byte
	used      []byte

	kick *os.File
	call *os.File

	enabled bool
	// lastAvail is our cursor into the available ring; the guest's producer index
	// lives in the ring itself.
	lastAvail uint16
}

func (v *vring) ready() bool {
	return v.enabled && v.descTable != nil && v.avail != nil && v.used != nil && v.kick != nil
}

func (v *vring) desc(idx uint16) (descriptor, error) {
	if idx >= v.size {
		return descriptor{}, fmt.Errorf("descriptor index %d out of range", idx)
	}
	off := int(idx) * descSize
	if off+descSize > len(v.descTable) {
		return descriptor{}, fmt.Errorf("descriptor %d outside the table", idx)
	}
	b := v.descTable[off:]
	return descriptor{
		addr:  binary.LittleEndian.Uint64(b[0:]),
		len:   binary.LittleEndian.Uint32(b[8:]),
		flags: binary.LittleEndian.Uint16(b[12:]),
		next:  binary.LittleEndian.Uint16(b[14:]),
	}, nil
}

// availIdx is the guest's producer index (offset 2 in the available ring).
func (v *vring) availIdx() uint16 {
	return binary.LittleEndian.Uint16(v.avail[2:])
}

// availRingEntry is the head descriptor index for slot i of the available ring.
func (v *vring) availRingEntry(i uint16) uint16 {
	off := 4 + int(i%v.size)*2
	return binary.LittleEndian.Uint16(v.avail[off:])
}

// usedIdx is our consumer index, published at offset 2 of the used ring.
func (v *vring) usedIdx() uint16 {
	return binary.LittleEndian.Uint16(v.used[2:])
}

func (v *vring) setUsedIdx(idx uint16) {
	binary.LittleEndian.PutUint16(v.used[2:], idx)
}

// addUsed publishes one completed chain: its head index and the number of bytes
// we wrote into its device-writable descriptors.
func (v *vring) addUsed(head uint16, written uint32) error {
	idx := v.usedIdx()
	off := 4 + int(idx%v.size)*8
	if off+8 > len(v.used) {
		return fmt.Errorf("used ring slot %d out of range", idx)
	}
	binary.LittleEndian.PutUint32(v.used[off:], uint32(head))
	binary.LittleEndian.PutUint32(v.used[off+4:], written)
	// The slot must be visible before the index that publishes it.
	storeBarrier()
	v.setUsedIdx(idx + 1)
	return nil
}

// notify signals the guest that we added to the used ring.
func (v *vring) notify() error {
	if v.call == nil {
		return nil
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	_, err := v.call.Write(buf[:])
	return err
}

// chain is one descriptor chain split into the parts the device reads and the
// parts it writes, which for virtio-fs is the FUSE request and its reply buffer.
type chain struct {
	head     uint16
	readable [][]byte
	writable [][]byte
}

// nextChain pops the next available chain, or returns ok=false when the guest has
// not produced one.
func (v *vring) nextChain(mem *guestMemory) (chain, bool, error) {
	if v.lastAvail == v.availIdx() {
		return chain{}, false, nil
	}
	// Ensure the descriptors the index published are visible before we read them.
	loadBarrier()
	head := v.availRingEntry(v.lastAvail)
	v.lastAvail++

	c := chain{head: head}
	idx := head
	for i := 0; ; i++ {
		// A cycle in the chain would otherwise spin forever.
		if i > int(v.size) {
			return chain{}, false, fmt.Errorf("descriptor chain from %d is too long", head)
		}
		d, err := v.desc(idx)
		if err != nil {
			return chain{}, false, err
		}
		if d.flags&descFIndirect != 0 {
			return chain{}, false, fmt.Errorf("indirect descriptors are not supported")
		}
		if d.len > 0 {
			buf, err := mem.slice(d.addr, uint64(d.len))
			if err != nil {
				return chain{}, false, err
			}
			if d.flags&descFWrite != 0 {
				c.writable = append(c.writable, buf)
			} else {
				c.readable = append(c.readable, buf)
			}
		}
		if d.flags&descFNext == 0 {
			break
		}
		idx = d.next
	}
	return c, true, nil
}
