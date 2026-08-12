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
	"fmt"
	"os"
	"syscall"
)

// region is one mapped slice of guest RAM.
type region struct {
	guestPhysAddr uint64
	// userAddr is the address the region has in the *frontend's* process. The
	// vring addresses the frontend sends are in that space, so translation needs
	// both mappings of the same memory.
	userAddr uint64
	size     uint64
	data     []byte
}

// guestMemory is the guest's RAM as mapped into this process by SET_MEM_TABLE.
type guestMemory struct {
	regions []region
}

func mapGuestMemory(regions []memoryRegion, files []*os.File) (*guestMemory, error) {
	if len(regions) != len(files) {
		return nil, fmt.Errorf("memory table has %d regions but %d fds", len(regions), len(files))
	}
	gm := &guestMemory{}
	for i, r := range regions {
		if r.MemorySize == 0 {
			continue
		}
		// The fd covers the whole backing file; MmapOffset locates this region in it.
		data, err := syscall.Mmap(
			int(files[i].Fd()),
			int64(r.MmapOffset),
			int(r.MemorySize),
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_SHARED|syscall.MAP_NORESERVE,
		)
		if err != nil {
			gm.unmap()
			return nil, fmt.Errorf("mmap guest region %d: %w", i, err)
		}
		gm.regions = append(gm.regions, region{
			guestPhysAddr: r.GuestPhysAddr,
			userAddr:      r.UserAddr,
			size:          r.MemorySize,
			data:          data,
		})
	}
	return gm, nil
}

func (gm *guestMemory) unmap() {
	for _, r := range gm.regions {
		_ = syscall.Munmap(r.data)
	}
	gm.regions = nil
}

// slice returns len bytes of guest memory at guest physical address addr.
func (gm *guestMemory) slice(addr, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	for _, r := range gm.regions {
		if addr < r.guestPhysAddr || addr >= r.guestPhysAddr+r.size {
			continue
		}
		off := addr - r.guestPhysAddr
		if off+length > r.size {
			return nil, fmt.Errorf("range %#x+%#x crosses the end of its region", addr, length)
		}
		return r.data[off : off+length], nil
	}
	return nil, fmt.Errorf("guest address %#x is not mapped", addr)
}

// sliceUserAddr is slice() for an address in the frontend's address space, which
// is how vring addresses arrive.
func (gm *guestMemory) sliceUserAddr(addr, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	for _, r := range gm.regions {
		if addr < r.userAddr || addr >= r.userAddr+r.size {
			continue
		}
		off := addr - r.userAddr
		if off+length > r.size {
			return nil, fmt.Errorf("range %#x+%#x crosses the end of its region", addr, length)
		}
		return r.data[off : off+length], nil
	}
	return nil, fmt.Errorf("frontend address %#x is not mapped", addr)
}
