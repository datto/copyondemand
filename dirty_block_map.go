/* This file is part of copyondemand.
 *
 * Copyright © 2020 Datto, Inc.
 * Author: Bryan Ehrlich <behrlich@datto.com>
 *
 * Licensed under the Apache Software License, Version 2.0
 * Fedora-License-Identifier: ASL 2.0
 * SPDX-2.0-License-Identifier: Apache-2.0
 * SPDX-3.0-License-Identifier: Apache-2.0
 *
 * copyondemand is free software.
 * For more information on the license, see LICENSE.
 * For more information on free software, see <https://www.gnu.org/philosophy/free-sw.en.html>.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package copyondemand

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	dirtyBlockFmt = "%d,%d,%d\n"
)

type writeAction struct {
	off uint64
	len int
}

// dirtyBlockMap contains a thread safe map of
// partially written blocks, and helper functions
// to reconcile these partially written states.
type dirtyBlockMap struct {
	dirtyMap        map[uint64][]*writeAction
	lock            *sync.Mutex
	writeActionPool *sync.Pool
	logger          *logrus.Logger
}

// newDirtyBlockMap initializes a DirtyBlockMap
func newDirtyBlockMap(log *logrus.Logger) *dirtyBlockMap {
	writeActionPool := &sync.Pool{
		New: func() interface{} {
			return &writeAction{}
		},
	}

	return &dirtyBlockMap{
		make(map[uint64][]*writeAction),
		&sync.Mutex{},
		writeActionPool,
		log,
	}
}

// recordWrite initializes a dirty block and
// "cleans" a portion of the block that was written to.
// If the block is still dirty from previous writes, this
// function will merge any offsets into existing clean
// ranges (if possible!). Callers are responsible for trimming
// any write overflow (i.e. off + len can't overflow BlockSize)
func (dm *dirtyBlockMap) recordWrite(block uint64, off uint64, len int) {
	if off+uint64(len) > BlockSize {
		dm.logger.Errorf("%d overflows block size", off+uint64(len))
		panic(fmt.Sprintf("%d overflows block size", off+uint64(len)))
	}

	dm.lock.Lock()
	defer dm.lock.Unlock()
	existingWrites := dm.dirtyMap[block]

	if existingWrites != nil {
		for _, existingWrite := range existingWrites {
			if mergeWrite(existingWrite, off, len) {
				// This write overlapped with an existing interval
				return
			}
		}
	} else {
		dm.dirtyMap[block] = make([]*writeAction, 0)
	}

	newWrite := dm.makeWrite(off, len)
	dm.dirtyMap[block] = append(dm.dirtyMap[block], newWrite)
}

// isDirty returns true if the given block is in the dirty block map
func (dm *dirtyBlockMap) isDirty(block uint64) bool {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	dirtyWrites := dm.dirtyMap[block]

	return dirtyWrites != nil
}

// allDirtyBlocks returns all the currently dirty blocks
func (dm *dirtyBlockMap) allDirtyBlocks() []uint64 {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	dirtyBlocks := make([]uint64, len(dm.dirtyMap))
	ix := 0
	for block := range dm.dirtyMap {
		dirtyBlocks[ix] = block
		ix++
	}

	return dirtyBlocks
}

// reconcileBlock takes a block of source data, a block of target data, and
// applies any clean ranges (i.e. ranges written to target) back to the source.
func (dm *dirtyBlockMap) reconcileBlock(block uint64, sourceData []byte, backingData []byte) {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	if writeActions, ok := dm.dirtyMap[block]; ok {
		for _, writeAction := range writeActions {
			copy(
				sourceData[writeAction.off:(writeAction.off+uint64(writeAction.len))],
				backingData[writeAction.off:(writeAction.off+uint64(writeAction.len))],
			)
		}
	} // Else there was no reconciliation to do
}

// remove removes a block, and all its associated write ranges, from the dirty block map
func (dm *dirtyBlockMap) remove(block uint64) {
	dm.lock.Lock()
	defer dm.lock.Unlock()

	writes := dm.dirtyMap[block]

	for _, write := range writes {
		dm.writeActionPool.Put(write)
	}

	delete(dm.dirtyMap, block)
}

func (dm *dirtyBlockMap) serialize() []byte {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	returnBytes := make([]byte, 0)

	for block, dirtyRanges := range dm.dirtyMap {
		for _, dirtyRange := range dirtyRanges {
			returnBytes = append(returnBytes, uint64ToBytes(1)...)
			returnBytes = append(returnBytes, uint64ToBytes(block)...)
			returnBytes = append(returnBytes, uint64ToBytes(dirtyRange.off)...)
			returnBytes = append(returnBytes, uint64ToBytes(uint64(dirtyRange.len))...)
		}
	}

	returnBytes = append(returnBytes, uint64ToBytes(0)...)

	return returnBytes
}

// mergeWrite merges a write action with an existing write action
// returns false if the write actions could not be merged
func mergeWrite(existingAction *writeAction, off uint64, len int) bool {
	existingEnd := existingAction.off + uint64(existingAction.len)
	newEnd := off + uint64(len)

	if off >= existingAction.off && off <= existingEnd {
		// New write is within, or extends existing action
		writeEnd := maxUInt(existingEnd, newEnd)
		existingAction.len = int(writeEnd - existingAction.off)

		return true
	} else if off < existingAction.off && newEnd >= existingAction.off {
		// Existing write is within, or extends current write
		writeEnd := maxUInt(existingEnd, newEnd)
		// Write now starts at current write offset, since our existing write is within the new write
		existingAction.off = off
		existingAction.len = int(writeEnd - off)

		return true
	}

	return false
}

func (dm *dirtyBlockMap) makeWrite(off uint64, len int) *writeAction {
	write := dm.writeActionPool.Get().(*writeAction)
	write.off = off
	write.len = len

	return write
}
