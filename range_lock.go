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
)

// RangeLocker implements "row level" locking for ranges of elements
type RangeLocker struct {
	lockedRanges   []*BlockRange
	rangeLockCond  *sync.Cond
	blockRangePool *sync.Pool
}

// NewRangeLocker returns a new RangeLocker
func NewRangeLocker(blockRangePool *sync.Pool) *RangeLocker {
	lockedRanges := make([]*BlockRange, 0)
	m := &sync.Mutex{}
	cond := sync.NewCond(m)

	return &RangeLocker{
		lockedRanges,
		cond,
		blockRangePool,
	}
}

// LockRange locks the range of elements defined
// by start and end (both inclusive). This function
// blocks until it can obtain an exclusive lock on
// the provided range
func (rl *RangeLocker) LockRange(start uint64, end uint64) {
	currentRange := rl.blockRangePool.Get().(*BlockRange)
	currentRange.Start = start
	currentRange.End = end

	rl.rangeLockCond.L.Lock()

	for rl.existsConflictingRange(currentRange) {
		rl.rangeLockCond.Wait()
	}

	rl.lockedRanges = append(rl.lockedRanges, currentRange)
	rl.rangeLockCond.L.Unlock()
}

// UnlockRange unlocks an existing range lock, and
// returns error if no matching range is found
func (rl *RangeLocker) UnlockRange(start uint64, end uint64) error {
	rl.rangeLockCond.L.Lock()
	defer rl.rangeLockCond.L.Unlock()
	currentRange := rl.blockRangePool.Get().(*BlockRange)
	currentRange.Start = start
	currentRange.End = end
	// currentRange is a temp variable so we always want to
	// add it back to the pool
	defer rl.blockRangePool.Put(currentRange)

	for i, lockedRange := range rl.lockedRanges {
		if (lockedRange.Start == currentRange.Start) && (lockedRange.End == currentRange.End) {
			if len(rl.lockedRanges) == 1 {
				rl.lockedRanges = rl.lockedRanges[:0]
			} else {
				rl.lockedRanges[i] = rl.lockedRanges[len(rl.lockedRanges)-1]
				rl.lockedRanges[len(rl.lockedRanges)-1] = nil
				rl.lockedRanges = rl.lockedRanges[:len(rl.lockedRanges)-1]
			}
			rl.blockRangePool.Put(lockedRange)
			// Wake up waiters
			rl.rangeLockCond.Broadcast()
			return nil
		}
	}

	return fmt.Errorf("Range did not exist")
}

func (rl *RangeLocker) existsConflictingRange(rangeToCheck *BlockRange) bool {
	for _, lockedRange := range rl.lockedRanges {
		if (lockedRange.Start <= rangeToCheck.End) && (lockedRange.End >= rangeToCheck.Start) {
			return true
		}
	}
	return false
}
