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

const (
	blockSyncIntentSize      = 8 * 2 // 2 64 bit ints
	dirtyBlockSyncIntentSize = 8 * 4 // 4 64 bit ints
)

type blockWriteAction struct {
	block uint64
	off   uint64
	len   int
}

// blockMapIntentTransaction is a memory-efficient struct
// for keeping track of a single write action, which can
// consist of many (contigous) block syncs, as well as up
// to 2 dirty block writes
type blockMapIntentTransaction struct {
	startBlock              uint64
	endBlock                uint64
	leadingDirtyBlockWrite  *blockWriteAction
	trailingDirtyBlockWrite *blockWriteAction
}

func (t *blockMapIntentTransaction) reset() {
	t.startBlock = 0
	t.endBlock = 0
	t.leadingDirtyBlockWrite = nil
	t.trailingDirtyBlockWrite = nil
}

// setStartBlock sets the beginning of the write range (inclusive)
func (t *blockMapIntentTransaction) setStartBlock(block uint64) {
	t.startBlock = block
}

// setEndBlock sets the end of the write range (inclusive)
func (t *blockMapIntentTransaction) setEndBlock(block uint64) {
	t.endBlock = block
}

// recordDirtyWrite records a dirty write. Attempting to record more than 2 dirty writes for a single transaction panics
func (t *blockMapIntentTransaction) recordDirtyWrite(intentLogger *blockMapIntentLogger, block, off uint64, len int) {
	blockAction := intentLogger.writeActionPool.Get().(*blockWriteAction)
	blockAction.block = block
	blockAction.off = off
	blockAction.len = len
	if t.leadingDirtyBlockWrite == nil {
		t.leadingDirtyBlockWrite = blockAction

		return
	} else if t.trailingDirtyBlockWrite == nil {
		t.trailingDirtyBlockWrite = blockAction

		return
	}

	panic("Attempted to record more than 2 dirty write actions from a single write")
}

func (t *blockMapIntentTransaction) serialize(pool *bytePool) []byte {
	serializedSize := uint64(0)
	if t.startBlock <= t.endBlock {
		serializedSize += ((t.endBlock - t.startBlock) + 1) * blockSyncIntentSize
	}

	if t.leadingDirtyBlockWrite != nil {
		serializedSize += dirtyBlockSyncIntentSize
	}

	if t.trailingDirtyBlockWrite != nil {
		serializedSize += dirtyBlockSyncIntentSize
	}

	if serializedSize == 0 {
		return nil
	}

	serializedBytes := pool.get(serializedSize)
	currentOffset := uint64(0)

	for i := t.startBlock; i <= t.endBlock; i++ {
		currentOffset = applyUint64AtOffset(blockWriteID, serializedBytes, currentOffset)
		currentOffset = applyUint64AtOffset(i, serializedBytes, currentOffset)
	}

	if t.leadingDirtyBlockWrite != nil {
		currentOffset = recordDirtyWriteAction(serializedBytes, t.leadingDirtyBlockWrite, currentOffset)
	}

	if t.trailingDirtyBlockWrite != nil {
		currentOffset = recordDirtyWriteAction(serializedBytes, t.trailingDirtyBlockWrite, currentOffset)
	}

	return serializedBytes
}

func recordDirtyWriteAction(serializedBytes []byte, action *blockWriteAction, currentOffset uint64) uint64 {
	currentOffset = applyUint64AtOffset(dirtyBlockWriteID, serializedBytes, currentOffset)
	currentOffset = applyUint64AtOffset(action.block, serializedBytes, currentOffset)
	currentOffset = applyUint64AtOffset(action.off, serializedBytes, currentOffset)
	currentOffset = applyUint64AtOffset(uint64(action.len), serializedBytes, currentOffset)

	return currentOffset
}
