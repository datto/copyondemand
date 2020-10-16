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
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	sampleWindowMilliseconds      = 1000 // What bandwidth window is considered when making queueing decisions
	queuePollIntervalMilliseconds = 10   // Poll at this interval if the queue is full
)

// SyncAllBlocks transfers all blocks from the source device to the backing device
func syncAllBlocks(fileBackedDevice *FileBackedDevice) {
	fileBackedDevice.terminationWaitGroup.Add(1)
	defer fileBackedDevice.terminationWaitGroup.Done()

	// Before we start syncing blocks, scan for any sparse ranges that we can mark as synced
	scanSparseFile(fileBackedDevice)

	totalBlocks := (fileBackedDevice.Source.Size / BlockSize) + 1
	blockIndex := uint64(0)
	for blockIndex < totalBlocks && !isCancelSignaled(fileBackedDevice.terminationContext) {
		if fileBackedDevice.blockMap.isSynced(blockIndex) {
			blockIndex++
			continue
		}

		wa := fileBackedDevice.writerQueue.writeActionPool.Get().(*QueuedWriteAction)
		wa.actionType = SyncBlock
		wa.startBlock = blockIndex

		if syncBlock(fileBackedDevice, blockIndex) {
			blockIndex++
		}
	}

	// Done queueing blocks, or cancel signaled
	syncedBlockIndex := uint64(0)
	for syncedBlockIndex < totalBlocks && !isCancelSignaled(fileBackedDevice.terminationContext) {
		if fileBackedDevice.blockMap.isSynced(syncedBlockIndex) {
			syncedBlockIndex++
		} else {
			// Slowly poll the lowest unsynced block, until it becomes synced
			time.Sleep(100 * time.Millisecond)
			syncBlock(fileBackedDevice, syncedBlockIndex)
		}
	}

	if syncedBlockIndex == totalBlocks {
		logrus.Info("Finished copying backing file")
		fileBackedDevice.SetSynced()
	} else {
		logrus.Info("Terminating syncing all blocks")
	}
}

func syncBlock(fileBackedDevice *FileBackedDevice, blockID uint64) bool {
	wa := fileBackedDevice.writerQueue.writeActionPool.Get().(*QueuedWriteAction)
	wa.actionType = SyncBlock
	wa.startBlock = blockID

	if fileBackedDevice.writerQueue.TryEnqueue(wa, 500) {
		return true
	}

	fileBackedDevice.writerQueue.writeActionPool.Put(wa)

	return false
}

// getAffectedBlockRange fills the affectedBlocks blockRange with the start and and blocks
func getAffectedBlockRange(len uint64, off uint64, affectedBlocks *BlockRange) {
	affectedBlocks.Start = off / BlockSize
	affectedBlocks.End = (off + (len - 1)) / BlockSize
}

// isBlockAligned returns true if the provided _byte_ index is the first byte in a block
func isBlockAligned(ix uint64) bool {
	return ix%BlockSize == 0
}

func isCancelSignaled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		logrus.Info("Got cancellation signal")
		return true
	default:
		// Do nothing if there is no signal to cancel
		return false
	}
}
