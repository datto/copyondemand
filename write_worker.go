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

// processWriteQueue reads and processes message from the write queue
// until cancellation is signaled
func processWriteQueue(f *FileBackedDevice) {
	f.terminationWaitGroup.Add(1)
	defer f.terminationWaitGroup.Done()

	for !isCancelSignaled(f.terminationContext) {
		dynamicAction := f.writerQueue.Dequeue(false)

		if dynamicAction != nil {
			// Fast path, consume all actions in the dynamic queue
			// as fast as possible
			handleAction(f, dynamicAction)
		} else {
			copyRate := f.GetBackgroundCopyRate()

			var fetchedAction *QueuedWriteAction
			if f.SampleRate(SourceRead, sampleWindowMilliseconds) > copyRate {
				// We're going too fast to try to consume background blocks. But we might want to start
				// consuming background blocks soon (if the dynamic thread slows down). Poll every 10ms.
				fetchedAction = f.writerQueue.TryDequeue(10, false)
			} else {
				// There was nothing in the dynamic queue when we checked. If we are still copying blocks
				// we will always immediately get a background block here. If we're done copying background blocks
				// and there are no dynamic actions, wake up every 500ms just to check if we're cancelled.
				fetchedAction = f.writerQueue.TryDequeue(500, true)
			}

			if fetchedAction != nil {
				handleAction(f, fetchedAction)
			}
		}
	}
}

func handleAction(f *FileBackedDevice, w *QueuedWriteAction) {
	switch w.actionType {
	case WriteData:
		handleWriteDataAction(f, w)
	case FixDirtyBlock:
		fallthrough
	case SyncBlock:
		handleFixDirtyBlockAction(f, w)
	default:
		f.log.Error("Invalid queued action type, did you implement a handler?")
		panic("Invalid queued action type, did you implement a handler?")
	}

	f.writerQueue.writeActionPool.Put(w)
}

func handleWriteDataAction(f *FileBackedDevice, w *QueuedWriteAction) {
	startBlock := w.startBlock
	endBlock := w.endBlock
	buffer := w.data
	defer f.bufferPool.put(buffer)

	f.log.Debugf("[WriterQueue] Processing write action start:%d end:%d", startBlock, endBlock)

	// Assert that we have a properly sized buffer
	if ((endBlock - startBlock + 1) * BlockSize) != uint64(len(buffer)) {
		f.log.Error("Invalid buffer for queued action")
		panic("Invalid buffer for queued action")
	}

	for i := startBlock; i <= endBlock; i++ {
		currentBufferOffset := (i * BlockSize) - (startBlock * BlockSize)

		// Fast path, the block became synced before we got to it, we don't need to lock
		if f.blockMap.isSynced(i) {
			continue
		}

		f.rangeLocker.LockRange(i, i)
		defer f.rangeLocker.UnlockRange(i, i)

		// The block was synced while we were sleeping in LockRange
		if f.blockMap.isSynced(i) {
			continue
		}

		cleanBlock := buffer[currentBufferOffset:(currentBufferOffset + BlockSize)]
		if f.dirtyBlockMap.isDirty(i) {
			dirtyBlock := f.bufferPool.get(BlockSize)

			err := f.readBacking(dirtyBlock, i*BlockSize)
			if err != nil {
				f.log.Errorf("%s", err)
				return
			}
			f.dirtyBlockMap.reconcileBlock(i, cleanBlock, dirtyBlock)
			f.bufferPool.put(dirtyBlock)
			f.dirtyBlockMap.remove(i)
		}
		err := f.writeBacking(cleanBlock, i*BlockSize)
		if err != nil {
			f.log.Errorf("%s", err)
			return
		}
		f.blockMap.setBlock(i, true)
	}
}

func handleFixDirtyBlockAction(f *FileBackedDevice, w *QueuedWriteAction) {
	// We can only enqueue single block fixes
	dirtyBlockID := w.startBlock

	f.log.Debugf("[WriterQueue] Fixing dirty block:%d", dirtyBlockID)

	if f.blockMap.isSynced(dirtyBlockID) {
		// This block became synced while we were in the queue
		return
	}

	cleanBlock := f.bufferPool.get(BlockSize)
	dirtyBlock := f.bufferPool.get(BlockSize)

	// The source is immutable so we don't need to lock for our source read
	f.readSource(cleanBlock, dirtyBlockID*BlockSize)
	f.rangeLocker.LockRange(dirtyBlockID, dirtyBlockID)
	defer f.rangeLocker.UnlockRange(dirtyBlockID, dirtyBlockID)

	if f.blockMap.isSynced(dirtyBlockID) {
		// This block became synced while we were sleeping in LockRange
		return
	}

	// This allows us to reuse this queue type for background syncs
	if f.dirtyBlockMap.isDirty(dirtyBlockID) {
		err := f.readBacking(dirtyBlock, dirtyBlockID*BlockSize)
		if err != nil {
			f.log.Errorf("%s", err)
			return
		}
		f.dirtyBlockMap.reconcileBlock(dirtyBlockID, cleanBlock, dirtyBlock)
	}

	// Write the now clean block to the backing file
	err := f.writeBacking(cleanBlock, dirtyBlockID*BlockSize)
	if err != nil {
		f.log.Errorf("%s", err)
		return
	}

	// The block is now synced and clean
	f.blockMap.setBlock(dirtyBlockID, true)
	f.dirtyBlockMap.remove(dirtyBlockID)
	f.bufferPool.put(cleanBlock)
	f.bufferPool.put(dirtyBlock)
}
