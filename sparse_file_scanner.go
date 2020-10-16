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
	"syscall"
)

const (
	seekData = 3
	seekHole = 4
)

var sysSeek = syscall.Seek

// ScanSparseFile scans the source file, looking for holes that it can consider synced
func scanSparseFile(fileBackedDevice *FileBackedDevice) {
	// If we don't have a file descriptor, we cannot find holes
	if fileBackedDevice.Source.File.Fd() == 0 {
		return
	}

	sourceFileDescriptor := int(fileBackedDevice.Source.File.Fd())
	totalSize := fileBackedDevice.Source.Size
	currentOffset := int64(0)
	currentSeekMode := seekData

	for currentOffset < (int64(totalSize) - 1) {
		newOffset, err := sysSeek(sourceFileDescriptor, currentOffset, currentSeekMode)
		if err != nil {
			fileBackedDevice.log.Error(err)
			break
		}

		if currentOffset == newOffset {
			// This should only happen at offset = 0, but this means the offset is inside
			// whatever we're looking for. E.g. SEEK_DATA, but the offset is in data already!
			currentSeekMode = swapSeek(currentSeekMode)
			continue
		}

		if currentSeekMode == seekData {
			// Since we're seeking data, we are exiting a hole, therefore currentOffset => newOffset is a hole
			syncHole(fileBackedDevice, uint64(currentOffset), uint64(newOffset-1))
		}

		currentSeekMode = swapSeek(currentSeekMode)
		currentOffset = newOffset
	}
}

func syncHole(fileBackedDevice *FileBackedDevice, startByte uint64, endByte uint64) {
	affectedBlocks := &BlockRange{}
	len := endByte - startByte + 1
	getAffectedBlockRange(len, startByte, affectedBlocks)

	for i := affectedBlocks.Start + 1; i < affectedBlocks.End; i++ {
		fileBackedDevice.blockMap.setBlock(i, true)
	}

	if isBlockAligned(startByte) && len >= BlockSize {
		fileBackedDevice.blockMap.setBlock(affectedBlocks.Start, true)
	}

	if isBlockAligned(endByte+1) && len >= BlockSize {
		fileBackedDevice.blockMap.setBlock(affectedBlocks.End, true)
	}
}

func swapSeek(currentSeekMode int) (newSeekMode int) {
	if currentSeekMode == seekData {
		return seekHole
	}

	return seekData
}
