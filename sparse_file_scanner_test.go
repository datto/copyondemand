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
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNonSparseFileDoesntMark(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData {
			return 0, nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(0), mockFileBackedDevice.TotalSyncedBlocks())
}

func TestBlockAlignedHoleIsRecorded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize, nil
		}

		if whence == seekData && offset == BlockSize {
			return BlockSize * 2, nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(1), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(1))
}

func TestBlockNonCoveringHoleIsNotRecorded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize, nil
		}

		if whence == seekData && offset == BlockSize {
			return BlockSize + (BlockSize - 1), nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(0), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, false, mockFileBackedDevice.blockMap.isSynced(1))
}

func TestMisalignedHoleIsProperlyRecorded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize - 1, nil
		}

		if whence == seekData && offset == BlockSize-1 {
			return BlockSize + (BlockSize + 1), nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(1), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(1))
}

func TestTrailingHoleIsProperlyRecorded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return int64(fileSize) - BlockSize, nil
		}

		if whence == seekData && offset == int64(fileSize)-BlockSize {
			return int64(fileSize), nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(1), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(9))
}

func TestMultiBlockMisalignedHoleIsProperlyRecordedLeading(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize + 1, nil
		}

		if whence == seekData && offset == BlockSize+1 {
			return BlockSize * 4, nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(2), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, false, mockFileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(3))
}

func TestMultiBlockMisalignedHoleIsProperlyRecordedTrailing(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize, nil
		}

		if whence == seekData && offset == BlockSize {
			return BlockSize*4 - 1, nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(2), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, false, mockFileBackedDevice.blockMap.isSynced(3))
}

func TestMultiBlockAlignedHoleIsProperlyRecorded(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileSize := BlockSize * 10
	sysSeek = func(fd int, offset int64, whence int) (off int64, err error) {
		if whence == seekData && offset == 0 {
			return 0, nil
		}

		if whence == seekHole && offset == 0 {
			return BlockSize, nil
		}

		if whence == seekData && offset == BlockSize {
			return BlockSize * 4, nil
		}

		return int64(fileSize - 1), nil
	}
	mockFileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, uint64(fileSize))
	scanSparseFile(mockFileBackedDevice)

	assert.Equal(t, uint64(3), mockFileBackedDevice.TotalSyncedBlocks())
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, mockFileBackedDevice.blockMap.isSynced(3))
}
