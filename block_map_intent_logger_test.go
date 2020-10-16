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
	"os"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

const (
	intentLogMockFileSize = BlockSize * 10
)

var intentLogHeader = []byte{
	0x43, 0x4F, 0x44, 0x20, 0x57, 0x49, 0x4C, 0x00,
	0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

var blockMapHeader = []byte{
	0x43, 0x4F, 0x44, 0x20, 0x42, 0x41, 0x4B, 0x00,
	0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

func createIntentLogger(t *testing.T, mockFs FileSystem, log *logrus.Logger) *blockMapIntentLogger {
	bufferPool := newBytePool(log)
	intentLogger := newBlockMapIntentLogger(bufferPool, mockFs, "file.backing", log)

	return intentLogger
}

// BlockMap/DirtyBlockMap serializes are separately unit tested, so we assume they work here
func getSerializedBlockMap(f *FileBackedDevice, syncedBlocks []uint64, dirtyMap map[uint64][]*writeAction) []byte {
	for _, syncedBlock := range syncedBlocks {
		f.blockMap.setBlock(syncedBlock, true)
	}

	for block, dirtyRanges := range dirtyMap {
		for _, dirtyRange := range dirtyRanges {
			f.dirtyBlockMap.recordWrite(block, dirtyRange.off, dirtyRange.len)
		}
	}

	returnBytes := make([]byte, 0)
	blockMapBytes, _ := f.blockMap.serialize()
	returnBytes = append(returnBytes, blockMapHeader...)
	returnBytes = append(returnBytes, blockMapBytes...)
	returnBytes = append(returnBytes, f.dirtyBlockMap.serialize()...)

	return returnBytes
}

// Just for safety, 0 out the block map so that we aren't
// accidentally asserting that the block map is good when nothing
// was actually loaded. This is needed since we use the "real"
// mock FileBackedDevice as scratch space to build the expected binary
// in getSerializedBlockMap
func resetBlockMaps(f *FileBackedDevice) {
	for ix := range f.blockMap.bitmaps {
		f.blockMap.bitmaps[ix] = roaring.New()
	}

	for block := range f.dirtyBlockMap.dirtyMap {
		f.dirtyBlockMap.remove(block)
	}
}

// This function is dumb, it just takes a 2d array of ints
// and serializes them to little endian bytes. There is no
// validation that this 2d array consists of valid intents.
func generateIntent(intents [][]uint64) []byte {
	serializedIntents := make([]byte, 0)

	serializedIntents = append(serializedIntents, intentLogHeader...)

	for _, intent := range intents {
		for _, i := range intent {
			serializedIntents = append(serializedIntents, uint64ToBytes(i)...)
		}
	}

	return serializedIntents
}

func TestNonRestartInitWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)
}

func TestHappyPathRestartInitWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	mockFs.On("ReadFile", "file.backing.bak").Return(getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks), nil)
	mockFs.On("Rename", "file.backing.bak", "file.backing.bak.1").Return(nil)
	mockFs.On("Remove", "file.backing.bak.1").Return(nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	resetBlockMaps(fileBackedDevice)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	assert.Equal(t, 1, len(reflect.ValueOf(fileBackedDevice.dirtyBlockMap.dirtyMap).MapKeys()))
	assert.Equal(t, true, fileBackedDevice.dirtyBlockMap.isDirty(5))
	assert.Equal(t, uint64(0), fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].off)
	assert.Equal(t, 10, fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].len)
	assert.Equal(t, uint64(11), fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].off)
	assert.Equal(t, 22, fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].len)
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(3))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(4))
	assert.Equal(t, uint64(3), fileBackedDevice.blockMap.bitmaps[0].GetCardinality())
	assert.Equal(t, 1, len(fileBackedDevice.blockMap.bitmaps))
}

func TestRestartWithFailoverInitWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	mockFs.On("ReadFile", "file.backing.bak.1").Return(getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks), nil)
	mockFs.On("Remove", "file.backing.bak.1").Return(nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	resetBlockMaps(fileBackedDevice)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	assert.Equal(t, 1, len(reflect.ValueOf(fileBackedDevice.dirtyBlockMap.dirtyMap).MapKeys()))
	assert.Equal(t, true, fileBackedDevice.dirtyBlockMap.isDirty(5))
	assert.Equal(t, uint64(0), fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].off)
	assert.Equal(t, 10, fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].len)
	assert.Equal(t, uint64(11), fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].off)
	assert.Equal(t, 22, fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].len)
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(3))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(4))
	assert.Equal(t, uint64(3), fileBackedDevice.blockMap.bitmaps[0].GetCardinality())
	assert.Equal(t, 1, len(fileBackedDevice.blockMap.bitmaps))
}

func TestRestartWithBothBlockMapsInitWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	mockFs.On("ReadFile", "file.backing.bak.1").Return(getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks), nil)
	mockFs.On("Remove", "file.backing.bak.1").Return(nil)
	mockFs.On("Rename", "file.backing.bak.1", "file.backing.bak").Return(nil)
	mockFs.On("Remove", "file.backing.bak").Return(nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	resetBlockMaps(fileBackedDevice)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	assert.Equal(t, 1, len(reflect.ValueOf(fileBackedDevice.dirtyBlockMap.dirtyMap).MapKeys()))
	assert.Equal(t, true, fileBackedDevice.dirtyBlockMap.isDirty(5))
	assert.Equal(t, uint64(0), fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].off)
	assert.Equal(t, 10, fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].len)
	assert.Equal(t, uint64(11), fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].off)
	assert.Equal(t, 22, fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].len)
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(3))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(4))
	assert.Equal(t, uint64(3), fileBackedDevice.blockMap.bitmaps[0].GetCardinality())
	assert.Equal(t, 1, len(fileBackedDevice.blockMap.bitmaps))
}

func TestRestartInitWorksWithIntents(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	mockFs.On("ReadFile", "file.backing.bak").Return(getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks), nil)
	mockFs.On("ReadFile", "file.backing.wil").Return(generateIntent(
		[][]uint64{
			[]uint64{blockWriteID, 2},
			[]uint64{blockWriteID, 3},
			[]uint64{blockWriteID, 4},
			[]uint64{dirtyBlockWriteID, 5, 0, 10},
			[]uint64{dirtyBlockWriteID, 5, 11, 22},
		},
	), nil)
	mockFs.On("Rename", "file.backing.bak", "file.backing.bak.1").Return(nil)
	mockFs.On("Remove", "file.backing.bak.1").Return(nil)
	mockFs.On("Remove", "file.backing.wil").Return(nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	resetBlockMaps(fileBackedDevice)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	assert.Equal(t, 1, len(reflect.ValueOf(fileBackedDevice.dirtyBlockMap.dirtyMap).MapKeys()))
	assert.Equal(t, true, fileBackedDevice.dirtyBlockMap.isDirty(5))
	assert.Equal(t, uint64(0), fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].off)
	assert.Equal(t, 10, fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].len)
	assert.Equal(t, uint64(11), fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].off)
	assert.Equal(t, 22, fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].len)
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(3))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(4))
	assert.Equal(t, uint64(3), fileBackedDevice.blockMap.bitmaps[0].GetCardinality())
	assert.Equal(t, 1, len(fileBackedDevice.blockMap.bitmaps))
}

func TestRestartInitWorksWithFailoverIntents(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, nil)
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, nil)
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	mockFs.On("ReadFile", "file.backing.bak").Return(getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks), nil)
	mockFs.On("ReadFile", "file.backing.wil").Return(generateIntent(
		[][]uint64{
			[]uint64{blockWriteID, 2},
			[]uint64{dirtyBlockWriteID, 5, 0, 10},
			[]uint64{dirtyBlockWriteID, 5, 11, 22},
			[]uint64{blockWriteID, 4},
		},
	), nil)
	mockFs.On("ReadFile", "file.backing.wil.1").Return(generateIntent(
		[][]uint64{
			[]uint64{blockWriteID, 3},
			// This block will be cleaned by the sync of block 2 in the .wil
			[]uint64{dirtyBlockWriteID, 2, 0, 10},
			[]uint64{dirtyBlockWriteID, 2, 11, 22},
		},
	), nil)
	mockFs.On("Rename", "file.backing.bak", "file.backing.bak.1").Return(nil)
	mockFs.On("Remove", "file.backing.bak.1").Return(nil)
	mockFs.On("Remove", "file.backing.wil").Return(nil)
	mockFs.On("Remove", "file.backing.wil.1").Return(nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	resetBlockMaps(fileBackedDevice)

	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	assert.Equal(t, 1, len(reflect.ValueOf(fileBackedDevice.dirtyBlockMap.dirtyMap).MapKeys()))
	assert.Equal(t, true, fileBackedDevice.dirtyBlockMap.isDirty(5))
	assert.Equal(t, uint64(0), fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].off)
	assert.Equal(t, 10, fileBackedDevice.dirtyBlockMap.dirtyMap[5][0].len)
	assert.Equal(t, uint64(11), fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].off)
	assert.Equal(t, 22, fileBackedDevice.dirtyBlockMap.dirtyMap[5][1].len)
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(2))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(3))
	assert.Equal(t, true, fileBackedDevice.blockMap.isSynced(4))
	assert.Equal(t, uint64(3), fileBackedDevice.blockMap.bitmaps[0].GetCardinality())
	assert.Equal(t, 1, len(fileBackedDevice.blockMap.bitmaps))
}

func TestFirstFlushWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockPrimaryBlockMap := new(MockFile)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.flushIntent(fileBackedDevice, true)
}

func TestSubsequentFlushesWork(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockPrimaryBlockMap := new(MockFile)
	mockFs := new(MockFs)
	setBlocks := []uint64{2, 3, 4}
	dirtyBlocks := map[uint64][]*writeAction{
		5: []*writeAction{
			&writeAction{0, 10},
			&writeAction{11, 22},
		},
	}
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	getSerializedBlockMap(fileBackedDevice, setBlocks, dirtyBlocks)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.flushIntent(fileBackedDevice, true)
}

func TestTransactionFlushWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockPrimaryWil := new(MockFile)
	mockPrimaryBlockMap := new(MockFile)
	mockFileInfo := new(MockFileInfo)
	mockFs := new(MockFs)
	mockFs.On("Stat", "file.backing.bak").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.bak.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("Stat", "file.backing.wil.1").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))
	mockFs.On("OpenFile", "file.backing.wil", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryWil, nil)
	mockFs.On("OpenFile", "file.backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockPrimaryBlockMap, nil)
	mockFileInfo.On("Name").Return("file.backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockPrimaryWil.On("Write", intentLogHeader).Return(0, nil)
	mockSourceFile.On("Fd").Return(1)

	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, intentLogMockFileSize)
	serializedBlockMap, _ := fileBackedDevice.blockMap.serialize()
	mockPrimaryBlockMap.On("Write", blockMapHeader).Return(0, nil)
	mockPrimaryBlockMap.On("Write", serializedBlockMap).Return(0, nil)
	mockPrimaryBlockMap.On("Write", fileBackedDevice.dirtyBlockMap.serialize()).Return(0, nil)
	mockPrimaryBlockMap.On("Close").Return(nil)
	intentLogger := createIntentLogger(t, mockFs, logger)
	intentLogger.init(fileBackedDevice)

	// Note: This test is a little bit testing 1 = 1.
	// However, transaction serialization is tested in the transaction
	// tests, so we're really just confirming that flushing the
	// transaction indeed writes out the serialized transaction
	transaction := intentLogger.getWriteTransaction()
	transaction.setStartBlock(1)
	transaction.setEndBlock(4)
	transaction.recordDirtyWrite(intentLogger, 5, 0, 100)
	transaction.recordDirtyWrite(intentLogger, 6, 101, 100)
	expectedWrite := transaction.serialize(intentLogger.bufferPool)
	mockPrimaryWil.On("Write", expectedWrite).Return(0, nil)
	intentLogger.flushWriteTransaction(transaction)
}
