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
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func newBlockRangePool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return &BlockRange{}
		},
	}
}

func CreateMockDevice(t *testing.T, mockSourceFile *MockFile, mockBackingFile *MockFile, log *logrus.Logger, size uint64) *FileBackedDevice {
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFs.On("Stat", "backing").Return(mockFileInfo, fmt.Errorf("File doesn't exist! (expected)"))
	mockFs.On("Stat", ".").Return(mockFileInfo, nil)
	mockFileInfo.On("Size").Return(int64(size))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockSourceFile, nil)
	mockFs.On("OpenFile", "backing", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockBackingFile, nil)
	mockFileInfo.On("Name").Return("backing")
	mockBackingFile.On("Stat").Return(mockFileInfo, nil)
	mockBackingFile.On("Truncate", int64(size)).Return(nil)

	fileBackedDevice, err := NewFileBackedDevice(
		"source",
		"backing",
		"/dev/nbd0",
		nil,
		mockFs,
		log,
		false,
		false,
	)
	assert.Equal(t, err, nil)

	return fileBackedDevice
}

func fill1s(b []byte) []byte {
	for index := range b {
		b[index] = 1
	}

	return b
}

func TestCreateDeviceFailsIfSourceDoesntExist(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockFs.On("Stat", "source").Return(mockFileInfo, fmt.Errorf("Doesn't exist"))

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "source file source does not exist")
}

func TestCreateDeviceFailsIfSourceNotOpened(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockFile := new(MockFile)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFileInfo.On("Size").Return(int64(1337))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockFile, fmt.Errorf("Something went wrong"))

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "cannot open provided file: Something went wrong")
}

func TestCreateDeviceFailsIfBackingFileExists(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockSourceFile := new(MockFile)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFs.On("Stat", "backing").Return(mockFileInfo, nil)
	mockFileInfo.On("Size").Return(int64(1337))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockSourceFile, nil)
	mockSourceFile.On("Fd").Return(1)

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "the backing file backing already exists")
}

func TestCreateDeviceFailsIfBackingFileRootDirDoesntExist(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockSourceFile := new(MockFile)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFs.On("Stat", "backing").Return(mockFileInfo, fmt.Errorf("File doesn't exist! (expected)"))
	mockFs.On("Stat", ".").Return(mockFileInfo, fmt.Errorf("Backing file root doesn't exist"))
	mockFileInfo.On("Size").Return(int64(1337))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockSourceFile, nil)
	mockFs.On("OpenFile", "backing", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(nil, "Can't open backing file")
	mockSourceFile.On("Fd").Return(1)

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "the backing file root directory . does not exist")
}

func TestCreateDeviceFailsIfBackingFileDoesntOpen(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFs.On("Stat", "backing").Return(mockFileInfo, fmt.Errorf("File doesn't exist! (expected)"))
	mockFs.On("Stat", ".").Return(mockFileInfo, nil)
	mockFileInfo.On("Size").Return(int64(1337))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockSourceFile, nil)
	mockFs.On("OpenFile", "backing", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockBackingFile, fmt.Errorf("Can't open backing file"))
	mockSourceFile.On("Fd").Return(1)

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "Cannot open backing file: Can't open backing file")
}

func TestCreateDeviceFailsIfBackingFileDoesntTruncate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockFs := new(MockFs)
	mockFileInfo := new(MockFileInfo)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockFs.On("Stat", "source").Return(mockFileInfo, nil)
	mockFs.On("Stat", "backing").Return(mockFileInfo, fmt.Errorf("File doesn't exist! (expected)"))
	mockFs.On("Stat", ".").Return(mockFileInfo, nil)
	mockFileInfo.On("Size").Return(int64(1337))
	mockFs.On("OpenFile", "source", os.O_RDWR, os.FileMode(int(0755))).Return(mockSourceFile, nil)
	mockFs.On("OpenFile", "backing", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755))).Return(mockBackingFile, nil)
	mockBackingFile.On("Truncate", int64(1337)).Return(fmt.Errorf("Can't truncate"))
	mockSourceFile.On("Fd").Return(1)

	_, err := NewFileBackedDevice("source", "backing", "/dev/nbd0", nil, mockFs, logger, false, false)

	assert.Equal(t, err.Error(), "could not truncate backing file: Can't truncate")
}

func TestCreateDeviceWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, 1337)

	assert.Equal(t, fileBackedDevice.Source.File, mockSourceFile)
	assert.Equal(t, fileBackedDevice.Source.Size, uint64(1337))
	assert.Equal(t, fileBackedDevice.BackingFile.File, mockBackingFile)
	assert.Equal(t, fileBackedDevice.BackingFile.Size, uint64(1337))
	assert.Equal(t, fileBackedDevice.IsFullySynced(), false)
	assert.Equal(t, fileBackedDevice.blockMap.size, uint64((1337/BlockSize)+1))
}

func TestFileTrim(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, 1337)

	err := fileBackedDevice.Trim(100, 1234)

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, "[FileBackedDevice] TRIM offset:100 len:1234", hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestFileFlushFail(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, 1337)
	mockBackingFile.On("Sync").Return(fmt.Errorf("Failed to sync"))

	err := fileBackedDevice.Flush()

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, "[FileBackedDevice] FLUSH", hook.LastEntry().Message)
	assert.Equal(t, err.Error(), "could not sync backing file: Failed to sync")
}

func TestFileFlush(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, 1337)
	mockBackingFile.On("Sync").Return(nil)

	err := fileBackedDevice.Flush()

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, "[FileBackedDevice] FLUSH", hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestDisconnect(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, 1337)

	fileBackedDevice.DriverDisconnect()

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, "[FileBackedDevice] DISCONNECT", hook.LastEntry().Message)
}

func TestReadSynced(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toRead := make([]byte, BlockSize/2)
	expectedRead := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.output = 1
	mockBackingFile.On("ReadAt", toRead, int64(0)).Return(len(toRead), nil)
	fileBackedDevice.SetSynced()

	err := fileBackedDevice.ReadAt(toRead, 0)

	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestReadSyncedFailure(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toRead := make([]byte, BlockSize/2)
	expectedRead := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.output = 1
	mockBackingFile.On("ReadAt", toRead, int64(0)).Return(len(toRead), fmt.Errorf("Could not read"))
	fileBackedDevice.SetSynced()

	err := fileBackedDevice.ReadAt(toRead, 0)

	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err.Error(), "could not read from backing file at offset 0: Could not read")
}

func TestReadCacheHit(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toRead := make([]byte, BlockSize/2)
	expectedRead := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.On("ReadAt", toRead, int64(0)).Return(len(toRead), nil)
	mockBackingFile.output = 1
	fileBackedDevice.blockMap.setBlock(0, true)

	err := fileBackedDevice.ReadAt(toRead, 0)

	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestReadCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toSourceRead := make([]byte, BlockSize)
	toRead := make([]byte, BlockSize/2)
	expectedRead := fill1s(make([]byte, BlockSize/2))
	mockSourceFile.output = 1
	mockSourceFile.On("ReadAt", toSourceRead, int64(0)).Return(len(toSourceRead), nil)

	err := fileBackedDevice.ReadAt(toRead, 0)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, WriteData, queuedWrite.actionType)
	assert.Equal(t, fill1s(toSourceRead), queuedWrite.data)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.Equal(t, uint64(0), queuedWrite.endBlock)
	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestLargeReadCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toSourceRead := make([]byte, BlockSize*2)
	toRead := make([]byte, BlockSize)
	expectedRead := fill1s(make([]byte, BlockSize))
	mockSourceFile.output = 1
	mockSourceFile.On("ReadAt", toSourceRead, int64(0)).Return(len(toSourceRead), nil)

	err := fileBackedDevice.ReadAt(toRead, BlockSize/2)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, WriteData, queuedWrite.actionType)
	assert.Equal(t, fill1s(toSourceRead), queuedWrite.data)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.Equal(t, uint64(1), queuedWrite.endBlock)
	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", BlockSize/2, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestExtraLargeReadCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toSourceRead := make([]byte, BlockSize*3)
	toRead := make([]byte, BlockSize*2)
	expectedRead := fill1s(make([]byte, BlockSize*2))
	mockSourceFile.output = 1
	mockSourceFile.On("ReadAt", toSourceRead, int64(0)).Return(len(toSourceRead), nil)

	err := fileBackedDevice.ReadAt(toRead, BlockSize/2)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, WriteData, queuedWrite.actionType)
	assert.Equal(t, fill1s(toSourceRead), queuedWrite.data)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.Equal(t, uint64(2), queuedWrite.endBlock)
	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, expectedRead, toRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", BlockSize/2, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestExtraLargeReadCacheMissWithSomeSynced(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*5)
	toSourceRead := make([]byte, BlockSize*3)
	toBackingRead := make([]byte, BlockSize)
	toRead := make([]byte, BlockSize*5)
	expectedRead := fill1s(make([]byte, BlockSize*5))
	fileBackedDevice.blockMap.setBlock(0, true)
	fileBackedDevice.blockMap.setBlock(4, true)
	fill(expectedRead[0:BlockSize], 2)
	fill(expectedRead[(BlockSize*4):(BlockSize*5)], 2)
	mockSourceFile.output = 1
	mockBackingFile.output = 2
	mockSourceFile.On("ReadAt", toSourceRead, int64(BlockSize)).Return(len(toSourceRead), nil)
	mockBackingFile.On("ReadAt", toBackingRead, int64(0)).Return(len(toBackingRead), nil)
	mockBackingFile.On("ReadAt", toBackingRead, int64(BlockSize*4)).Return(len(toBackingRead), nil)

	err := fileBackedDevice.ReadAt(toRead, 0)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, WriteData, queuedWrite.actionType)
	assert.Equal(t, expectedRead, queuedWrite.data)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.Equal(t, uint64(4), queuedWrite.endBlock)
	assert.Equal(t, toRead, expectedRead)
	assert.Equal(t, expectedRead, toRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestReadDirtyBlock(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toSourceRead := make([]byte, BlockSize)
	toBackingRead := make([]byte, BlockSize)
	toRead := make([]byte, BlockSize/2)
	expectedRead := fill1s(make([]byte, BlockSize/2))
	fill(expectedRead[BlockSize/4:BlockSize/2], 2)
	mockSourceFile.output = 1
	mockBackingFile.output = 2
	mockSourceFile.On("ReadAt", toSourceRead, int64(0)).Return(len(toSourceRead), nil)
	mockBackingFile.On("ReadAt", toBackingRead, int64(0)).Return(len(toBackingRead), nil)

	fileBackedDevice.dirtyBlockMap.recordWrite(0, BlockSize/4, BlockSize/4)

	err := fileBackedDevice.ReadAt(toRead, 0)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, WriteData, queuedWrite.actionType)
	fill(toSourceRead, 1)
	fill(toSourceRead[BlockSize/4:BlockSize/2], 2)
	assert.Equal(t, toSourceRead, queuedWrite.data)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.Equal(t, uint64(0), queuedWrite.endBlock)
	assert.Equal(t, expectedRead, toRead)
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] READ offset:%d len:%d", 0, len(toRead)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestWriteSynced(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toWrite := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.On("WriteAt", toWrite, int64(0)).Return(len(toWrite), nil)
	fileBackedDevice.SetSynced()

	err := fileBackedDevice.WriteAt(toWrite, 0)

	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", 0, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestWriteSyncedFailure(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toWrite := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.On("WriteAt", toWrite, int64(0)).Return(len(toWrite), fmt.Errorf("Could not Write"))
	fileBackedDevice.SetSynced()

	err := fileBackedDevice.WriteAt(toWrite, 0)

	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", 0, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err.Error(), "could not write to backing file at offset 0: Could not Write")
}

func TestWriteCacheHit(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toWrite := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.On("WriteAt", toWrite, int64(0)).Return(len(toWrite), nil)
	fileBackedDevice.blockMap.setBlock(0, true)

	err := fileBackedDevice.WriteAt(toWrite, 0)

	assert.False(t, fileBackedDevice.dirtyBlockMap.isDirty(0))
	assert.Nil(t, fileBackedDevice.writerQueue.TryDequeue(1, false))
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", 0, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestWriteCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	toBackingWrite := fill1s(make([]byte, BlockSize/2))
	toWrite := fill1s(make([]byte, BlockSize/2))
	mockBackingFile.On("WriteAt", toBackingWrite, int64(0)).Return(len(toBackingWrite), nil)

	err := fileBackedDevice.WriteAt(toWrite, 0)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, FixDirtyBlock, queuedWrite.actionType)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.True(t, fileBackedDevice.dirtyBlockMap.isDirty(0))
	assert.Equal(t, &writeAction{0, BlockSize / 2}, fileBackedDevice.dirtyBlockMap.dirtyMap[0][0])
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", 0, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestWriteLargeCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*5)
	toBackingWrite := fill1s(make([]byte, BlockSize*2))
	toWrite := fill1s(make([]byte, BlockSize*2))
	mockBackingFile.On("WriteAt", toBackingWrite, int64(BlockSize/2)).Return(len(toBackingWrite), nil)

	err := fileBackedDevice.WriteAt(toWrite, BlockSize/2)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, FixDirtyBlock, queuedWrite.actionType)
	assert.Equal(t, uint64(0), queuedWrite.startBlock)
	assert.True(t, fileBackedDevice.dirtyBlockMap.isDirty(0))
	assert.Equal(t, &writeAction{BlockSize / 2, BlockSize / 2}, fileBackedDevice.dirtyBlockMap.dirtyMap[0][0])
	queuedWrite2 := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Equal(t, FixDirtyBlock, queuedWrite2.actionType)
	assert.Equal(t, uint64(2), queuedWrite2.startBlock)
	assert.True(t, fileBackedDevice.dirtyBlockMap.isDirty(2))
	assert.Equal(t, &writeAction{0, BlockSize / 2}, fileBackedDevice.dirtyBlockMap.dirtyMap[2][0])
	assert.True(t, fileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", BlockSize/2, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}

func TestWriteAlignedCacheMiss(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockBackingFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*5)
	toBackingWrite := fill1s(make([]byte, BlockSize*2))
	toWrite := fill1s(make([]byte, BlockSize*2))
	mockBackingFile.On("WriteAt", toBackingWrite, int64(0)).Return(len(toBackingWrite), nil)

	err := fileBackedDevice.WriteAt(toWrite, 0)

	queuedWrite := fileBackedDevice.writerQueue.TryDequeue(10, false)
	assert.Nil(t, queuedWrite)
	assert.False(t, fileBackedDevice.dirtyBlockMap.isDirty(0))
	assert.False(t, fileBackedDevice.dirtyBlockMap.isDirty(1))
	assert.True(t, fileBackedDevice.blockMap.isSynced(0))
	assert.True(t, fileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, fmt.Sprintf("[FileBackedDevice] WRITE offset:%d len:%d", 0, len(toWrite)), hook.LastEntry().Message)
	assert.Equal(t, err, nil)
}
