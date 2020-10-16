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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandleWriteDataActionWrites(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	readData := fill1s(make([]byte, BlockSize*2))
	mockBackingFile.On("WriteAt", readData[0:BlockSize], int64(0)).Return(BlockSize, nil)
	mockBackingFile.On("WriteAt", readData[BlockSize:BlockSize*2], int64(BlockSize)).Return(BlockSize, nil)
	writeAction := &QueuedWriteAction{}
	writeAction.startBlock = 0
	writeAction.endBlock = 1
	writeAction.data = readData

	handleWriteDataAction(fileBackedDevice, writeAction)

	assert.True(t, fileBackedDevice.blockMap.isSynced(0))
	assert.True(t, fileBackedDevice.blockMap.isSynced(1))
}

func TestHandleWriteDataReconcilesDirtyData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	mockBackingFile.output = 2
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	readData := fill1s(make([]byte, BlockSize*2))
	expectedData := fill1s(make([]byte, BlockSize*2))
	fill(expectedData[(BlockSize+(BlockSize/4)):(BlockSize*2-(BlockSize/4))], 2)
	mockBackingFile.On("WriteAt", readData[0:BlockSize], int64(0)).Return(BlockSize, nil)
	mockBackingFile.On("WriteAt", readData[BlockSize:BlockSize*2], int64(BlockSize)).Return(BlockSize, nil)
	mockBackingFile.On("ReadAt", make([]byte, BlockSize), int64(BlockSize)).Return(BlockSize, nil)
	fileBackedDevice.dirtyBlockMap.recordWrite(1, BlockSize/4, BlockSize/2)
	writeAction := &QueuedWriteAction{}
	writeAction.startBlock = 0
	writeAction.endBlock = 1
	writeAction.data = readData

	handleWriteDataAction(fileBackedDevice, writeAction)

	assert.True(t, fileBackedDevice.blockMap.isSynced(0))
	assert.True(t, fileBackedDevice.blockMap.isSynced(1))
	assert.Equal(t, expectedData, readData)
}

func TestFixDirtyBlockWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	mockSourceFile := new(MockFile)
	mockSourceFile.On("Fd").Return(1)
	mockBackingFile := new(MockFile)
	mockSourceFile.output = 1
	mockBackingFile.output = 2
	fileBackedDevice := CreateMockDevice(t, mockSourceFile, mockBackingFile, logger, BlockSize*2)
	expectedData := fill1s(make([]byte, BlockSize))
	fill(expectedData[(BlockSize/4):(BlockSize-(BlockSize/4))], 2)
	mockBackingFile.On("WriteAt", expectedData, int64(BlockSize)).Return(BlockSize, nil)
	mockBackingFile.On("ReadAt", make([]byte, BlockSize), int64(BlockSize)).Return(BlockSize, nil)
	mockSourceFile.On("ReadAt", make([]byte, BlockSize), int64(BlockSize)).Return(BlockSize, nil)
	fileBackedDevice.dirtyBlockMap.recordWrite(1, BlockSize/4, BlockSize/2)
	writeAction := &QueuedWriteAction{}
	writeAction.startBlock = 1

	handleFixDirtyBlockAction(fileBackedDevice, writeAction)

	assert.False(t, fileBackedDevice.dirtyBlockMap.isDirty(1))
	assert.True(t, fileBackedDevice.blockMap.isSynced(1))
}
