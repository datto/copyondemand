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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestRecordWriteStoresWrite(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 0, 1)
	writeList := dm.dirtyMap[0]

	assert.Equal(t, len(writeList), 1)
	assert.Equal(t, writeList[0].off, uint64(0))
	assert.Equal(t, writeList[0].len, 1)
}

func TestRecordMultipleWritesStoresWrites(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 0, 1)
	dm.recordWrite(0, 3, 5)
	writeList := dm.dirtyMap[0]

	assert.Equal(t, len(writeList), 2)
	assert.Equal(t, writeList[0].off, uint64(0))
	assert.Equal(t, writeList[0].len, 1)
	assert.Equal(t, writeList[1].off, uint64(3))
	assert.Equal(t, writeList[1].len, 5)
}

func TestRecordMultipleWritesMergesMergeableWrites(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 1, 2)
	dm.recordWrite(0, 3, 1)
	writeList := dm.dirtyMap[0]

	assert.Equal(t, len(writeList), 1)
	assert.Equal(t, writeList[0].off, uint64(1))
	assert.Equal(t, writeList[0].len, 3)
}

func TestRecordMultipleWritesMergesMergeableWrites2(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 3, 1)
	dm.recordWrite(0, 1, 2)
	writeList := dm.dirtyMap[0]

	assert.Equal(t, len(writeList), 1)
	assert.Equal(t, writeList[0].off, uint64(1))
	assert.Equal(t, writeList[0].len, 3)
}

func TestRecordMultipleWritesMergesMergeableWrites3(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(1, 5, 10)
	dm.recordWrite(1, 9, 15)
	writeList := dm.dirtyMap[1]

	assert.Equal(t, len(writeList), 1)
	assert.Equal(t, writeList[0].off, uint64(5))
	assert.Equal(t, writeList[0].len, 19)
}

func TestRecordMultipleWritesMergesMergeableWrites4(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(1, 9, 15)
	dm.recordWrite(1, 5, 10)
	writeList := dm.dirtyMap[1]

	assert.Equal(t, len(writeList), 1)
	assert.Equal(t, writeList[0].off, uint64(5))
	assert.Equal(t, writeList[0].len, 19)
}

func TestDeleteClearsAllWrites(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 0, 1)
	dm.recordWrite(0, 3, 5)
	dm.remove(0)
	writeList := dm.dirtyMap[0]

	assert.Nil(t, writeList)
}

func TestReconcileFixesBlock(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 0, 100)
	dm.recordWrite(0, 123, 100)
	dm.recordWrite(0, 220, 100)

	source := fill(make([]byte, BlockSize), 1)
	backing := fill(make([]byte, BlockSize), 2)
	expected := fill(make([]byte, BlockSize), 1)
	// Manually apply the slice writes from above
	fill(sliceSlice(expected, 0, 100), 2)
	fill(sliceSlice(expected, 123, 100), 2)
	fill(sliceSlice(expected, 220, 100), 2)

	dm.reconcileBlock(0, source, backing)

	assert.Equal(t, source, expected)
}

func TestReconcileOverlappingWrites(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 0, 100)
	dm.recordWrite(0, 50, 382)

	source := fill(make([]byte, BlockSize), 1)
	backing := fill(make([]byte, BlockSize), 2)
	expected := fill(make([]byte, BlockSize), 1)
	// Manually apply the slice writes from above
	fill(sliceSlice(expected, 0, 100), 2)
	fill(sliceSlice(expected, 50, 382), 2)

	dm.reconcileBlock(0, source, backing)

	assert.Equal(t, source, expected)
}

func TestReconcileOverlappingWritesInverse(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 50, 382)
	dm.recordWrite(0, 0, 100)

	source := fill(make([]byte, BlockSize), 1)
	backing := fill(make([]byte, BlockSize), 2)
	expected := fill(make([]byte, BlockSize), 1)
	// Manually apply the slice writes from above
	fill(sliceSlice(expected, 0, 100), 2)
	fill(sliceSlice(expected, 50, 382), 2)

	dm.reconcileBlock(0, source, backing)

	assert.Equal(t, source, expected)
}

func TestSerializationWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 50, 382)
	dm.recordWrite(1, 0, 100)

	serializedBytes := dm.serialize()
	expectedBytesPermutation0 := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		50, 0, 0, 0, 0, 0, 0, 0,
		126, 1, 0, 0, 0, 0, 0, 0, // 382 length
		1, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		100, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
	}
	expectedBytesPermutation1 := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		100, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		50, 0, 0, 0, 0, 0, 0, 0,
		126, 1, 0, 0, 0, 0, 0, 0, // 382 length
		0, 0, 0, 0, 0, 0, 0, 0,
	}

	// Ugh, golang doesn't want you to depend on map ordering so bad, that they intentionally make the
	// order non-deterministic. So we check that our serialized data is one of the possible permutations.
	// Order doesn't matter to the dirty block map either, so this is safe.
	assert.Subset(t, [][]byte{expectedBytesPermutation0, expectedBytesPermutation1}, [][]byte{serializedBytes})
}

func TestGetAllDirtyBlocks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dm := newDirtyBlockMap(logger)

	dm.recordWrite(0, 50, 382)
	dm.recordWrite(1, 0, 100)

	dirtyBlocks := dm.allDirtyBlocks()
	expectedBlocksPermutation0 := []uint64{1, 0}
	expectedBlocksPermutation1 := []uint64{0, 1}

	assert.Subset(t, [][]uint64{expectedBlocksPermutation0, expectedBlocksPermutation1}, [][]uint64{dirtyBlocks})
}

func fill(slice []byte, val byte) []byte {
	for i := range slice {
		slice[i] = val
	}

	return slice
}

func sliceSlice(slice []byte, off, len int) []byte {
	return slice[off:(off + len)]
}
