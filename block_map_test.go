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
	"math"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestAccessingOverflowIndexFails(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("This test should panic")
		} else {
			assert.Equal(t, "block 1 out of range", r)
		}
	}()

	logger, _ := test.NewNullLogger()
	b := newBlockMap(1, logger)

	b.setBlock(1, true)
}

func TestSettingMaxIndexSucceeds(t *testing.T) {
	logger, hook := test.NewNullLogger()
	b := newBlockMap(1, logger)

	b.setBlock(0, true)

	assert.Nil(t, hook.LastEntry())
}

func TestSettingIndexWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	b := newBlockMap(1, logger)

	val := b.isSynced(0)
	assert.False(t, val)

	b.setBlock(0, true)

	val = b.isSynced(0)
	assert.True(t, val)
}

func TestUnSettingIndexWorks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	b := newBlockMap(1, logger)

	val := b.isSynced(0)
	assert.False(t, val)

	b.setBlock(0, true)

	val = b.isSynced(0)
	assert.True(t, val)

	b.setBlock(0, false)

	val = b.isSynced(0)
	assert.False(t, val)
}

func TestSettingMaxInt(t *testing.T) {
	logger, _ := test.NewNullLogger()
	b := newBlockMap(2*math.MaxUint32, logger)

	val := b.isSynced(math.MaxUint32)
	assert.False(t, val)

	b.setBlock(math.MaxUint32, true)

	val = b.isSynced(math.MaxUint32)
	assert.True(t, val)
}

func TestSettingGreaterThanMaxInt(t *testing.T) {
	logger, _ := test.NewNullLogger()
	b := newBlockMap(2*math.MaxUint32, logger)

	val := b.isSynced(math.MaxUint32 + 1)
	assert.False(t, val)

	b.setBlock(math.MaxUint32+1, true)

	val = b.isSynced(math.MaxUint32 + 1)
	assert.True(t, val)
}

// Note: There isn't much to test here, we assume that roaring serialize
// is a working black box. So these serialization tests just test nothing
// went wrong with how we wrap that serialization.
func TestBasicSerializationAndDeserialization(t *testing.T) {

	logger, _ := test.NewNullLogger()
	b := newBlockMap(1024, logger)

	b.setBlock(3, true)
	b.setBlock(10, true)
	b.setBlock(35, true)

	roaringSerializedBytes, _ := b.bitmaps[0].ToBytes()

	expectedBytes := make([]byte, 0)
	expectedBytes = append(expectedBytes, uint64ToBytes(uint64(len(roaringSerializedBytes)))...)
	expectedBytes = append(expectedBytes, roaringSerializedBytes...)
	expectedBytes = append(expectedBytes, uint64ToBytes(0)...)

	serializedBytes, _ := b.serialize()

	assert.Equal(t, expectedBytes, serializedBytes)

	d := newBlockMap(1024, logger)
	d.deserialize(roaringSerializedBytes, 0)

	assert.True(t, d.isSynced(3))
	assert.True(t, d.isSynced(10))
	assert.True(t, d.isSynced(35))
	assert.Equal(t, uint64(3), d.bitmaps[0].GetCardinality())
	assert.Equal(t, uint64(3), d.synced)
}

func TestLargeSerializationAndDeserialization(t *testing.T) {

	logger, _ := test.NewNullLogger()
	b := newBlockMap(2*math.MaxUint32-1, logger)

	b.setBlock(2, true)
	b.setBlock(9, true)
	b.setBlock(34, true)

	b.setBlock(3+math.MaxUint32, true)
	b.setBlock(10+math.MaxUint32, true)
	b.setBlock(35+math.MaxUint32, true)

	roaringSerializedBytes0, _ := b.bitmaps[0].ToBytes()
	roaringSerializedBytes1, _ := b.bitmaps[1].ToBytes()

	expectedBytes := make([]byte, 0)
	expectedBytes = append(expectedBytes, uint64ToBytes(uint64(len(roaringSerializedBytes0)))...)
	expectedBytes = append(expectedBytes, roaringSerializedBytes0...)
	expectedBytes = append(expectedBytes, uint64ToBytes(uint64(len(roaringSerializedBytes1)))...)
	expectedBytes = append(expectedBytes, roaringSerializedBytes1...)
	expectedBytes = append(expectedBytes, uint64ToBytes(0)...)

	serializedBytes, _ := b.serialize()

	assert.Equal(t, expectedBytes, serializedBytes)

	d := newBlockMap(2*math.MaxUint32-1, logger)
	d.deserialize(roaringSerializedBytes0, 0)
	d.deserialize(roaringSerializedBytes1, 1)

	assert.True(t, d.isSynced(2))
	assert.True(t, d.isSynced(9))
	assert.True(t, d.isSynced(34))
	assert.True(t, d.isSynced(3+math.MaxUint32))
	assert.True(t, d.isSynced(10+math.MaxUint32))
	assert.True(t, d.isSynced(35+math.MaxUint32))
	assert.Equal(t, uint64(3), d.bitmaps[0].GetCardinality())
	assert.Equal(t, uint64(3), d.bitmaps[1].GetCardinality())
	assert.Equal(t, uint64(6), d.synced)
}
