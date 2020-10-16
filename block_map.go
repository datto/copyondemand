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
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/sirupsen/logrus"
)

// blockMap is mostly a thin wrapper around
// https://github.com/RoaringBitmap/roaring (compressed bitmap library)
// providing thread safety. We also handle block maps that are larger
// than uint32 (largest index supported by roaring) by storing a
// slice of bitmaps, rather than just one.
type blockMap struct {
	bitmaps []*roaring.Bitmap
	lock    *sync.Mutex
	size    uint64
	logger  *logrus.Logger
	synced  uint64
}

// newBlockMap initializes a BlockMap
func newBlockMap(size uint64, logger *logrus.Logger) *blockMap {
	blockCount := (size / math.MaxUint32) + 1
	bitmaps := make([]*roaring.Bitmap, 0)

	for i := uint64(0); i < blockCount; i++ {
		bitmaps = append(bitmaps, roaring.New())
	}

	return &blockMap{
		bitmaps,
		&sync.Mutex{},
		size,
		logger,
		0,
	}
}

// setBlock sets a block to either synced or unsynced
func (bm *blockMap) setBlock(block uint64, synced bool) {
	if block >= bm.size || block < 0 {
		bm.logger.Errorf("block %d out of range", block)
		panic(fmt.Sprintf("block %d out of range", block))
	}

	bitmap := block / math.MaxUint32
	bitmapIx := uint32(block % math.MaxUint32)

	bm.lock.Lock()
	if synced {
		if bm.bitmaps[bitmap].CheckedAdd(bitmapIx) {
			bm.synced++
		}
	} else {
		if bm.bitmaps[bitmap].CheckedRemove(bitmapIx) {
			bm.synced--
		}
	}
	bm.lock.Unlock()
}

// isSynced checks if a block index has been synced
func (bm *blockMap) isSynced(block uint64) bool {
	if block >= bm.size || block < 0 {
		bm.logger.Errorf("block %d out of range", block)
		panic(fmt.Sprintf("block %d out of range", block))
	}

	bitmap := block / math.MaxUint32
	bitmapIx := uint32(block % math.MaxUint32)

	bm.lock.Lock()
	isSynced := bm.bitmaps[bitmap].Contains(bitmapIx)
	bm.lock.Unlock()

	return isSynced
}

// totalSynced returns how many total blocks are synced
func (bm *blockMap) totalSynced() uint64 {
	bm.lock.Lock()
	totalSynced := bm.synced
	bm.lock.Unlock()

	return totalSynced
}

func (bm *blockMap) serialize() ([]byte, error) {
	bm.lock.Lock()
	defer bm.lock.Unlock()
	returnBytes := make([]byte, 0)
	for _, bm := range bm.bitmaps {
		currentBytes, err := bm.ToBytes()
		if err != nil {
			return nil, err
		}
		intLen := len(currentBytes)
		lenHeader := uint64ToBytes(uint64(intLen))
		returnBytes = append(returnBytes, lenHeader...)
		returnBytes = append(returnBytes, currentBytes...)
	}

	// Terminate with a 64 bit int 0 value
	returnBytes = append(returnBytes, uint64ToBytes(0)...)

	return returnBytes, nil
}

func (bm *blockMap) deserialize(content []byte, ix int) error {
	bm.lock.Lock()
	defer bm.lock.Unlock()
	_, err := bm.bitmaps[ix].FromBuffer(content)

	// Recalculate the synced counter, so progress bars aren't wonky
	bm.synced = 0
	for _, currentBitmap := range bm.bitmaps {
		bm.synced += currentBitmap.GetCardinality()
	}

	return err
}
