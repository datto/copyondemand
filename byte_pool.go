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
	"errors"
	"math/bits"
	"sync"

	"github.com/sirupsen/logrus"
)

const minChunkPower = 9  // 2 ^ 9 bytes, 512 bytes
const maxChunkPower = 20 // 2 ^ 20 bytes, 1 MiB
const minChunkSize = 1 << minChunkPower
const maxChunkSize = 1 << maxChunkPower

// bytePool allows caching of variable length byte
// slices to relieve pressure on the garbage collector
type bytePool struct {
	pools  []*sync.Pool
	logger *logrus.Logger
}

// newBytePool initializes a memory pool
func newBytePool(logger *logrus.Logger) *bytePool {
	pools := make([]*sync.Pool, 0)

	for i := minChunkPower; i <= maxChunkPower; i++ {
		currentPool := &sync.Pool{
			New: func(pow int) func() interface{} {
				return func() interface{} {
					return make([]byte, 1<<uint(pow))
				}
			}(i),
		}

		pools = append(pools, currentPool)
	}

	return &bytePool{
		pools,
		logger,
	}
}

// get fetches a slice of given length, from the pool of available slices
// NOTE: This slice will contain junk, it will not be 0'd!
func (mp *bytePool) get(bufferLength uint64) []byte {
	if bufferLength == 0 {
		return nil
	}

	poolID, err := getPoolID(bufferLength)

	if err != nil {
		mp.logger.Warnf("Large allocation %d", bufferLength)

		// Requested blocks that are larger than
		// the max block size are just directly allocated
		return make([]byte, bufferLength)
	}

	// Get a memory chunk big enough to contain the requested
	// length, and slice it to exactly the provided length
	return mp.pools[poolID].Get().([]byte)[0:bufferLength]
}

// put adds a slice of memory back to the available pool
// Returns whether or not the slice was able to be added
// back to a pool.
func (mp *bytePool) put(buffer []byte) error {
	sliceCapacity := uint64(cap(buffer))
	ones := bits.OnesCount64(sliceCapacity)

	// If this isn't an exact power of 2, we definitely
	// didn't make it. Release to GC
	if ones != 1 {
		return errors.New("Attempted to re-insert an incorrectly sized buffer")
	}

	// If this is out of range of our capacity, we could have made it,
	// but we don't have any place to Put it (this is expected for
	// chunks larger than maxChunkSize). Release to GC
	if sliceCapacity < minChunkSize || sliceCapacity > maxChunkSize {
		return errors.New("Attempted to re-insert an over-sized buffer")
	}

	poolID, err := getPoolID(sliceCapacity)

	if err != nil {
		return err
	}

	// Reslice to capacity before inserting back into pool
	mp.pools[poolID].Put(buffer[0:sliceCapacity])

	return nil
}

// getPoolID returns the index of the pool (in BufferPool.pools) which
// can contain the provided length. For example if bufferLength = 1025, the smallest
// power of 2 length buffer is 2048, which is index = 2 in our pool array (assuming
// min pool size = 512).
// Returns an error if the length is larger than the largest buffer.
func getPoolID(bufferLength uint64) (int, error) {
	powerOfTwo := 64 - bits.LeadingZeros64(bufferLength) // Lowest power of 2 that can contain our len
	ones := bits.OnesCount64(bufferLength)

	// If we're an exact power of 2 then we don't need to round up
	if ones == 1 {
		powerOfTwo--
	}

	if powerOfTwo <= minChunkPower {
		return 0, nil
	} else if powerOfTwo <= maxChunkPower {
		return powerOfTwo - minChunkPower, nil
	} else {
		return 0, errors.New("Buffer too large")
	}
}
