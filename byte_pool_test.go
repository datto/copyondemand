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

func TestBufferPoolReturnsMinimumBuffer(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	buffer := bp.get(minChunkSize - 12)

	assert.Equal(t, minChunkSize, cap(buffer))
	assert.Equal(t, minChunkSize-12, len(buffer))
}

func TestBufferPoolReturnsMaximumBuffer(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	buffer := bp.get(maxChunkSize + 100)

	assert.Equal(t, maxChunkSize+100, cap(buffer))
	assert.Equal(t, maxChunkSize+100, len(buffer))
}

func TestBufferPoolReturnsEnclosingBuffer(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	buffer := bp.get(minChunkSize*2 + 1)

	assert.Equal(t, minChunkSize*4, cap(buffer))
	assert.Equal(t, minChunkSize*2+1, len(buffer))
}

func TestBufferPoolReturnsExactBuffer(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	buffer := bp.get(minChunkSize * 2)

	assert.Equal(t, minChunkSize*2, cap(buffer))
	assert.Equal(t, minChunkSize*2, len(buffer))
}

func TestReturningBufferToPoolFailsForIncorrectlySizedBuffers(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	integrated := bp.put(make([]byte, minChunkSize+1))

	assert.Error(t, integrated)
}

func TestReturningBufferToPoolFailsForLargeBuffers(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	integrated := bp.put(make([]byte, maxChunkSize*2))

	assert.Error(t, integrated)
}

func TestReturningBufferToPoolFailsForSmallBuffers(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	integrated := bp.put(make([]byte, 1<<(minChunkPower-1)))

	assert.Error(t, integrated)
}

func TestReturningBufferToPoolSucceedsSmall(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	integrated := bp.put(bp.get(minChunkSize + 1))

	assert.Nil(t, integrated)
}

func TestReturningBufferToPoolSucceedsLarge(t *testing.T) {
	logger, _ := test.NewNullLogger()
	bp := newBytePool(logger)

	integrated := bp.put(bp.get(maxChunkSize - 1))

	assert.Nil(t, integrated)
}
