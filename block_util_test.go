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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSingleBlock(t *testing.T) {
	expectedRange := BlockRange{0, 0}
	actualRange := BlockRange{}

	getAffectedBlockRange(1, 0, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetTinyOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 1}
	actualRange := BlockRange{}

	getAffectedBlockRange(2, BlockSize-1, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetTinyNoOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 0}
	actualRange := BlockRange{}

	getAffectedBlockRange(1, BlockSize-1, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetMultiOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 2}
	actualRange := BlockRange{}

	getAffectedBlockRange(BlockSize+2, BlockSize-1, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetLargeNoOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 0}
	actualRange := BlockRange{}

	getAffectedBlockRange(BlockSize, 0, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetExtraLargeNoOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 1}
	actualRange := BlockRange{}

	getAffectedBlockRange(BlockSize*2, 0, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetMiddleNoOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 0}
	actualRange := BlockRange{}

	getAffectedBlockRange(BlockSize/2, BlockSize/2, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}

func TestGetMiddleOverlap(t *testing.T) {
	expectedRange := BlockRange{0, 1}
	actualRange := BlockRange{}

	getAffectedBlockRange((BlockSize/2)+1, BlockSize/2, &actualRange)

	assert.Equal(t, expectedRange, actualRange)
}
