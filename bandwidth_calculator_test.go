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

func TestUpdateBandwidthNoOverrideConfigBandwidthUnmodified(t *testing.T) {
	logger, _ := test.NewNullLogger()
	device := CreateMockDevice(t, new(MockFile), new(MockFile), logger, 1)
	bandwidthLimit := updateBandwidth(1, device)
	assert.Equal(t, defaultMaxMachineBytesPerSecond, bandwidthLimit)
}

func TestUpdateBandwidthNoOverrideConfigBandwidthIsUpdated(t *testing.T) {
	logger, _ := test.NewNullLogger()
	device := CreateMockDevice(t, new(MockFile), new(MockFile), logger, 1)
	bandwidthLimit := updateBandwidth(2, device)
	assert.Equalf(t, defaultMaxMachineBytesPerSecond/2, bandwidthLimit, "Expect bandwidth limit to be %d, got %d", defaultMaxMachineBytesPerSecond, bandwidthLimit)
}
