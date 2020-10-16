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
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	overrideBandwidthConfig = "/datto/config/codBandwidth"
)

var defaultMaxMachineBytesPerSecond uint64 = 32768000 // = 31.25MB/s = 250mb/s
var currentMaxMachineBytesPerSecond uint64

// TrackDynamicBandwidthLimit periodically updates the bandwidth limit based on how many other cod processes are running
func trackDynamicBandwidthLimit(ctx context.Context, waitGroup *sync.WaitGroup, fileBackedDevice *FileBackedDevice) {
	waitGroup.Add(1)
	defer waitGroup.Done()
	var codCount int
	for !fileBackedDevice.IsFullySynced() && !isCancelSignaled(ctx) {
		codProcesses, err := FindMaxProcessesCounts(LocalFs{}, logrus.StandardLogger(), fileBackedDevice.processFiles)
		if err != nil {
			fileBackedDevice.log.Warnf("Could not read progress file(s): %v", fileBackedDevice.processFiles)
		} else {
			codCount = codProcesses
		}
		updateBandwidth(codCount, fileBackedDevice)
		time.Sleep(5 * time.Second)
	}
	return
}

func updateBandwidth(progressCount int, fileBackedDevice *FileBackedDevice) uint64 {
	currentMaxMachineBytesPerSecond = defaultMaxMachineBytesPerSecond
	// Need to account for max bandwidth at the box level first
	contents, err := ioutil.ReadFile(overrideBandwidthConfig)
	if err == nil {
		bandwidthInt, err2 := strconv.Atoi(strings.Trim(string(contents), "\n"))
		if err2 == nil {
			currentMaxMachineBytesPerSecond = uint64(bandwidthInt)
		} else {
			fileBackedDevice.log.Errorf("Failed to read bandwidth override config: %s", err2)
		}
	}

	var newCopyRate = currentMaxMachineBytesPerSecond
	if progressCount > 1 {
		newCopyRate = currentMaxMachineBytesPerSecond / uint64(progressCount)
	}

	isUpdated := fileBackedDevice.SetBackgroundCopyRate(newCopyRate)
	if isUpdated {
		fileBackedDevice.log.Info(fmt.Sprintf("Updating copy rate: %db/s", newCopyRate))
	}
	return newCopyRate
}
