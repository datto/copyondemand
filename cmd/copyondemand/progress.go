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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datto/copyondemand"
)

const (
	progressEnds                = "|"
	progressPending             = "-"
	progressComplete            = "="
	progressBarWidth            = 30
	byteSpeedWindowMilliseconds = 5000
)

var previousOutputWidth int

// OutputStatus is a blocking function that outputs a progress bar for background syncing +
// speed metrics for backing devices at an interval
func outputStatus(ctx context.Context, fileBackedDevice *copyondemand.FileBackedDevice) {
	previousOutputWidth = -1
	for true {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			outputProgressBar(fileBackedDevice)
		}
	}
}

func outputProgressBar(fileBackedDevice *copyondemand.FileBackedDevice) {
	totalBlocks := (fileBackedDevice.Source.Size / copyondemand.BlockSize) + 1
	blockProgress := fileBackedDevice.TotalSyncedBlocks()
	percentComplete := (blockProgress * 100) / totalBlocks
	backingReadRate := fileBackedDevice.SampleRate(copyondemand.BackingRead, byteSpeedWindowMilliseconds)
	backingWriteRate := fileBackedDevice.SampleRate(copyondemand.BackingWrite, byteSpeedWindowMilliseconds)
	sourceReadRate := fileBackedDevice.SampleRate(copyondemand.SourceRead, byteSpeedWindowMilliseconds)
	maxBandwidth := fileBackedDevice.GetBackgroundCopyRate()
	barsComplete := (percentComplete * progressBarWidth) / 100
	barsNotComplete := progressBarWidth - barsComplete

	fmt.Print("\r")
	if previousOutputWidth != -1 {
		fmt.Printf("%s\r", strings.Repeat(" ", previousOutputWidth))
	}

	progressBar := fmt.Sprintf(
		"%s%s%s%s %d%% br %s/s bw %s/s sr %s mb %s/s",
		progressEnds,
		strings.Repeat(progressComplete, int(barsComplete)),
		strings.Repeat(progressPending, int(barsNotComplete)),
		progressEnds,
		percentComplete,
		copyondemand.BytesToHumanReadable(backingReadRate),
		copyondemand.BytesToHumanReadable(backingWriteRate),
		copyondemand.BytesToHumanReadable(sourceReadRate),
		copyondemand.BytesToHumanReadable(maxBandwidth),
	)
	previousOutputWidth = len(progressBar)
	fmt.Print(progressBar)
}
