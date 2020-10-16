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
	"time"
)

func mockNow(offsetMilliseconds int64) {
	// Arbitrary start time, 01/01/2019 00:00:00
	start := int64(1546300800)

	mockTime := time.Unix(start, 0)

	mockTime = mockTime.Add(time.Duration(offsetMilliseconds) * time.Millisecond)

	timeNow = func() time.Time {
		return mockTime
	}
}

func flushQueue(dat *diskActionTracker) {
	for true {
		select {
		case da := <-dat.queue:
			dat.processAction(da)
		default:
			return
		}
	}
}

func TestActionIsQueued(t *testing.T) {
	dat := newDiskActionTracker()

	dat.recordAction(100, 1024)

	enqueuedAction := dat.tryDequeue(500)

	assert.Equal(t, DiskActionType(100), enqueuedAction.actionType)
	assert.Equal(t, uint64(1024), enqueuedAction.count)
}

func TestActionIsRecorded(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(0)

	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds)

	assert.Equal(t, uint64(1024), count)
}

func TestSampleWindowFallsOff(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(0)

	dat.recordAction(BackingWrite, 1111)
	flushQueue(dat)

	mockNow(100)
	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds)

	assert.Equal(t, uint64(1024), count)
}

func TestSampleWindowMultipleBuckets(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(0)

	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	mockNow(100)
	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds*2)

	assert.Equal(t, uint64(1024*2), count)
}

func TestSampleWindowExpireBucket(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(0)

	dat.recordAction(BackingWrite, 1111)
	flushQueue(dat)

	mockNow(maximumRecordSeconds * (1000 / intervalMilliseconds))
	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds)

	assert.Equal(t, uint64(1024), count)
}

func TestSampleWindowCrossBucketLengthBoundary(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(-100)

	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	mockNow(0)
	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds*2)

	assert.Equal(t, uint64(1024*2), count)
}

func TestSampleWindowSampleStale(t *testing.T) {
	dat := newDiskActionTracker()
	mockNow(0)

	dat.recordAction(BackingWrite, 1024)
	flushQueue(dat)

	count := dat.Sample(BackingWrite, intervalMilliseconds*10)

	assert.Equal(t, uint64(1024), count)
}

func TestBytesToHumanReadable(t *testing.T) {
	b := BytesToHumanReadable(1000)
	kb := BytesToHumanReadable(1024)
	mb := BytesToHumanReadable(1024 * 1024)
	gb := BytesToHumanReadable(1024 * 1024 * 1024)
	tb := BytesToHumanReadable(1024 * 1024 * 1024 * 1024)

	assert.Equal(t, "1000B", b)
	assert.Equal(t, "1.0KB", kb)
	assert.Equal(t, "1.0MB", mb)
	assert.Equal(t, "1.0GB", gb)
	assert.Equal(t, "1.0TB", tb)
}
