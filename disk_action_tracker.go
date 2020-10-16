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
	"sync"
	"time"
)

// DiskActionType is an enum for read/write actions happening on a disk
type DiskActionType int

// The maximum amount of seconds of history we store in action trackers
const maximumRecordSeconds = 30

const millisecondWindow = maximumRecordSeconds * 1000

// How much time each actionBucket stores
// WARNING: Must be < 1000 and must evenly divide millisecondWindow (millisecondWindow % intervalMilliseconds == 0)!
const intervalMilliseconds = 100

const (
	// BackingWrite is a write to the backing disk
	BackingWrite DiskActionType = 0
	// BackingRead is a read to the backing disk
	BackingRead DiskActionType = 1
	// SourceRead is a read to the source disk
	SourceRead DiskActionType = 2
	// SourceWrite is a write to the source disk
	SourceWrite DiskActionType = 3
)

type diskAction struct {
	actionType DiskActionType
	count      uint64
}

type actionBucket struct {
	updateTime   time.Time
	actionCounts []uint64
}

// diskActionTracker efficiently stores read and write byte counts over time
type diskActionTracker struct {
	queue         chan *diskAction
	actionBuckets []*actionBucket
	actionPool    *sync.Pool
	actionLock    *sync.Mutex
}

// Define the now function so that we can overwrite the definition in tests
var timeNow = time.Now

// newDiskActionTracker creates a new disk action tracker
func newDiskActionTracker() *diskActionTracker {
	bucketCount := maximumRecordSeconds * (1000 / intervalMilliseconds)
	actionBuckets := make([]*actionBucket, bucketCount)
	for i := 0; i < bucketCount; i++ {
		currentActionBucket := &actionBucket{
			time.Time{},
			make([]uint64, 4),
		}

		actionBuckets[i] = currentActionBucket
	}

	diskActionPool := &sync.Pool{
		New: func() interface{} {
			return &diskAction{}
		},
	}

	return &diskActionTracker{
		make(chan *diskAction, 100),
		actionBuckets,
		diskActionPool,
		&sync.Mutex{},
	}
}

// recordAction records a disk action with a given type
func (d *diskActionTracker) recordAction(actionType DiskActionType, byteCount uint64) {
	action := d.actionPool.Get().(*diskAction)
	action.actionType = actionType
	action.count = byteCount
	d.queue <- action
}

// Sample the number of bytes matching actionType that have happened in the last timeMilliseconds milliseconds
func (d *diskActionTracker) Sample(actionType DiskActionType, timeMilliseconds uint64) uint64 {
	now := timeNow()
	startBucket := getBucketIndex(now)

	d.actionLock.Lock()

	currentIx := startBucket
	currentTime := now
	returnCount := uint64(0)
	for i := uint64(0); i < timeMilliseconds/intervalMilliseconds; i++ {
		if isFresh(d.actionBuckets[currentIx], currentTime) {
			returnCount += d.actionBuckets[currentIx].actionCounts[actionType]
		}
		currentIx--
		if currentIx < 0 {
			currentIx = maximumRecordSeconds*(1000/intervalMilliseconds) - 1
		}
		currentTime = currentTime.Add(-intervalMilliseconds * time.Millisecond)
	}

	d.actionLock.Unlock()

	return returnCount
}

// processQueue blocks, processing the action queue, until cancellation
func (d *diskActionTracker) processQueue(ctx context.Context, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	defer waitGroup.Done()

	for !isCancelSignaled(ctx) {
		diskAction := d.tryDequeue(500)

		if diskAction == nil {
			continue
		}

		d.processAction(diskAction)
	}
}

func (d *diskActionTracker) processAction(diskAction *diskAction) {
	now := timeNow()
	actionBucketIx := getBucketIndex(now)

	actionBucket := d.actionBuckets[actionBucketIx]

	d.actionLock.Lock()
	if !isFresh(actionBucket, now) {
		reset(actionBucket)
	}

	actionBucket.updateTime = now
	actionBucket.actionCounts[diskAction.actionType] += diskAction.count
	d.actionLock.Unlock()
}

// BytesToHumanReadable converts a raw byte count to a human readable value (e.g. 1024 becomes '1KB')
func BytesToHumanReadable(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func (d *diskActionTracker) tryDequeue(waitMilliseconds int) *diskAction {
	select {
	case da := <-d.queue:
		return da
	case <-time.After(time.Duration(waitMilliseconds) * time.Millisecond):
		return nil
	}
}

func isFresh(bucket *actionBucket, now time.Time) bool {
	nonStaleTime := now.Add(time.Duration(-intervalMilliseconds-1) * time.Millisecond)

	return bucket.updateTime.After(nonStaleTime)
}

func reset(bucket *actionBucket) {
	bucket.updateTime = time.Time{}
	for i := 0; i < 4; i++ {
		bucket.actionCounts[i] = 0
	}
}

func getBucketIndex(now time.Time) int64 {
	millisecondNow := now.UnixNano() / int64(time.Millisecond)
	millisecondMod := millisecondNow % millisecondWindow

	return millisecondMod / intervalMilliseconds
}
