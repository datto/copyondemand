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
	"sync"
	"testing"
	"time"
)

var sleepTime = 10 * time.Millisecond

func makeRangeLocker() *RangeLocker {
	blockRangePool := &sync.Pool{
		New: func() interface{} {
			return &BlockRange{}
		},
	}

	return NewRangeLocker(blockRangePool)
}

func TestConflictingLocksBlock(t *testing.T) {
	rl := makeRangeLocker()
	timeChan := make(chan time.Duration)
	start := time.Now()
	rl.LockRange(0, 10)
	go func() {
		rl.LockRange(3, 7)
		timeChan <- time.Since(start)
	}()
	time.Sleep(sleepTime)
	rl.UnlockRange(0, 10)
	select {
	case threadTook := <-timeChan:
		assert.True(t, threadTook >= sleepTime)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "Test timed out (probably blocked forever)")
	}
}

func TestManyConflictingLocksBlock(t *testing.T) {
	rl := makeRangeLocker()
	timeChan := make(chan time.Duration)
	start := time.Now()
	rl.LockRange(0, 10)
	go func() {
		rl.LockRange(3, 7)
		timeChan <- time.Since(start)
		time.Sleep(sleepTime)
		rl.UnlockRange(3, 7)
	}()
	go func() {
		rl.LockRange(3, 7)
		timeChan <- time.Since(start)
		time.Sleep(sleepTime)
		rl.UnlockRange(3, 7)
	}()

	time.Sleep(sleepTime)
	rl.UnlockRange(0, 10)
	select {
	case threadTook := <-timeChan:
		assert.True(t, threadTook >= sleepTime)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "Test timed out (probably blocked forever)")
	}
	select {
	case threadTook := <-timeChan:
		assert.True(t, threadTook >= sleepTime*2)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "Test timed out (probably blocked forever)")
	}
}

func TestManyNonOverlappingLocksBlock(t *testing.T) {
	rl := makeRangeLocker()
	timeChan := make(chan time.Duration)
	start := time.Now()
	// These do not overlap so don't block eachother
	rl.LockRange(0, 10)
	rl.LockRange(20, 30)
	go func() {
		rl.LockRange(5, 25)
		timeChan <- time.Since(start)
		rl.UnlockRange(5, 25)
	}()

	// Unlock one, we should still be blocking
	time.Sleep(sleepTime)
	rl.UnlockRange(0, 10)

	// Unlock second, this unblocks
	time.Sleep(sleepTime)
	rl.UnlockRange(20, 30)

	select {
	case threadTook := <-timeChan:
		assert.True(t, threadTook >= sleepTime*2)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "Test timed out (probably blocked forever)")
	}
}

func TestNonConflictingLocksDontBlock(t *testing.T) {
	rl := makeRangeLocker()
	timeChan := make(chan time.Duration)
	start := time.Now()
	rl.LockRange(0, 10)
	go func() {
		rl.LockRange(11, 20)
		timeChan <- time.Since(start)
	}()

	time.Sleep(sleepTime)
	rl.UnlockRange(0, 10)
	select {
	case threadTook := <-timeChan:
		assert.True(t, threadTook < sleepTime)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "Test timed out (probably blocked forever)")
	}
}
