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
	"sync"
	"time"
)

const (
	dynamicQueueCapacity    = 500 // Hard cap of # of actions that can be in the queue
	backgroundQueueCapacity = 1
)

// WriteActionType is an enum for the type of action that can be in the writer queue
type WriteActionType int

// QueueType is an enum for the type of queue you want an action from
type QueueType int

const (
	// WriteData actions flush data to disk that was already read from source by a dynamic read action
	WriteData WriteActionType = 0
	// FixDirtyBlock actions sync data from source, to fully sync blocks that were partially written on the target
	FixDirtyBlock WriteActionType = 1
	// SyncBlock actions sync data from source and write them to the backing file
	SyncBlock WriteActionType = 2
)

const (
	// DynamicQueue serves actions from the user
	DynamicQueue QueueType = 0
	// BackgroundQueue copies blocks in the background
	BackgroundQueue QueueType = 1
)

// QueuedWriteAction contains the type, affected block range, and optional data to be written to disk
type QueuedWriteAction struct {
	startBlock uint64
	endBlock   uint64
	actionType WriteActionType
	data       []byte
}

// WriterQueue is a thin wrapper to a channel, which allows for limiting the amount of data in the channel at once
type WriterQueue struct {
	dynamicActionQueue    chan *QueuedWriteAction
	backgroundActionQueue chan *QueuedWriteAction
	writeActionPool       *sync.Pool
}

// NewWriterQueue initializes a writer queue
func NewWriterQueue() *WriterQueue {
	dynamicActionQueue := make(chan *QueuedWriteAction, dynamicQueueCapacity)
	backgroundActionQueue := make(chan *QueuedWriteAction, backgroundQueueCapacity)
	writeActionPool := &sync.Pool{
		New: func() interface{} {
			return &QueuedWriteAction{}
		},
	}

	return &WriterQueue{
		dynamicActionQueue,
		backgroundActionQueue,
		writeActionPool,
	}
}

// TryEnqueue attempts to add a write action to the write queue. timeoutMilliseconds = 0
// indicates that this function should block
func (wq *WriterQueue) TryEnqueue(wa *QueuedWriteAction, timeoutMilliseconds int) bool {
	var selectedQueue chan *QueuedWriteAction
	if wa.actionType == SyncBlock {
		selectedQueue = wq.backgroundActionQueue
	} else {
		selectedQueue = wq.dynamicActionQueue
	}

	if timeoutMilliseconds == 0 {
		selectedQueue <- wa
		return true
	}

	select {
	case selectedQueue <- wa:
		return true
	case <-time.After(time.Duration(timeoutMilliseconds) * time.Millisecond):
		return false
	}
}

// TryDequeue attempts to pull an item off the queue, if no message has
// arrived in waitMilliseconds this method returns nil. If includeBackgroundItems
// is true you _may_ also get background block sync actions
func (wq *WriterQueue) TryDequeue(waitMilliseconds int, includeBackgroundItems bool) *QueuedWriteAction {
	if includeBackgroundItems {
		select {
		case wa := <-wq.dynamicActionQueue:
			return wa
		case wa := <-wq.backgroundActionQueue:
			return wa
		case <-time.After(time.Duration(waitMilliseconds) * time.Millisecond):
			return nil
		}
	} else {
		select {
		case wa := <-wq.dynamicActionQueue:
			return wa
		case <-time.After(time.Duration(waitMilliseconds) * time.Millisecond):
			return nil
		}
	}
}

// Dequeue attempts to pull an item off the queue. This method is non-blocking
// if the queue contains no items, this function returns nil. If includeBackgroundItems
// is true you _may_ also get background block sync actions.
func (wq *WriterQueue) Dequeue(includeBackgroundItems bool) *QueuedWriteAction {
	if includeBackgroundItems {
		select {
		case wa := <-wq.dynamicActionQueue:
			return wa
		case wa := <-wq.backgroundActionQueue:
			return wa
		default:
			return nil
		}
	} else {
		select {
		case wa := <-wq.dynamicActionQueue:
			return wa
		default:
			return nil
		}
	}
}

// MakeWriteAction constructs a QueuedWriteAction struct
func (wq *WriterQueue) MakeWriteAction() *QueuedWriteAction {
	return wq.writeActionPool.Get().(*QueuedWriteAction)
}

// PutWriteAction adds a write action back to the pool
func (wq *WriterQueue) PutWriteAction(wa *QueuedWriteAction) {
	// Remove references to heap data
	wa.data = nil
	wq.writeActionPool.Put(wa)
}
