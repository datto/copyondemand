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

func TestNoOpSerialization(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pool := newBytePool(logger)
	transaction := &blockMapIntentTransaction{}
	transaction.setStartBlock(1)
	transaction.setEndBlock(0)

	serializedTransaction := transaction.serialize(pool)

	assert.Nil(t, serializedTransaction)
}

func TestDirtyOverflowPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("This test should panic")
		} else {
			assert.Equal(t, "Attempted to record more than 2 dirty write actions from a single write", r)
		}
	}()

	logger, _ := test.NewNullLogger()
	transaction := &blockMapIntentTransaction{}
	intentLogger := createIntentLogger(t, new(MockFs), logger)
	transaction.recordDirtyWrite(intentLogger, 0, 0, 1)
	transaction.recordDirtyWrite(intentLogger, 1, 0, 1)
	transaction.recordDirtyWrite(intentLogger, 2, 0, 1)
}

func TestSingleBlockSerialization(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pool := newBytePool(logger)
	transaction := &blockMapIntentTransaction{}
	transaction.setStartBlock(3)
	transaction.setEndBlock(3)
	expectedSerialization := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
	}

	serializedTransaction := transaction.serialize(pool)

	assert.Equal(t, expectedSerialization, serializedTransaction)
}

func TestDirtyWriteSerialization(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pool := newBytePool(logger)
	transaction := &blockMapIntentTransaction{}
	intentLogger := createIntentLogger(t, new(MockFs), logger)
	transaction.setStartBlock(3)
	transaction.setEndBlock(2)
	transaction.recordDirtyWrite(intentLogger, 3, 100, 200)
	transaction.recordDirtyWrite(intentLogger, 4, 99, 199)
	expectedSerialization := []byte{
		2, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		100, 0, 0, 0, 0, 0, 0, 0,
		200, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
		99, 0, 0, 0, 0, 0, 0, 0,
		199, 0, 0, 0, 0, 0, 0, 0,
	}

	serializedTransaction := transaction.serialize(pool)

	assert.Equal(t, expectedSerialization, serializedTransaction)
}

func TestMultipleBlocksAndDirtyWriteSerialization(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pool := newBytePool(logger)
	transaction := &blockMapIntentTransaction{}
	intentLogger := createIntentLogger(t, new(MockFs), logger)
	transaction.setStartBlock(3)
	transaction.setEndBlock(5)
	transaction.recordDirtyWrite(intentLogger, 4, 99, 199)
	expectedSerialization := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		3, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 0, 0, 0, 0, 0, 0,
		99, 0, 0, 0, 0, 0, 0, 0,
		199, 0, 0, 0, 0, 0, 0, 0,
	}

	serializedTransaction := transaction.serialize(pool)

	assert.Equal(t, expectedSerialization, serializedTransaction)
}
