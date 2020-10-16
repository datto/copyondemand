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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindMaxProcessesSuccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	payload := []byte("10")
	mockFs := new(MockFs)
	mockFs.On("ReadFile", "source").Return(payload, nil)
	maxProcesses, err := FindMaxProcessesCounts(mockFs, logger, []string{"source"})
	assert.Equal(t, maxProcesses, 10)
	assert.Nil(t, err)
}

func TestFindMaxProcessesSuccessMultiFile(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	payload := []byte("10")
	payload2 := []byte("20")
	mockFs := new(MockFs)
	mockFs.On("ReadFile", "source").Return(payload, nil)
	mockFs.On("ReadFile", "source2").Return(payload2, nil)
	maxProcesses, err := FindMaxProcessesCounts(mockFs, logger, []string{"source", "source2"})
	assert.Equal(t, maxProcesses, 20)
	assert.Nil(t, err)
}

func TestFindMaxProcessesFailInvalidFormat(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	payload := []byte("abc")
	mockFs := new(MockFs)
	mockFs.On("ReadFile", "source").Return(payload, nil)
	maxProcesses, err := FindMaxProcessesCounts(mockFs, logger, []string{"source"})
	// We expect the default value is returned if the file(s) cannot be read
	assert.Equal(t, maxProcesses, 0)
	assert.NotNil(t, err)
}
