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
	"strconv"
	"strings"
)

// FindMaxProcessesCounts returns the largest integer found from searching the files found
// at the location of each progress file path
func FindMaxProcessesCounts(reader FileSystem, log *logrus.Logger, progressFiles []string) (int, error) {
	var max int
	for _, progressFilePath := range progressFiles {
		contents, err := reader.ReadFile(progressFilePath)
		if err != nil {
			log.Warnf("Could not read process count file %s %v", progressFilePath, err)
			return max, err
		}
		numProcesses, err := readCodProcessFile(progressFilePath, contents)
		if err != nil {
			log.Warnf("Could not read process count file, file does not have the correct contents %s %v", progressFilePath, err)
			return max, err
		}
		if numProcesses > max {
			max = numProcesses
		}

	}
	return max, nil
}

func readCodProcessFile(progressFilePath string, contents []byte) (int, error) {
	numProcesses, err := strconv.Atoi(strings.Trim(string(contents), "\n"))
	if err != nil {
		logrus.Warnf("The contents of the progress file %s are unexpected. Expected single integer", progressFilePath)
	}
	return numProcesses, err
}
