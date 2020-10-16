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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/sirupsen/logrus"
)

// maxUInt returns the maximum of 2 unsigned integer values
// The standard go library only has a Max function for floats :(
func maxUInt(x, y uint64) uint64 {
	if x > y {
		return x
	}

	return y
}

// uint64ToBytes returns the little endian representation of an unsigned 64 bit int
func uint64ToBytes(i uint64) []byte {
	returnBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(returnBytes, i)

	return returnBytes
}

func applyUint64(i uint64, buf []byte) {
	binary.LittleEndian.PutUint64(buf, i)
}

func applyUint64AtOffset(i uint64, buf []byte, offset uint64) uint64 {
	newOffset := offset + 8
	applyBuf := buf[offset:newOffset]
	applyUint64(i, applyBuf)

	return newOffset
}

func readUint64(r *bytes.Reader) (uint64, error) {
	littleEndianBytes := make([]byte, 8)
	n, err := r.Read(littleEndianBytes)
	if err != nil {
		return 0, err
	}

	if n != 8 {
		return 0, fmt.Errorf("Attempted to read 8 byte int but only got %d bytes", n)
	}

	return binary.LittleEndian.Uint64(littleEndianBytes), nil
}

func panicOnError(err error, log *logrus.Logger) {
	if err == nil {
		return
	}

	log.Error(err)
	panic(err)
}
