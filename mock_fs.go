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
	"os"

	"github.com/stretchr/testify/mock"
)

// MockFs is a filesystem that isn't real
type MockFs struct {
	mock.Mock
	FileSystem
}

// MockFileInfo returns fake file info
type MockFileInfo struct {
	mock.Mock
	os.FileInfo
}

// MockFile is a fake file
type MockFile struct {
	output int
	mock.Mock
	File
}

// Stat mocks the stat os call
func (mfs *MockFs) Stat(name string) (os.FileInfo, error) {
	args := mfs.Called(name)

	return args.Get(0).(os.FileInfo), args.Error(1)
}

// OpenFile mocks the openfile os call
func (mfs *MockFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	args := mfs.Called(name, flag, perm)

	return args.Get(0).(File), args.Error(1)
}

// ReadFile mocks the readfile os call
func (mfs *MockFs) ReadFile(name string) ([]byte, error) {
	args := mfs.Called(name)

	return args.Get(0).([]byte), args.Error(1)
}

// Rename mocks the rename os call
func (mfs *MockFs) Rename(oldname, newname string) error {
	args := mfs.Called(oldname, newname)

	return args.Error(0)
}

// Remove mocks the remove os call
func (mfs *MockFs) Remove(name string) error {
	args := mfs.Called(name)

	return args.Error(0)
}

// Size returns a fake size
func (mfi *MockFileInfo) Size() int64 {
	args := mfi.Called()

	return args.Get(0).(int64)
}

// Name returns a fake name
func (mfi *MockFileInfo) Name() string {
	args := mfi.Called()

	return args.Get(0).(string)
}

// Truncate mocks the truncate call
func (mf *MockFile) Truncate(size int64) error {
	args := mf.Called(size)

	return args.Error(0)
}

// Sync mocks the sync call
func (mf *MockFile) Sync() error {
	args := mf.Called()

	return args.Error(0)
}

// ReadAt mocks the read call, and always reads 1's
func (mf *MockFile) ReadAt(b []byte, off int64) (n int, err error) {
	args := mf.Called(b, off)

	for index := range b {
		b[index] = byte(mf.output)
	}

	return args.Int(0), args.Error(1)
}

// WriteAt mocks the writeat call
func (mf *MockFile) WriteAt(b []byte, off int64) (n int, err error) {
	args := mf.Called(b, off)

	return args.Int(0), args.Error(1)
}

// Write mocks the write call
func (mf *MockFile) Write(b []byte) (n int, err error) {
	args := mf.Called(b)

	return args.Int(0), args.Error(1)
}

// Close mocks the close call
func (mf *MockFile) Close() error {
	args := mf.Called()

	return args.Error(0)
}

// Fd mocks a file descriptor
func (mf *MockFile) Fd() uintptr {
	args := mf.Called()

	return uintptr(args.Int(0))
}

// Stat mocks the file stat call
func (mf *MockFile) Stat() (os.FileInfo, error) {
	args := mf.Called()

	return args.Get(0).(os.FileInfo), args.Error(1)
}
