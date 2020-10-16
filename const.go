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
	"io"
	"io/ioutil"
	"os"
	"sync"
)

// BlockSize is the chunk size (in bytes) that are transferred between the source and backing files
const BlockSize = 4096 // 4k

// requestWorkerCount defines the number of parallel nbd processors that are run in buse
const requestWorkerCount = 5

// SyncFile is a simple struct to hold a file pointer and the Stat'd size of the file
type SyncFile struct {
	File File
	Size uint64
}

// SyncSource is a struct that points to the read-only source
type SyncSource struct {
	File ReadOnlyFile
	Size uint64
}

// FileBlock holds a mutex for a block and whether it has been synced to the backing file
type FileBlock struct {
	cached bool
	lock   *sync.Mutex
}

// BlockRange defines a contiguous, inclusive range of blocks
type BlockRange struct {
	Start uint64
	End   uint64
}

// Borrowed heavily from https://stackoverflow.com/questions/16742331/how-to-mock-abstract-filesystem-in-go
// golang doesn't have interfaces for os.File struct, meaning everyone has to write these thin wrapper
// interfaces for testing purposes.
// Discussion on possible improvements for golang2 https://github.com/golang/go/issues/14106

// FileSystem is an interface wrapper to the buildin os filesystem operations, for unit testability
type FileSystem interface {
	Open(name string) (File, error)
	Stat(name string) (os.FileInfo, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Rename(oldpath, newpath string) error
	Remove(name string) error
	ReadFile(name string) ([]byte, error)
}

// ReadOnlyFile provides an interface for the read functions of the native file struct
type ReadOnlyFile interface {
	io.ReaderAt
	Fd() uintptr
}

// File provides an interface for the native file struct
type File interface {
	ReadOnlyFile
	io.Closer
	io.Reader
	io.Seeker
	io.Writer
	io.WriterAt
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

// LocalFs implements FileSystem using the local disk.
type LocalFs struct{}

// Open opens a file with the native os function
func (LocalFs) Open(name string) (File, error) {
	return os.Open(name)
}

// OpenFile opens a file with the native os function
func (LocalFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

// Stat stats a file with the native os function
func (LocalFs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Rename renames a file using the native os function
func (LocalFs) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Remove removes a file using the native os function
func (LocalFs) Remove(name string) error {
	return os.Remove(name)
}

// ReadFile reads the full binary content of a provided file
func (LocalFs) ReadFile(name string) ([]byte, error) {
	return ioutil.ReadFile(name)
}
