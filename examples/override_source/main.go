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

package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/datto/copyondemand"
)

const (
	nbdToUse        = "/dev/nbd0"
	exampleFileSize = 1024 * 1024 // 1 MB
)

func main() {
	// Source files are immutable and we therefore only need to implement "ReadAt"
	sourceFile := &copyondemand.SyncSource{
		File: &oneFile{},
		Size: exampleFileSize,
	}
	// You could overwrite the backing file behavior as well. This is out of scope for this example.
	// For this example we will read files from our non-file source, but write them to a normal on-disk
	// file called "backing.bak"
	backingOsFile, err := os.OpenFile("./backing.bak", os.O_RDWR|os.O_CREATE, os.FileMode(int(0755)))
	backingFile := &copyondemand.SyncFile{
		File: backingOsFile,
		Size: exampleFileSize,
	}

	if err != nil {
		fmt.Printf("Could not open ./backing.bak: %s\n", err)
	}

	config := &copyondemand.DriverConfig{
		Source:               sourceFile,
		Backing:              backingFile,
		NbdFileName:          nbdToUse,
		EnableBackgroundSync: false,
		Resumable:            false,
	}

	driver, err := copyondemand.New(config)

	if err != nil {
		fmt.Printf("Could not construct driver: %s\n", err)
	}

	errorChan := make(chan error)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	// Connect blocks, so we wrap it in a routine
	go func(errorChan chan<- error) {
		if err := driver.Connect(); err != nil {
			errorChan <- err
		} else {
			fmt.Println("Driver stopped gracefully.")
		}
	}(errorChan)

	select {
	case <-sig:
		// Received SIGINT, cancel the sync operation and clean up
		break
	case err := <-errorChan:
		// Recieved an error, cancel the sync operation and clean up
		fmt.Printf("Driver stopped with error: %s\n", err)
		break
	}

	fmt.Println("Disconnecting...")
	driver.Disconnect()
}

// oneFile implements a copyondemand.ReadOnlyFile that contains only ones
type oneFile struct {
	copyondemand.ReadOnlyFile
}

// 0 indicates that this is not a traditional filesystem file, and therefore
// the COD driver shouldn't attempt to do out-of-band file operations.
// Namely - this non-file should not be scanned for sparseness with lseek.
func (f *oneFile) Fd() uintptr {
	return 0
}

// Fill the requested buffer with ones, regardless of offset.
// Always report that we read the full buffer size.
func (f *oneFile) ReadAt(p []byte, off int64) (n int, err error) {
	requestedReadSize := len(p)

	for ix := range p {
		p[ix] = 1
	}

	return requestedReadSize, nil
}
