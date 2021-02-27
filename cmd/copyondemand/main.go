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
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/datto/copyondemand"
	"github.com/sirupsen/logrus"
)

const (
	verbosityNormal verbosityLevel = iota
	verbosityVerbose
	verbosityVery
)

const (
	resumableCopyEnabled = true
)

type verbosityLevel int

// ProgressFiles is a type to hold paths for files that separate copy-on-demand processes use to interact
type ProgressFiles []string

func (i *ProgressFiles) String() string {
	return strings.Join(*i, ",")
}

// Set is public so that it's accessable by goflags
func (i *ProgressFiles) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-v] [-vv] [--progress-handle file_path] /dev/nbdX [file_to_mirror] [backing_file]\n", os.Args[0])
	flag.PrintDefaults()

	os.Exit(2)
}

var blockProgress uint64

func main() {
	verboseFlag := false
	veryVerboseFlag := false
	preventBackgroundSync := false
	useDblockDriver := false
	flag.BoolVar(&verboseFlag, "v", false, "Enable logging to stderr")
	flag.BoolVar(&veryVerboseFlag, "vv", false, "Enable very verbose logging to stderr")
	flag.BoolVar(&preventBackgroundSync, "b", false, "Disable background sync thread, for testing purposes only!")
	flag.BoolVar(&useDblockDriver, "d", false, "Use the kmod-dblock driver rather than the default NBD driver")

	var progressFiles ProgressFiles
	flag.Var(&progressFiles, "progress-handle", "The path to the file containing the number of active copy-on-demand processes")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 3 {
		usage()
	}

	errorChan := make(chan error)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	blockDeviceName := args[0]
	sourceFileName, err := filepath.Abs(args[1])
	if err != nil {
		logrus.Fatal(err)
	}
	backingFileName, err := filepath.Abs(args[2])
	if err != nil {
		logrus.Fatal(err)
	}

	verbosity := calculateVerbosityLevel(verboseFlag, veryVerboseFlag)
	err = setupLogging(verbosity, blockDeviceName, sourceFileName, backingFileName)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Infof("Progress handle file paths provided: %v .", progressFiles)
	_, err = copyondemand.FindMaxProcessesCounts(copyondemand.LocalFs{}, logrus.StandardLogger(), progressFiles)
	if err != nil {
		logrus.Fatalf("Could not read progress file(s): %v", progressFiles)
	}

	nbdDriver, err := copyondemand.NewFileBackedDevice(
		sourceFileName,
		backingFileName,
		blockDeviceName,
		progressFiles,
		copyondemand.LocalFs{},
		logrus.StandardLogger(),
		!preventBackgroundSync,
		resumableCopyEnabled,
		useDblockDriver,
	)

	if err != nil {
		logrus.Fatalf("Cannot create copy on demand device: %s\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func(errorChan chan<- error) {
		if err := nbdDriver.Connect(); err != nil {
			errorChan <- fmt.Errorf("Buse device stopped with error: %s", err)
		} else {
			logrus.Info("Buse device stopped gracefully.")
		}
	}(errorChan)

	go outputStatus(ctx, nbdDriver)

	/**
	 * TODO:
	 * In linux 4.4 something is holding a lock
	 * that causes our process to not cleanly terminate.
	 * This does not happen on 4.18 so I'm inclined to
	 * believe this isn't a user space issue. This
	 * sleep/terminate hack should no longer be needed
	 * after we get on the latest kernel.
	 */
	select {
	case <-sig:
		// Received SIGINT, cancel the sync operation and clean up
		break
	case err := <-errorChan:
		// Recieved an error, cancel the sync operation and clean up
		logrus.Error(err)
		break
	}
	cancel()
	logrus.Info("Disconnecting...")
	nbdDriver.Disconnect()

	time.Sleep(10 * time.Second)
}

// calculateVerbosityLevel produces a verbosity level given the verbosity level flags proivded
func calculateVerbosityLevel(verbose, veryVerbose bool) verbosityLevel {
	if veryVerbose {
		return verbosityVery
	} else if verbose {
		return verbosityVerbose
	}

	return verbosityNormal
}
