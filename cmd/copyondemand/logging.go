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
	"io/ioutil"
	"log/syslog"
	"os"

	"github.com/sirupsen/logrus"
	syslogrus "github.com/sirupsen/logrus/hooks/syslog"
)

const (
	syslogTag           = "copyondemand"
	nbdLogField         = "nbdDevice"
	sourceFileLogField  = "sourceFile"
	backingFileLogField = "backingFile"
)

// fieldLogHook will provide a constant set of fields to logrus.WithFields, such that all messages contain these fields.
// Implements logrus.Hook
type fieldLogHook struct {
	// fields will be passed as fields to all log entries that enter it
	fields logrus.Fields
	// levels will restrict this hook to certain levels. If null, the struct will hook to all log levels.
	levels []logrus.Level
}

// Levels returns all of the levels at which this hook will be fired.
func (hook fieldLogHook) Levels() []logrus.Level {
	if hook.levels != nil {
		return hook.levels
	}

	return logrus.AllLevels
}

// Fire dumps all stored fields into the given the given Entry.
func (hook fieldLogHook) Fire(entry *logrus.Entry) error {
	for key, value := range hook.fields {
		entry.Data[key] = value
	}

	return nil
}

// setupLogging setups up all logging for this run of the utility.
func setupLogging(verbosity verbosityLevel, nbdDevice, sourceFileName, backingFileName string) error {
	setupLogOutput(verbosity)
	setupLogFormatting()
	err := setupLogHooks(nbdDevice, sourceFileName, backingFileName)
	if err != nil {
		return fmt.Errorf("could not setup logging: %s", err)
	}

	return nil
}

// setupLogOutput sets up logging output based on whether or not debug is enabled
func setupLogOutput(verbosity verbosityLevel) {
	if verbosity == verbosityNormal {
		logrus.SetOutput(ioutil.Discard)
		return
	}

	// If we have any level of verbosity, start piping things to stderr
	logrus.SetOutput(os.Stderr)
	// Any verbosity of very or higher (should one eventually exist) should emit debug logs.
	if verbosity >= verbosityVery {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

// setupLogFormatting sets the formatting for the logger.
func setupLogFormatting() {
	formatter := &logrus.JSONFormatter{}
	logrus.SetFormatter(formatter)
}

// setupLogHooks injects log hooks for the given information about this run of the utility.
func setupLogHooks(nbdDevice, sourceFileName, backingFileName string) error {
	fields := logrus.Fields{
		nbdLogField:         nbdDevice,
		sourceFileLogField:  sourceFileName,
		backingFileLogField: backingFileName,
	}

	hook := fieldLogHook{fields: fields}
	logrus.AddHook(hook)

	syslogHook, err := syslogrus.NewSyslogHook("", "", syslog.LOG_USER|syslog.LOG_INFO, syslogTag)
	if err != nil {
		return fmt.Errorf("could not setup syslog hook: %s", err)
	}

	logrus.AddHook(syslogHook)

	return nil
}
