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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	flushPeriodMilliseconds = 5000
	intentFmt               = "%s.wil"
	failoverIntentFmt       = intentFmt + ".1"
	blockMapFmt             = "%s.bak"
	failoverBlockMapFmt     = blockMapFmt + ".1"
	blockWriteID            = 1
	dirtyBlockWriteID       = 2
	intentLogVersion        = 1                 // Future proof format changes
	blockMapVersion         = 1                 // Future proof format changes
	intentLogMagicInt       = 21472736782339907 // "COD WIL\0" interpreted as a little endian 64 bit int
	blockMapMagicInt        = 21182375518293827 // "COD BAK\0" interpreted as a little endian 64 bit int
)

var intentLogMagic = []byte{0x43, 0x4F, 0x44, 0x20, 0x57, 0x49, 0x4C, 0x00} // "COD WIL\0"
var blockMapMagic = []byte{0x43, 0x4F, 0x44, 0x20, 0x42, 0x41, 0x4B, 0x00}  // "COD BAK\0"

// blockMapIntentLogger is responsible for periodically
// flushing the block map to disk, as well as keeping
// an intent log for future block map flushes
type blockMapIntentLogger struct {
	bufferPool       *bytePool
	writeActionPool  *sync.Pool
	transactionPool  *sync.Pool
	intentLock       *sync.Mutex
	blockMapLock     *sync.Mutex
	fs               FileSystem
	currentIntentLog File
	backingFile      string
	logger           *logrus.Logger
}

// newBlockMapIntentLogger constructs a block map intent logger
func newBlockMapIntentLogger(bufferPool *bytePool, fs FileSystem, backingFile string, logger *logrus.Logger) *blockMapIntentLogger {
	writeActionPool := &sync.Pool{
		New: func() interface{} {
			return &blockWriteAction{}
		},
	}

	transactionPool := &sync.Pool{
		New: func() interface{} {
			return &blockMapIntentTransaction{}
		},
	}

	return &blockMapIntentLogger{
		bufferPool,
		writeActionPool,
		transactionPool,
		&sync.Mutex{},
		&sync.Mutex{},
		fs,
		nil,
		backingFile,
		logger,
	}
}

// init is responsible for, on resuming, fixing any invalid crashed state
func (bm *blockMapIntentLogger) init(f *FileBackedDevice) {
	primaryIntentLogName := fmt.Sprintf(intentFmt, bm.backingFile)
	primaryIntentLogExists := bm.intentFileExists(primaryIntentLogName)
	failoverIntentLogName := fmt.Sprintf(failoverIntentFmt, bm.backingFile)
	failoverIntentLogExists := bm.intentFileExists(failoverIntentLogName)
	primaryBlockMapName := fmt.Sprintf(blockMapFmt, bm.backingFile)
	primaryBlockMapExists := bm.intentFileExists(primaryBlockMapName)
	failoverBlockMapName := fmt.Sprintf(failoverBlockMapFmt, bm.backingFile)
	failoverBlockMapExists := bm.intentFileExists(failoverBlockMapName)

	if primaryBlockMapExists && failoverBlockMapExists {
		// Both files existing means we crashed mid-flush, apply the _failover_ file
		// since we're guaranteed to have intent logs for all writes after the failover file
		// Delete the primary, because it's useless/maybe even half-written
		panicOnError(bm.applyBlockMap(failoverBlockMapName, f), bm.logger)
		panicOnError(bm.fs.Remove(primaryBlockMapName), bm.logger)
		panicOnError(bm.fs.Rename(failoverBlockMapName, primaryBlockMapName), bm.logger)
	} else if failoverBlockMapExists {
		// Only the failover exists, this means we crashed a bit later, still apply this file
		// Leave this in the failover location, since we're just about to re-flush
		panicOnError(bm.applyBlockMap(failoverBlockMapName, f), bm.logger)
	} else if primaryBlockMapExists {
		// This is the happy path, we probably didn't crash
		// Move primary into failover's place, since we're just about to re-flush
		panicOnError(bm.applyBlockMap(primaryBlockMapName, f), bm.logger)
		panicOnError(bm.fs.Rename(primaryBlockMapName, failoverBlockMapName), bm.logger)
	} else {
		// We're init'ing in resumable mode, but no block map has ever been flushed
		// This means we're starting for the first time. We flush a (guaranteed empty)
		// block map, and initialize our intent log.
		bm.flushPrimaryBlockMap(f)
		bm.initWilPointer(primaryIntentLogName)

		return
	}

	if failoverIntentLogExists {
		panicOnError(bm.applyIntent(failoverIntentLogName, f), bm.logger)
	}
	if primaryIntentLogExists {
		panicOnError(bm.applyIntent(primaryIntentLogName, f), bm.logger)
	}

	// At this point we have everything successfully in memory, we need to re-flush our block
	// map before we delete anything, just in case we crash
	bm.flushPrimaryBlockMap(f)

	// If either map existed when we started, we would have loaded it => moved it to failover
	// So now that we have a clean flush, we can delete that file
	if primaryBlockMapExists || failoverBlockMapExists {
		panicOnError(bm.fs.Remove(failoverBlockMapName), bm.logger)
	}

	// All intents have been applied/safely flushed to a new block map
	// So now we can delete the old intent files
	if primaryIntentLogExists {
		panicOnError(bm.fs.Remove(primaryIntentLogName), bm.logger)
	}
	if failoverIntentLogExists {
		panicOnError(bm.fs.Remove(failoverIntentLogName), bm.logger)
	}

	bm.initWilPointer(primaryIntentLogName)

	// Check if loading the block map caused us to be done syncing
	f.CheckSynced()
}

// getWriteTransaction returns a new transaction that can be used to record a write
func (bm *blockMapIntentLogger) getWriteTransaction() *blockMapIntentTransaction {
	transaction := bm.transactionPool.Get().(*blockMapIntentTransaction)
	transaction.reset()

	return transaction
}

// flushWriteTransaction records a write action in the write intent log
// The transaction passed in is not safe to reuse
func (bm *blockMapIntentLogger) flushWriteTransaction(transaction *blockMapIntentTransaction) error {
	serializedBytes := transaction.serialize(bm.bufferPool)
	var err error
	if serializedBytes != nil {
		err = bm.writeIncrementalIntent(serializedBytes)
	}
	bm.discardWriteTransaction(transaction)
	bm.bufferPool.put(serializedBytes)

	return err
}

// discardWriteTransaction adds a transaction back to the transaction pool, making it no longer safe to use
func (bm *blockMapIntentLogger) discardWriteTransaction(transaction *blockMapIntentTransaction) {
	bm.transactionPool.Put(transaction)
}

// periodicFlush is a blocking function that perioidically flushes the block map to disk
func (bm *blockMapIntentLogger) periodicFlush(f *FileBackedDevice) {
	f.terminationWaitGroup.Add(1)
	defer f.terminationWaitGroup.Done()

	periodDuration := flushPeriodMilliseconds * time.Millisecond
	lastFlush := time.Now()
	for !isCancelSignaled(f.terminationContext) && !f.IsFullySynced() {
		// Check if we're cancelled every 500ms
		time.Sleep(500 * time.Millisecond)

		if time.Since(lastFlush) > periodDuration {
			bm.flushIntent(f, true)
			lastFlush = time.Now()
		}
	}
}

// finalize flushes the block map and cleans up any unnecessary files
// Only for use on shutdown. This will almost definitely panic other threads
// if other threads are running.
func (bm *blockMapIntentLogger) finalize(f *FileBackedDevice) {
	bm.flushIntent(f, false)
}

// flushIntent atomically flushes the block map to disk
func (bm *blockMapIntentLogger) flushIntent(f *FileBackedDevice, reinitialize bool) {
	wilFileExisted := false
	var existingPointer File
	// First save the old intent log to wil.1, then repoint all intent log writes to a new intent log file
	bm.intentLock.Lock()
	intentFileName := fmt.Sprintf(intentFmt, bm.backingFile)

	// If an existing intent log file is present, back it up
	if bm.intentFileExists(intentFileName) {
		wilFileExisted = true
		existingPointer = bm.currentIntentLog
		panicOnError(bm.fs.Rename(intentFileName, fmt.Sprintf(failoverIntentFmt, bm.backingFile)), bm.logger)
	}

	if reinitialize {
		bm.initWilPointer(intentFileName)
	} else {
		bm.currentIntentLog = nil
	}

	// Writes are free to continue, we haven't flushed the block map yet, but we have a copy of our intent log
	bm.intentLock.Unlock()

	bm.blockMapLock.Lock()
	defer bm.blockMapLock.Unlock()
	backingFileName := fmt.Sprintf(blockMapFmt, bm.backingFile)
	backingFileExisted := false

	// If an existing backing file is present, back it up
	if bm.intentFileExists(backingFileName) {
		backingFileExisted = true
		panicOnError(bm.fs.Rename(backingFileName, fmt.Sprintf(failoverBlockMapFmt, bm.backingFile)), bm.logger)
	}

	// Flush a new block map to disk
	bm.flushPrimaryBlockMap(f)

	if backingFileExisted {
		panicOnError(os.Remove(fmt.Sprintf(failoverBlockMapFmt, bm.backingFile)), bm.logger)
	}
	if wilFileExisted {
		panicOnError(existingPointer.Close(), bm.logger)
		panicOnError(os.Remove(fmt.Sprintf(failoverIntentFmt, bm.backingFile)), bm.logger)
	}
}

func (bm *blockMapIntentLogger) initWilPointer(logName string) {
	newWilPointer, err := bm.fs.OpenFile(logName, os.O_RDWR|os.O_CREATE, 0755)
	panicOnError(err, bm.logger)
	panicOnError(bm.writeHeader(newWilPointer, intentLogMagic, intentLogVersion), bm.logger)
	bm.currentIntentLog = newWilPointer
}

func (bm *blockMapIntentLogger) writeHeader(file File, magic []byte, version uint64) error {
	headerBytes := make([]byte, 0)
	headerBytes = append(headerBytes, magic...)
	headerBytes = append(headerBytes, uint64ToBytes(version)...)

	_, err := file.Write(headerBytes)

	return err
}

func (bm *blockMapIntentLogger) validateHeader(reader *bytes.Reader, expectedMagic, expectedVersion uint64) {
	magic, err := readUint64(reader)
	panicOnError(err, bm.logger)
	version, err := readUint64(reader)
	panicOnError(err, bm.logger)

	if magic != expectedMagic {
		panicOnError(fmt.Errorf("Expected magic %d, got %d", expectedMagic, magic), bm.logger)
	}

	if version != expectedVersion {
		panicOnError(fmt.Errorf("This version of COD handles format version %d, got %d", expectedVersion, version), bm.logger)
	}
}

func (bm *blockMapIntentLogger) flushPrimaryBlockMap(f *FileBackedDevice) {
	backingFileName := fmt.Sprintf(blockMapFmt, bm.backingFile)
	dirtyBlockMapBytes := f.dirtyBlockMap.serialize()
	blockMapBytes, err := f.blockMap.serialize()
	panicOnError(err, bm.logger)
	newBlockMap, err := bm.fs.OpenFile(backingFileName, os.O_RDWR|os.O_CREATE, 0755)
	panicOnError(err, bm.logger)
	panicOnError(bm.writeHeader(newBlockMap, blockMapMagic, blockMapVersion), bm.logger)
	_, err = newBlockMap.Write(blockMapBytes)
	panicOnError(err, bm.logger)
	_, err = newBlockMap.Write(dirtyBlockMapBytes)
	panicOnError(err, bm.logger)
	panicOnError(newBlockMap.Close(), bm.logger)
}

func (bm *blockMapIntentLogger) applyBlockMap(blockMapBackupFile string, f *FileBackedDevice) error {
	blockMapContent, err := bm.fs.ReadFile(blockMapBackupFile)
	panicOnError(err, bm.logger)
	blockMapReader := bytes.NewReader(blockMapContent)
	bm.validateHeader(blockMapReader, blockMapMagicInt, blockMapVersion)
	roaringIx := 0

	// Read roaring bitmap
	for readSize, err := readUint64(blockMapReader); readSize != 0 || err != nil; readSize, err = readUint64(blockMapReader) {
		if err != nil {
			return err
		}
		roaringBuffer := make([]byte, readSize)
		n, err := blockMapReader.Read(roaringBuffer)
		if err != nil {
			return err
		}
		if n != len(roaringBuffer) {
			return fmt.Errorf("Could not full serialized bitmap from bitmap backup")
		}
		if err = f.blockMap.deserialize(roaringBuffer, roaringIx); err != nil {
			return err
		}

		roaringIx++
	}

	// Read dirty block map
	for readSize, err := readUint64(blockMapReader); readSize != 0 || err != nil; readSize, err = readUint64(blockMapReader) {
		if err != nil {
			return err
		}

		blockIx, err := readUint64(blockMapReader)
		if err != nil {
			return err
		}

		offset, err := readUint64(blockMapReader)
		if err != nil {
			return err
		}

		length, err := readUint64(blockMapReader)
		if err != nil {
			return err
		}

		// We don't need a dedicated deserialize function, since there should be a vanishingly small number of these
		// Even if there weren't this is about as fast as it gets anyway (the only thing we could possibly eliminate would be write merging)
		f.dirtyBlockMap.recordWrite(blockIx, offset, int(length))
	}

	return nil
}

func (bm *blockMapIntentLogger) applyIntent(intentFile string, f *FileBackedDevice) error {
	intentFileContents, err := bm.fs.ReadFile(intentFile)
	panicOnError(err, bm.logger)
	intentFileReader := bytes.NewReader(intentFileContents)
	bm.validateHeader(intentFileReader, intentLogMagicInt, intentLogVersion)

	for intentType, err := readUint64(intentFileReader); intentType != 0 || err != nil; intentType, err = readUint64(intentFileReader) {
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		blockIx, err := readUint64(intentFileReader)
		if err != nil {
			return err
		}

		if intentType == blockWriteID {
			f.blockMap.setBlock(blockIx, true)

			// We need to clear any dirty blocks, if we get a confirmed flush
			// for that block after dirty'ing it (which should happen ~100% of the time)
			f.dirtyBlockMap.remove(blockIx)
		} else if intentType == dirtyBlockWriteID {
			offset, err := readUint64(intentFileReader)
			if err != nil {
				return err
			}

			length, err := readUint64(intentFileReader)
			if err != nil {
				return err
			}

			// Make sure this block didn't get synced at some point!
			// Our intent logs are guaranteed to be in order, but there's no
			// guarantee even in the normal case that we won't momentarily
			// have things in our dirty block map that then become synced in
			// another routine.
			if !f.blockMap.isSynced(blockIx) {
				f.dirtyBlockMap.recordWrite(blockIx, offset, int(length))
			}
		} else {
			return fmt.Errorf("Got unknown intent type %d", intentType)
		}
	}

	return nil
}

func (bm *blockMapIntentLogger) writeIncrementalIntent(intentEntry []byte) error {
	bm.intentLock.Lock()
	defer bm.intentLock.Unlock()

	_, err := bm.currentIntentLog.Write(intentEntry)

	return err
}

func (bm *blockMapIntentLogger) intentFileExists(file string) bool {
	if _, err := bm.fs.Stat(file); err != nil {
		return false
	}

	return true
}
