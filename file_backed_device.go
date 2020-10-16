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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"
)

// FileBackedDevice is the main BUSE driver object.
// The BUSE driver calls functions on this struct when associated read/write operations are sent from the kernel.
type FileBackedDevice struct {
	Source                 *SyncSource
	BackingFile            *SyncFile
	nbdFile                string
	processFiles           []string
	terminationContext     context.Context
	terminationFunction    context.CancelFunc
	terminationWaitGroup   *sync.WaitGroup
	blockMap               *blockMap
	blockMapIntentLogger   *blockMapIntentLogger
	dirtyBlockMap          *dirtyBlockMap
	bufferPool             *bytePool
	writerQueue            *WriterQueue
	rangeLocker            *RangeLocker
	diskActionTracker      *diskActionTracker
	blockRangePool         *sync.Pool
	isSyncedCtx            context.Context
	SetSynced              context.CancelFunc
	resumable              bool
	log                    *logrus.Logger
	bd                     *buseDevice
	enableBackgroundSync   bool
	copyRateBytesPerSecond uint64
	copyRateLock           *sync.Mutex
}

// DriverConfig contains the needed data to construct a driver
// If you are using a traditional filesystem (i.e. not overwriting
// the source or backing file interfaces) you can use NewFileBackedDevice
// to construct a driver based on on-disk file names.
type DriverConfig struct {
	Source               *SyncSource
	Backing              *SyncFile
	NbdFileName          string
	ProcessFiles         []string
	Fs                   FileSystem
	Log                  *logrus.Logger
	EnableBackgroundSync bool
	Resumable            bool
}

// NewFileBackedDevice constructs a FileBackedDevice based on a source file
func NewFileBackedDevice(
	sourceFileName string,
	backingFileName string,
	nbdFileName string,
	processFiles []string,
	fs FileSystem,
	log *logrus.Logger,
	enableBackgroundSync bool,
	resumable bool,
) (*FileBackedDevice, error) {
	sourceFileInfo, err := fs.Stat(sourceFileName)
	if err != nil {
		return nil, fmt.Errorf("source file %s does not exist", sourceFileName)
	}

	size := uint64(sourceFileInfo.Size())
	f, err := fs.OpenFile(sourceFileName, os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open provided file: %s", err)
	}

	sourceFile := &SyncSource{File: f, Size: size}
	backingExists := false
	if _, err := fs.Stat(backingFileName); err == nil {
		backingExists = true
		if !resumable {
			return nil, fmt.Errorf("the backing file %s already exists", backingFileName)
		}
	}

	backingFileRoot := filepath.Dir(backingFileName)
	if _, err := fs.Stat(backingFileRoot); err != nil {
		return nil, fmt.Errorf("the backing file root directory %s does not exist", backingFileRoot)
	}

	backingFp, err := fs.OpenFile(backingFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("Cannot open backing file: %s", err)
	}

	if !backingExists {
		err = backingFp.Truncate(int64(size))
		if err != nil {
			return nil, fmt.Errorf("could not truncate backing file: %s", err)
		}
	}
	backingFile := &SyncFile{File: backingFp, Size: size}

	config := &DriverConfig{
		Source:               sourceFile,
		Backing:              backingFile,
		NbdFileName:          nbdFileName,
		ProcessFiles:         processFiles,
		Fs:                   fs,
		Log:                  log,
		EnableBackgroundSync: enableBackgroundSync,
		Resumable:            resumable,
	}

	return New(config)
}

// New constructs a FileBackedDevice with potentially overwritten
// source and backing file interfaces. If you are using a traditional
// filesystem (i.e. not overwriting the source or backing file interfaces)
// you can use NewFileBackedDevice to construct a driver based on on-disk
// file names.
func New(config *DriverConfig) (*FileBackedDevice, error) {
	config, err := setDefaultsAndCopy(config)
	if err != nil {
		return nil, err
	}
	blockRangePool := &sync.Pool{
		New: func() interface{} {
			return &BlockRange{}
		},
	}
	totalBlocks := (config.Source.Size / BlockSize) + 1
	blockMap := newBlockMap(totalBlocks, config.Log)
	dirtyBlockMap := newDirtyBlockMap(config.Log)
	bufferPool := newBytePool(config.Log)
	writerQueue := NewWriterQueue()
	rangeLocker := NewRangeLocker(blockRangePool)
	diskActionTracker := newDiskActionTracker()
	backingInfo, err := config.Backing.File.Stat()
	if err != nil {
		return nil, err
	}
	backingFileName := backingInfo.Name()
	blockMapIntentLogger := newBlockMapIntentLogger(bufferPool, config.Fs, backingFileName, config.Log)
	isSyncedCtx, setSynced := context.WithCancel(context.Background())

	driver := &FileBackedDevice{
		Source:                 config.Source,
		BackingFile:            config.Backing,
		nbdFile:                config.NbdFileName,
		blockMap:               blockMap,
		blockMapIntentLogger:   blockMapIntentLogger,
		dirtyBlockMap:          dirtyBlockMap,
		bufferPool:             bufferPool,
		writerQueue:            writerQueue,
		rangeLocker:            rangeLocker,
		diskActionTracker:      diskActionTracker,
		blockRangePool:         blockRangePool,
		isSyncedCtx:            isSyncedCtx,
		SetSynced:              setSynced,
		resumable:              config.Resumable,
		log:                    config.Log,
		copyRateBytesPerSecond: defaultMaxMachineBytesPerSecond,
		copyRateLock:           &sync.Mutex{},
	}

	return driver, nil
}

func setDefaultsAndCopy(config *DriverConfig) (*DriverConfig, error) {
	newConfig := &DriverConfig{
		Source:               config.Source,
		Backing:              config.Backing,
		NbdFileName:          config.NbdFileName,
		ProcessFiles:         append([]string(nil), config.ProcessFiles...),
		Fs:                   config.Fs,
		Log:                  config.Log,
		EnableBackgroundSync: config.EnableBackgroundSync,
		Resumable:            config.Resumable,
	}

	if newConfig.Log == nil {
		newConfig.Log = logrus.StandardLogger()
	}

	if newConfig.Source == nil {
		err := errors.New("Source file cannot be nil")
		newConfig.Log.Error(err)
		return nil, err
	}

	if newConfig.Backing == nil {
		err := errors.New("Backing file cannot be nil")
		newConfig.Log.Error(err)
		return nil, err
	}

	if len(newConfig.NbdFileName) == 0 {
		err := errors.New("Nbd file name must be provided")
		newConfig.Log.Error(err)
		return nil, err
	}

	if newConfig.Fs == nil {
		newConfig.Fs = LocalFs{}
	}

	return newConfig, nil
}

// Connect is a blocking function that initiates the NBD device driver
func (d *FileBackedDevice) Connect() error {
	d.Resume()

	ctx, cancel := context.WithCancel(context.Background())
	waitGroup := &sync.WaitGroup{}

	d.terminationContext = ctx
	d.terminationFunction = cancel
	d.terminationWaitGroup = waitGroup

	d.ProcessQueues(ctx, waitGroup)

	if d.enableBackgroundSync && !d.IsFullySynced() {
		go trackDynamicBandwidthLimit(ctx, waitGroup, d)
		go syncAllBlocks(d)
	}

	if d.resumable {
		d.EnqueueAllDirtyBlocks()
	}

	if d.bd == nil {
		buseDevice, err := createNbdDevice(
			d.nbdFile,
			calculateNbdSize(d.Source.Size),
			d,
			newBytePool(d.log),
			d.blockRangePool,
		)
		if err != nil {
			return err
		}
		d.bd = buseDevice
	}

	return d.bd.connect()
}

// Disconnect terminates the NBD driver connection. This call blocks
// while write queues are flushing, and the intent log is finalizing.
// Ending the program without calling disconnect will be treated as a
// crash for the purposes of resuming.
func (d *FileBackedDevice) Disconnect() {
	d.terminationFunction()
	d.terminationWaitGroup.Wait()
	if d.bd != nil {
		d.bd.disconnect()
	}
	d.Finalize()
}

// GetBackgroundCopyRate gets the current background copy rate
func (d *FileBackedDevice) GetBackgroundCopyRate() uint64 {
	var returnRate uint64
	d.copyRateLock.Lock()
	defer d.copyRateLock.Unlock()
	returnRate = d.copyRateBytesPerSecond

	return returnRate
}

// SetBackgroundCopyRate sets the current background copy rate
// Returns whether the value was updated
func (d *FileBackedDevice) SetBackgroundCopyRate(rateInBytesPerSecond uint64) bool {
	d.copyRateLock.Lock()
	defer d.copyRateLock.Unlock()
	updated := d.copyRateBytesPerSecond != rateInBytesPerSecond
	d.copyRateBytesPerSecond = rateInBytesPerSecond

	return updated
}

// ProcessQueues is a non-blocking function that begins required background processing for
// this FileBackedDevice. Backgrounded processes increment the provided WaitGroup, and self
// terminate when cancellation is signalled on the provided Context.
func (d *FileBackedDevice) ProcessQueues(ctx context.Context, waitGroup *sync.WaitGroup) {
	go processWriteQueue(d)
	go d.diskActionTracker.processQueue(ctx, waitGroup)
	go d.blockMapIntentLogger.periodicFlush(d)
}

// SampleRate returns the rate (in bytes per second) for the given interval
func (d *FileBackedDevice) SampleRate(actionType DiskActionType, intervalMilliseconds uint64) uint64 {
	bytes := float64(d.diskActionTracker.Sample(actionType, intervalMilliseconds))
	seconds := float64(intervalMilliseconds) / float64(1000)

	return uint64(bytes / seconds)
}

// Resume reads any on-disk block maps that were written by previous executions
// and initialized the write intent log
func (d *FileBackedDevice) Resume() {
	if d.resumable {
		d.blockMapIntentLogger.init(d)
	}
}

// Finalize runs any necessary cleanup tasks
func (d *FileBackedDevice) Finalize() {
	if d.resumable {
		d.blockMapIntentLogger.finalize(d)
	}
}

// EnqueueAllDirtyBlocks adds all dirty blocks to the write queue
func (d *FileBackedDevice) EnqueueAllDirtyBlocks() {
	dirtyBlocks := d.dirtyBlockMap.allDirtyBlocks()

	for _, block := range dirtyBlocks {
		d.enqueueDirtyWrite(block)
	}
}

// TotalSyncedBlocks returns how many blocks are present on the backing file
func (d *FileBackedDevice) TotalSyncedBlocks() uint64 {
	return d.blockMap.totalSynced()
}

// CheckSynced checks if the backing file is fully synced, and if so
// cancels the sync cancellation context
func (d *FileBackedDevice) CheckSynced() {
	totalBlocks := (d.Source.Size / BlockSize) + 1

	if d.TotalSyncedBlocks() == totalBlocks {
		d.SetSynced()
	}
}

// ReadAt is called by the BUSE driver in response to read requests from the kernel.
// * If there are no unsynced blocks in the range, pass through the read to the backing file
// * If there are unsynced blocks in the range
//     - Do a continuous read from source from the first unsynced block => last unsynced block
//     - Read any synced blocks from the backing file
//     - If any unsynced blocks are dirty, reconcile them using the dirty block map
//     - Enqueue the reconciled buffer to be flushed to disk
//     - Return requested data to the user
func (d *FileBackedDevice) ReadAt(p []byte, off uint64) error {
	d.log.Debugf("[FileBackedDevice] READ offset:%d len:%d", off, len(p))
	if d.IsFullySynced() {
		return d.readBacking(p, off)
	}

	affectedBlockRange := d.blockRangePool.Get().(*BlockRange)
	defer d.blockRangePool.Put(affectedBlockRange)
	getAffectedBlockRange(uint64(len(p)), off, affectedBlockRange)

	firstUnsyncedBlock := int64(-1)
	lastUnsyncedBlock := int64(-1)

	// Note: we could perhaps come up with a better heuristic here,
	// this avoids round trips at all costs by doing a continuous read
	// from the source from the first unsynced block to the last unsynced
	// block
	for i := affectedBlockRange.Start; i <= affectedBlockRange.End; i++ {
		if !d.blockMap.isSynced(i) {
			if firstUnsyncedBlock == -1 {
				firstUnsyncedBlock = int64(i)
			}
			lastUnsyncedBlock = int64(i)
		}
	}

	if firstUnsyncedBlock == -1 {
		// Fast path, all blocks are synced
		return d.readBacking(p, off)
	}

	affectedLength := ((affectedBlockRange.End - affectedBlockRange.Start) + 1) * BlockSize
	reconciliationBuffer := d.bufferPool.get(affectedLength)

	unsyncedRangeStart := (uint64(firstUnsyncedBlock) - affectedBlockRange.Start) * BlockSize
	unsyncedRangeEnd := (uint64(lastUnsyncedBlock+1) - affectedBlockRange.Start) * BlockSize
	sourceBuffer := reconciliationBuffer[unsyncedRangeStart:unsyncedRangeEnd]

	// Note: We don't yet have a range lock here, we're reading the blocks we think are not synced based
	// on our race-y read of the block map. This means we may read blocks that eventually become synced.
	// The tradeoff is that we've avoided locking our entire block range, so flushes can continue to happen
	// in our affected range (if they exist).
	err := d.readSource(sourceBuffer, uint64(firstUnsyncedBlock)*BlockSize)
	if err != nil {
		return err
	}

	d.rangeLocker.LockRange(affectedBlockRange.Start, affectedBlockRange.End)
	defer d.rangeLocker.UnlockRange(affectedBlockRange.Start, affectedBlockRange.End)

	// Fill in any synced blocks (from backing), and reconcile any dirty writes
	for i := affectedBlockRange.Start; i <= affectedBlockRange.End; i++ {
		currentBufferOffset := (i * BlockSize) - (affectedBlockRange.Start * BlockSize)
		currentSlice := reconciliationBuffer[currentBufferOffset:(currentBufferOffset + BlockSize)]

		// These backing reads are unbatched, can we batch them?
		if d.blockMap.isSynced(i) {
			err := d.readBacking(currentSlice, i*BlockSize)
			if err != nil {
				return err
			}
		} else if d.dirtyBlockMap.isDirty(i) {
			dirtyBuffer := d.bufferPool.get(BlockSize)
			err := d.readBacking(dirtyBuffer, i*BlockSize)
			if err != nil {
				return err
			}
			d.dirtyBlockMap.reconcileBlock(i, currentSlice, dirtyBuffer)
			d.bufferPool.put(dirtyBuffer)
		}
	}

	// At this point our reconciliationBuffer is fully in sync with reality.
	// Copy to the response buffer, and enqueue the buffer for flushing.
	alignedReadStart := off - (affectedBlockRange.Start * BlockSize)
	syncSlice := reconciliationBuffer[alignedReadStart:(alignedReadStart + uint64(len(p)))]
	copy(p, syncSlice)

	writeAction := d.writerQueue.MakeWriteAction()
	writeAction.actionType = WriteData
	writeAction.startBlock = affectedBlockRange.Start
	writeAction.endBlock = affectedBlockRange.End
	writeAction.data = reconciliationBuffer
	d.writerQueue.TryEnqueue(writeAction, 0)

	return nil
}

// WriteAt is called by the BUSE driver in response to write requests from the kernel.
// * Write requests are always passed straight to the backing file
// * Any blocks in the middle of a write are considered "synced" since they are fully overwritten
// * Any partially written blocks are recorded in the dirty block map, and enqueued to be fixed by the flush queue
func (d *FileBackedDevice) WriteAt(p []byte, off uint64) error {
	d.log.Debugf("[FileBackedDevice] WRITE offset:%d len:%d", off, len(p))
	if d.IsFullySynced() {
		return d.writeBacking(p, off)
	}

	writeLength := uint64(len(p))
	affectedBlockRange := d.blockRangePool.Get().(*BlockRange)
	defer d.blockRangePool.Put(affectedBlockRange)
	getAffectedBlockRange(writeLength, off, affectedBlockRange)

	d.rangeLocker.LockRange(affectedBlockRange.Start, affectedBlockRange.End)
	defer d.rangeLocker.UnlockRange(affectedBlockRange.Start, affectedBlockRange.End)
	wilTransaction := d.blockMapIntentLogger.getWriteTransaction()

	// Starting at the first block _after_ start, and ending at the last block
	// _before_ end. Mark blocks "synced". This makes sense because the blocks
	// in the middle of a write will be fully overwritten, and we therefore
	// never need to read the data from source. The first or last block being
	// fully overwritten is handled below.
	for i := affectedBlockRange.Start + 1; i < affectedBlockRange.End; i++ {
		d.blockMap.setBlock(i, true)
		wilTransaction.setEndBlock(i)
	}

	wilTransaction.setStartBlock(affectedBlockRange.Start + 1)
	relativeWriteStart := off - (affectedBlockRange.Start * BlockSize)

	if !d.blockMap.isSynced(affectedBlockRange.Start) {
		// Calculate the range written in the start block
		if relativeWriteStart == 0 && writeLength >= BlockSize {
			// When the entire start block is written
			d.blockMap.setBlock(affectedBlockRange.Start, true)
			wilTransaction.setStartBlock(affectedBlockRange.Start)
		} else {
			// Part of the start block is written
			startBlockWriteLength := writeLength
			if relativeWriteStart+writeLength > BlockSize {
				startBlockWriteLength = BlockSize - relativeWriteStart
			}
			d.dirtyBlockMap.recordWrite(affectedBlockRange.Start, relativeWriteStart, int(startBlockWriteLength))
			d.enqueueDirtyWrite(affectedBlockRange.Start)
			wilTransaction.recordDirtyWrite(d.blockMapIntentLogger, affectedBlockRange.Start, relativeWriteStart, int(startBlockWriteLength))
		}
	}

	// Calculate the range written in the end block (if there is an end block)
	if affectedBlockRange.Start != affectedBlockRange.End && !d.blockMap.isSynced(affectedBlockRange.End) {
		// This write always overwrites the beginning of the last block
		relativeEndWriteStart := uint64(0)
		endBlockBoundary := affectedBlockRange.End * BlockSize
		endBlockWriteLength := (off + writeLength) - endBlockBoundary
		if endBlockWriteLength == BlockSize {
			d.blockMap.setBlock(affectedBlockRange.End, true)
			wilTransaction.setEndBlock(affectedBlockRange.End)
		} else {
			d.dirtyBlockMap.recordWrite(affectedBlockRange.End, relativeEndWriteStart, int(endBlockWriteLength))
			d.enqueueDirtyWrite(affectedBlockRange.End)
			wilTransaction.recordDirtyWrite(d.blockMapIntentLogger, affectedBlockRange.End, relativeEndWriteStart, int(endBlockWriteLength))
		}
	}

	err := d.writeBacking(p, off)

	if err != nil {
		return err
	}

	// This location, and the fact that this is syncronous, is very important.
	// After the write is performed on our backing file but before we return
	// success to the kernel.
	if d.resumable {
		d.blockMapIntentLogger.flushWriteTransaction(wilTransaction)
	} else {
		d.blockMapIntentLogger.discardWriteTransaction(wilTransaction)
	}

	return nil
}

// DriverDisconnect is called by the BUSE driver in response to disconnect requests from the kernel.
func (d *FileBackedDevice) DriverDisconnect() {
	d.log.Debug("[FileBackedDevice] DISCONNECT")
}

// Flush is called by the BUSE driver in response to flush requests from the kernel.
func (d *FileBackedDevice) Flush() error {
	d.log.Debug("[FileBackedDevice] FLUSH")
	err := d.BackingFile.File.Sync()
	if err != nil {
		return fmt.Errorf("could not sync backing file: %s", err)
	}

	return nil
}

// Trim is called by the BUSE driver in response to trim requests from the kernel.
func (d *FileBackedDevice) Trim(off, length uint64) error {
	d.log.Debugf("[FileBackedDevice] TRIM offset:%d len:%d", off, length)
	return nil
}

func (d *FileBackedDevice) writeBacking(p []byte, off uint64) error {
	backingSize := d.BackingFile.Size
	readSize := uint64(len(p))
	trimmedBuffer := p

	if off+readSize > backingSize {
		trimmedLength := readSize - ((off + readSize) - backingSize)
		trimmedBuffer = p[:trimmedLength]
	}
	writeLen, err := d.BackingFile.File.WriteAt(trimmedBuffer, int64(off))
	d.diskActionTracker.recordAction(BackingWrite, uint64(writeLen))
	if err != nil {
		err = fmt.Errorf("could not write to backing file at offset %d: %s", off, err)
	}

	return err
}

func (d *FileBackedDevice) readBacking(p []byte, off uint64) error {
	readLen, err := d.BackingFile.File.ReadAt(p, int64(off))
	d.diskActionTracker.recordAction(BackingRead, uint64(readLen))
	if err == io.EOF {
		return nil
	}
	if err != nil {
		err = fmt.Errorf("could not read from backing file at offset %d: %s", off, err)
	}

	return err
}

func (d *FileBackedDevice) readSource(p []byte, off uint64) error {
	readLen, err := d.Source.File.ReadAt(p, int64(off))
	d.diskActionTracker.recordAction(SourceRead, uint64(readLen))
	if err == io.EOF {
		return nil
	}
	if err != nil {
		err = fmt.Errorf("could not read from source file at offset %d: %s", off, err)
	}

	return err
}

func (d *FileBackedDevice) enqueueDirtyWrite(block uint64) {
	writeAction := d.writerQueue.MakeWriteAction()
	writeAction.actionType = FixDirtyBlock
	writeAction.startBlock = block
	d.writerQueue.TryEnqueue(writeAction, 0)
}

// IsFullySynced returns true if the backing file is ready to use directly
func (d *FileBackedDevice) IsFullySynced() bool {
	select {
	case <-d.isSyncedCtx.Done():
		return true
	default:
		return false
	}
}

// Calculate the NBD device size by rounding up to the nearest nbdBlockSize
func calculateNbdSize(originalFileSize uint64) uint64 {
	blockSizeRemainder := originalFileSize % nbdBlockSize
	roundAddition := uint64(0)
	if blockSizeRemainder != 0 {
		roundAddition = nbdBlockSize - blockSizeRemainder
		logrus.Warn(fmt.Sprintf("Source file not a multiple of NBD block size (%d), rounding up by %d bytes", nbdBlockSize, roundAddition))
	}

	return originalFileSize + roundAddition
}
