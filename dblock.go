package copyondemand

import (
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

type IOType int

const (
	Read IOType = iota
	Write
)

/**
 * TODO:
 * * Add multi-threading similar to the NBD driver (1 op buffer per worker, range locking, etc)
 * * Think through misaligned file sizes (NBD driver rounds up the file size, do we need to do the same here?)
 * * Fix broken unit tests
 * * All other in-lined TODO's in this file will be taken care of pre-merge
 */

// Connect connects a dblock device to an actual device file
// and starts handling requests. It does not return until it's done serving requests (i.e. until the block device is torn down).
func (bd *dblockKernelClient) Connect() error {
	handleID, err := bd.createBlockDevice(bd.device[5:], dblockBlockSize, bd.size/dblockBlockSize, 30)

	if err != nil {
		return err
	}

	bd.logger.Debugf("Got handleID = %d", handleID)
	bd.deviceHandleID = handleID

	for i := 0; i < 2; i++ {
		go func() {
			bd.disconnectWaitGroup.Add(1)
			defer bd.disconnectWaitGroup.Done()

			controlFp, err := os.OpenFile(bd.device+"-ctl", os.O_RDWR, 0600)
			if err != nil {
				logrus.Fatalf("The create device ioctl appeared to succeed but unable to open the \"%s\" ctl device: %s", bd.device+"-ctl", err)
			}
			haltRequested := false
			op := &dblockOperation{}
			op.handleID = bd.deviceHandleID
			op.header.size = 0
			op.header.operation = dblockOperationKernelBlockForRequest
			for !haltRequested {
				controlFileDescriptor := controlFp.Fd()
				switch operation := op.header.operation; operation {
				case dblockOperationKernelBlockForRequest:
					bd.logger.Debugf("Got a request on %d to wait for work", controlFileDescriptor)
					err = bd.blockForOperation(controlFp, op)
					break
				case dblockOperationKernelReadRequest:
					bd.logger.Debugf("Got a read request on %d", controlFileDescriptor)
					err = bd.processIORequest(Read, controlFp, op)
					break
				case dblockOperationKernelWriteRequest:
					bd.logger.Debugf("Got a write request on %d", controlFileDescriptor)
					err = bd.processIORequest(Write, controlFp, op)
					break
				case dblockOperationKernelUserspaceExit:
					bd.logger.Infof("Got a request to stop on %d, stopping", controlFileDescriptor)
					haltRequested = true
				}
				if err != nil {
					logrus.Fatalf("Error processing request on %d '%s' crashing", controlFileDescriptor, err)
				}
			}
		}()
	}

	bd.disconnectWaitGroup.Wait()
	return nil
}

func (bd *dblockKernelClient) processIORequest(ioType IOType, controlFile *os.File, op *dblockOperation) error {
	if op.packet.segmentCount == 0 {
		return fmt.Errorf("IO segment count is zero, crashing")
	}
	if op.packet.segmentCount > maxBioSegmentsPerRequest {
		return fmt.Errorf("Tried to perform IO on %d blocks but the max block count is %d, crashing", op.packet.segmentCount, maxBioSegmentsPerRequest)
	}

	segmentCount := op.packet.segmentCount

	currentSegmentStart := uint32(0)
	for i := uint32(1); i < segmentCount; i++ {
		if op.metadata[i-1].start+op.metadata[i-1].length != op.metadata[i].start {
			err := bd.doIO(ioType, op, currentSegmentStart, i-1)
			if err != nil {
				return err
			}
			currentSegmentStart = i
		}
	}

	err := bd.doIO(ioType, op, currentSegmentStart, segmentCount-1)
	if err != nil {
		return err
	}

	bd.logger.Debugf("Doing ioctl on %d to respond to IO request op %d", controlFile.Fd(), op.operationID)
	for {
		op.errorCode = 0
		op.handleID = bd.deviceHandleID
		if ioType == Read {
			op.header.operation = dblockOperationReadResponse
		} else if ioType == Write {
			op.header.operation = dblockOperationWriteResponse
		}

		ep, err := bd.safeOperationIoctl(controlFile, dblockIoctlDeviceOperation, op)
		if ep != 0 {
			// This syscall is interruptable, if we're interrupted try again
			if ep == syscall.EINTR {
				continue
			}
			return err
		}

		return nil
	}
}

func (bd *dblockKernelClient) doIO(ioType IOType, op *dblockOperation, startSegmentID uint32, endSegmentID uint32) error {
	startSegment := op.metadata[startSegmentID]
	endSegment := op.metadata[endSegmentID]

	offset := startSegment.start
	len := endSegment.start - startSegment.start + endSegment.length
	bufOffset := startSegmentID * dblockBlockSize
	bufStart := bufOffset
	bufEnd := uint64(bufOffset) + len

	if ioType == Read {
		return bd.driver.ReadAt(op.buffer[bufStart:bufEnd], offset)
	} else if ioType == Write {
		return bd.driver.WriteAt(op.buffer[bufStart:bufEnd], offset)
	} else {
		return fmt.Errorf("Developer error, invalid io type")
	}
}

func (bd *dblockKernelClient) createBlockDevice(
	deviceName string,
	blockSize uint32,
	numberOfBlocks uint64,
	deviceTimeoutSeconds uint32,
) (uint32, error) {
	var nameBuffer [maxDiskNameLength]byte
	nameBytes := []byte(deviceName)
	copy(nameBuffer[:], nameBytes)
	createParams := &dblockControlCreateDeviceParams{
		deviceName:           nameBuffer,
		kernelBlockSize:      blockSize,
		numberOfBlocks:       numberOfBlocks,
		deviceTimeoutSeconds: deviceTimeoutSeconds,
		handleID:             0,
		errorCode:            0,
	}

	err := bd.safeCreateIoctl(bd.mainControlFp, createParams)
	if err != nil {
		return 0, err
	}

	/*
		fp, err := os.OpenFile(bd.device+"-ctl", os.O_RDWR, 0600)
		if err != nil {
			return 0, fmt.Errorf("The create device ioctl appeared to succeed but unable to open the \"%s\" ctl device: %s", bd.device+"-ctl", err)
		}

		bd.deviceControlFp = fp
	*/

	return createParams.handleID, nil
}

func (bd *dblockKernelClient) blockForOperation(controlFile *os.File, dblockOp *dblockOperation) error {
	dblockOp.header.operation = dblockOperationNoResponseBlockForRequest
	dblockOp.header.size = 0

	_, err := bd.safeOperationIoctl(controlFile, dblockIoctlDeviceOperation, dblockOp)

	return err
}

// Disconnect disconnects the BuseDevice
func (bd *dblockKernelClient) Disconnect() {
	destroyParams := &dblockControlDestroyDeviceByIDParams{
		handleID:  bd.deviceHandleID,
		errorCode: 0,
		force:     0,
	}

	err := bd.safeDestroyByIdIoctl(bd.mainControlFp, destroyParams)
	if err != nil {
		// The options are:
		// * Potentially be stuck here forever, with the kernel refusing to release us
		// * Crash
		// IMO there's no good option but I chose the second.
		bd.logger.Fatal(err)
	}

	bd.logger.Debugf("Waiting for kernel to signal exit")
	bd.disconnectWaitGroup.Wait()
}

func (bd *dblockKernelClient) sealed() {}

func createDblockKernelClient(
	device string,
	size uint64,
	driver kernelDriverInterface,
	bufferPool *bytePool,
	blockRangePool *sync.Pool,
	logger *logrus.Logger,
) (KernelClient, error) {
	fp, err := os.OpenFile(dblockControlDevicePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("Cannot open \"%s\". Make sure the 'kmod-dblock' kernel module is loaded: %s", dblockControlDevicePath, err)
	}

	opPool := &sync.Pool{
		New: func() interface{} {
			return &dblockOperation{}
		},
	}

	dblockDriverInternal := &dblockKernelClient{
		size:                size,
		device:              device,
		driver:              driver,
		connected:           false,
		queue:               make(chan *queuedDblockRequest, 100),
		rangeLocker:         NewRangeLocker(blockRangePool),
		mainControlFp:       fp,
		logger:              logger,
		opPool:              opPool,
		disconnectWaitGroup: &sync.WaitGroup{},
	}

	return dblockDriverInternal, nil
}
