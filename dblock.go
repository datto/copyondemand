package copyondemand

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/sirupsen/logrus"
)

// Connect connects a dblock device to an actual device file
// and starts handling requests. It does not return until it's done serving requests.
func (bd *dblockKernelClient) Connect() error {
	bd.disconnectWaitGroup.Add(1)
	defer bd.disconnectWaitGroup.Done()
	// TODO: Don't hard-code this size
	handleID, err := bd.createBlockDevice(bd.device[5:], 4096, 1048576, 30)

	if err != nil {
		return err
	}

	bd.logger.Debugf("Got handleID = %d", handleID)
	bd.deviceHandleID = handleID

	haltRequested := false
	op := &dblockOperation{}
	op.handleID = bd.deviceHandleID
	op.header.size = 0
	op.header.operation = dblockOperationKernelBlockForRequest
	for !haltRequested {
		switch operation := op.header.operation; operation {
		case dblockOperationKernelBlockForRequest:
			// no-op, we'll do this operation at the beginning of the next loop
			bd.logger.Debugf("Got a request to wait for work")
			err = bd.blockForOperation(op)
			break
		case dblockOperationKernelReadRequest:
			// TODO service read request
			bd.logger.Debugf("Got a read request")
			err = bd.processReadRequest(op)
			break
		case dblockOperationKernelWriteRequest:
			// TODO service write request
			bd.logger.Debugf("Got a write request")
			break
		case dblockOperationKernelUserspaceExit:
			bd.logger.Info("Got a request to stop, stopping")
			haltRequested = true
		}
		if err != nil {
			return fmt.Errorf("Error in get request syscall '%s' crashing", err)
		}
	}

	return nil
}

func (bd *dblockKernelClient) processReadRequest(op *dblockOperation) error {
	if op.packet.segmentCount == 0 {
		return fmt.Errorf("Read segment count is zero, crashing")
	}
	if op.packet.segmentCount > maxBioSegmentsPerRequest {
		return fmt.Errorf("Tried to read %d blocks but the max block count is %d, crashing", op.packet.segmentCount, maxBioSegmentsPerRequest)
	}

	segmentCount := op.packet.segmentCount

	currentSegmentStart := uint32(0)
	for i := uint32(1); i < segmentCount; i++ {
		if op.metadata[i-1].start+op.metadata[i-1].length != op.metadata[i].start {
			err := bd.doRead(op, currentSegmentStart, i-1)
			if err != nil {
				return err
			}
			currentSegmentStart = i
		}
	}

	err := bd.doRead(op, currentSegmentStart, segmentCount-1)
	if err != nil {
		return err
	}

	bd.logger.Debugf("Doing ioctl to respond to read request op %d", op.operationID)
	for {
		op.errorCode = 0
		op.handleID = bd.deviceHandleID
		op.header.operation = dblockOperationReadResponse
		// <*** DANGER *** DANGER *** DANGER ***>
		// Do not touch me, this line is a compiler hint that guarantees the createParams
		// struct stays in place in memory while this syscall is being serviced. Read
		// https://golang.org/pkg/unsafe/ section (4) for additional details.
		r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(op)))
		// </*** DANGER *** DANGER *** DANGER ***>
		if ep != 0 {
			// If we're interrupted, try again for some reason? Kernel developers send help.
			if ep == syscall.EINTR {
				continue
			}
			return fmt.Errorf("ioctl(%d, %d, %d) read response failed: %s (%d, %d, %d)", bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(op)), syscall.Errno(ep), r1, r2, ep)
		}
		bd.logger.Debugf("Read respond syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)
		return nil
	}
}

func (bd *dblockKernelClient) doRead(op *dblockOperation, startSegmentID uint32, endSegmentID uint32) error {
	startSegment := op.metadata[startSegmentID]
	endSegment := op.metadata[endSegmentID]

	offset := startSegment.start
	len := endSegment.start - startSegment.start + endSegment.length
	bufOffset := startSegmentID * dblockBlockSize

	bd.logger.Debugf("offset = %d, len = %d, bufOffset = %d", offset, len, bufOffset)

	// We are from time to time passed overflowing requests, we are responsible
	// for shielding these overflows from the driver.
	/*if offset >= bd.size {
		op.header.size = 0
		op.packet.totalSegmentBytes = 0
		op.packet.segmentCount = 0
		return nil
	}
	if offset+len >= bd.size {
		len = bd.size - offset
	}

	bd.logger.Debugf("fixedLen = %d", len)*/

	return bd.driver.ReadAt(op.buffer[bufOffset:(uint64(bufOffset)+len)], offset)
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

	// <*** DANGER *** DANGER *** DANGER ***>
	// Do not touch me, this line is a compiler hint that guarantees the createParams
	// struct stays in place in memory while this syscall is being serviced. Read
	// https://golang.org/pkg/unsafe/ section (4) for additional details.
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.mainControlFp.Fd(), uintptr(dblockControlCreateDevice), uintptr(unsafe.Pointer(createParams)))
	// </*** DANGER *** DANGER *** DANGER ***>
	if ep != 0 {
		return 0, fmt.Errorf("ioctl(%d, %d, %d) failed: %s", bd.mainControlFp.Fd(), uintptr(dblockControlCreateDevice), uintptr(unsafe.Pointer(createParams)), syscall.Errno(ep))
	}

	bd.logger.Debugf("Create syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	fp, err := os.OpenFile(bd.device+"-ctl", os.O_RDWR, 0600)
	if err != nil {
		return 0, fmt.Errorf("Create device appeared to succeed but unable to open the \"%s\" ctl device: %s", bd.device+"-ctl", err)
	}

	bd.deviceControlFp = fp

	return createParams.handleID, nil
}

func (bd *dblockKernelClient) blockForOperation(dblockOp *dblockOperation) error {
	dblockOp.header.operation = dblockOperationNoResponseBlockForRequest
	dblockOp.header.size = 0

	// <*** DANGER *** DANGER *** DANGER ***>
	// Do not touch me, this line is a compiler hint that guarantees the dblockOp
	// struct stays in place in memory while this syscall is being serviced. Read
	// https://golang.org/pkg/unsafe/ section (4) for additional details.
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(dblockOp)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		return fmt.Errorf("ioctl(%d, %d, %d) failed: %s (%d, %d, %d)", bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(dblockOp)), syscall.Errno(ep), r1, r2, ep)
	}

	bd.logger.Debugf("Request syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	return nil
}

// Disconnect disconnects the BuseDevice
func (bd *dblockKernelClient) Disconnect() {
	destroyParams := &dblockControlDestroyDeviceByIDParams{
		handleID:  bd.deviceHandleID,
		errorCode: 0,
		force:     0,
	}

	// <*** DANGER *** DANGER *** DANGER ***>
	// Do not touch me, this line is a compiler hint that guarantees the dblockOp
	// struct stays in place in memory while this syscall is being serviced. Read
	// https://golang.org/pkg/unsafe/ section (4) for additional details.
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.mainControlFp.Fd(), uintptr(dblockControlDestroyDeviceByID), uintptr(unsafe.Pointer(destroyParams)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		bd.logger.Fatalf("ioctl(%d, %d, %d) to destroy device failed: %s", bd.mainControlFp.Fd(), uintptr(dblockControlDestroyDeviceByID), uintptr(unsafe.Pointer(destroyParams)), syscall.Errno(ep))
	}

	bd.logger.Debugf("Request syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

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
