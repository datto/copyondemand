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

	for !haltRequested {
		op := bd.opPool.Get().(*dblockOperation)
		err := bd.fillRequest(op)
		if err != nil {
			return fmt.Errorf("Error in get request syscall '%s' crashing", err)
		}

		switch operation := op.header.operation; operation {
		case dblockOperationKernelBlockForRequest:
			// no-op, we'll do this operation at the beginning of the next loop
			bd.logger.Debugf("Got a request to block again")
			break
		case dblockOperationKernelReadRequest:
			// TODO service read request
			bd.logger.Debugf("Got a read request")
			break
		case dblockOperationKernelWriteRequest:
			// TODO service write request
			bd.logger.Debugf("Got a write request")
			break
		case dblockOperationKernelUserspaceExit:
			bd.logger.Info("Got a request to stop, stopping")
			haltRequested = true
		}
		bd.opPool.Put(op)
	}

	return nil
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

func (bd *dblockKernelClient) fillRequest(dblockOp *dblockOperation) error {
	dblockOp.handleID = bd.deviceHandleID
	dblockOp.header.operation = dblockOperationNoResponseBlockForRequest
	dblockOp.header.size = 0

	// <*** DANGER *** DANGER *** DANGER ***>
	// Do not touch me, this line is a compiler hint that guarantees the dblockOp
	// struct stays in place in memory while this syscall is being serviced. Read
	// https://golang.org/pkg/unsafe/ section (4) for additional details.
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(dblockOp)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		return fmt.Errorf("ioctl(%d, %d, %d) failed: %s", bd.deviceControlFp.Fd(), uintptr(dblockIoctlDeviceOperation), uintptr(unsafe.Pointer(dblockOp)), syscall.Errno(ep))
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
