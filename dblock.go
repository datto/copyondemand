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
	// TODO: Don't hard-code this size
	handleID := bd.createBlockDevice(bd.device[5:], 4096, 1048576, 30)

	bd.logger.Debugf("Got handleID = %d", handleID)

	return nil
}

func (bd *dblockKernelClient) createBlockDevice(
	deviceName string,
	blockSize uint32,
	numberOfBlocks uint64,
	deviceTimeoutSeconds uint32,
) uint32 {
	var nameBuffer [maxDiskNameLength]byte
	nameBytes := []byte(deviceName)
	copy(nameBuffer[:], nameBytes)
	createParams := &dblockControlCreateDeviceParams{
		deviceName:           nameBuffer, // TODO
		kernelBlockSize:      blockSize,
		numberOfBlocks:       numberOfBlocks,
		deviceTimeoutSeconds: deviceTimeoutSeconds,
		handleID:             0,
		errorCode:            0,
	}

	// *** DANGER *** DANGER *** DANGER ***
	// Do not touch me, this line is a compiler hint that guarantees the createParams struct
	// stays in place while this syscall is being serviced. Read https://golang.org/pkg/unsafe/
	// section (4) for additional details.
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, bd.controlFp.Fd(), uintptr(dblockControlCreateDevice), uintptr(unsafe.Pointer(createParams)))
	// *** DANGER *** DANGER *** DANGER ***
	if ep != 0 {
		bd.logger.Fatalf("ioctl(%d, %d, %d) failed: %s", bd.controlFp.Fd(), uintptr(unsafe.Pointer(createParams)), 0, syscall.Errno(ep))
	}

	bd.logger.Debugf("Syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	return createParams.handleID
}

// Disconnect disconnects the BuseDevice
func (bd *dblockKernelClient) Disconnect() {

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

	dblockDriverInternal := &dblockKernelClient{
		size:        size,
		device:      device,
		driver:      driver,
		connected:   false,
		queue:       make(chan *queuedDblockRequest, 100),
		rangeLocker: NewRangeLocker(blockRangePool),
		controlFp:   fp,
		logger:      logger,
	}

	return dblockDriverInternal, nil
}
