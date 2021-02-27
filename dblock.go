package copyondemand

import (
	"sync"
)

// Connect connects a dblock device to an actual device file
// and starts handling requests. It does not return until it's done serving requests.
func (bd *dblockKernelClient) Connect() error {

	return nil
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
) (KernelClient, error) {
	dblockDriverInternal := &dblockKernelClient{
		size:        size,
		device:      device,
		driver:      driver,
		connected:   false,
		queue:       make(chan *queuedDblockRequest, 100),
		rangeLocker: NewRangeLocker(blockRangePool),
	}

	return dblockDriverInternal, nil
}
