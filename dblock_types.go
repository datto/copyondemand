package copyondemand

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

const dblockBlockSize = 4096

const maxDiskNameLength = 32
const maxBioSegmentsPerRequest = 256
const maxBioSegmentsBufferSizePerRequest = 256 * dblockBlockSize

const dblockControlCreateDevice = uint64(21)
const dblockControlDestroyDeviceByID = uint64(22)
const dblockControlDestroyDeviceByName = uint64(23)
const dblockControlDestroyAllDevices = uint64(25)

const dblockOperationNoResponseBlockForRequest = 30
const dblockOperationReadResponse = 31
const dblockOperationWriteResponse = 37
const dblockOperationStatus = 39
const dblockOperationKernelBlockForRequest = 40
const dblockOperationKernelReadRequest = 41
const dblockOperationKernelWriteRequest = 42

const dblockOperationKernelUserspaceExit = 8086

const dblockIoctlDeviceOperation = uint64(58)

const dblockControlDevicePath = "/dev/dblockctl"

// len("-ctl") + the string needs to be null terminated
const dblockCtlSuffixLength = 5

type dblockControlCreateDeviceParams struct {
	deviceName            [maxDiskNameLength]byte
	kernelBlockSize       uint32
	numberOfBlocks        uint64
	deviceTimeoutSeconds  uint32
	maxSegmentsPerRequest uint32
	handleID              uint32
	errorCode             int32
}

type dblockControlDestroyDeviceByIDParams struct {
	handleID  uint32
	errorCode int32
	force     int32

	// This is a 256 byte union with the rest of the control types
	// Pad to 256 bytes
	padding [160]byte
}

type dblockControlDestroyByID struct {
	handleID  uint32
	errorCode int32
	force     uint32
}

type dblockHeader struct {
	operation    uint32
	size         uint64
	signalNumber uint32
}

type dblockSegmentMetadata struct {
	start  uint64
	length uint64
}

type dblockPacket struct {
	totalSegmentBytes uint64
	segmentCount      uint32
}

type dblockOperation struct {
	handleID    uint32
	header      dblockHeader
	packet      dblockPacket
	metadata    [maxBioSegmentsPerRequest]dblockSegmentMetadata
	errorCode   uint32
	operationID uint32
	buffer      [maxBioSegmentsPerRequest * dblockBlockSize]byte
}

type queuedDblockRequest struct {
	requestType uint32
	buffer      []byte
}

// dblockKernelClient is the kernel client that can connect to kmod-dblock
type dblockKernelClient struct {
	size                uint64
	device              string
	driver              kernelDriverInterface
	disconnectChan      chan int
	connected           bool
	logger              *logrus.Logger
	bufferPool          *bytePool
	queue               chan *queuedDblockRequest
	rangeLocker         *RangeLocker
	mainControlFp       *os.File
	deviceControlFp     *os.File
	deviceHandleID      uint32
	opPool              *sync.Pool
	disconnectWaitGroup *sync.WaitGroup
}
