package copyondemand

const dblockBlockSize = 4096

const maxDiskNameLength = 32
const maxBioSegmentsPerRequest = 256
const maxBioSegmentsBufferSizePerRequest = 256 * dblockBlockSize

const dblockOperationNoResponseBlockForRequest = 30
const dblockOperationReadResponse = 31
const dblockOperationStatus = 39
const dblockOperationKernelBlockForRequest = 40
const dblockOperationKernelWriteRequest = 41
const dblockOperationKernelUserspaceExit = 42

type dblockControlCreateDeviceParams struct {
	deviceName            [maxDiskNameLength]byte
	kernelBlockSize       uint32
	numberOfBlocks        uint64
	deviceTimeoutSeconds  uint32
	maxSegmentsPerRequest uint32
	handleID              uint32
	errorCode             int32
}

type dblockControlDestroyByID struct {
	handleID  uint32
	errorCode int32
	force     uint32
}

type dblockBlock struct {
	block [dblockBlockSize]byte
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
	handleID uint32
	header   dblockHeader
	packet   dblockPacket
	metadata [maxBioSegmentsPerRequest]dblockSegmentMetadata
	buffer   [maxBioSegmentsPerRequest]dblockBlock
}

type queuedDblockRequest struct {
	requestType uint32
	buffer      []byte
}

// dblockKernelClient is the kernel client that can connect to kmod-dblock
type dblockKernelClient struct {
	size           uint64
	device         string
	driver         kernelDriverInterface
	disconnectChan chan int
	connected      bool
	bufferPool     *bytePool
	queue          chan *queuedDblockRequest
	rangeLocker    *RangeLocker
}
