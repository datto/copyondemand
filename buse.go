/*
MIT License

Copyright (c) 2017 Sam Alba

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package copyondemand

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/sirupsen/logrus"
)

func ioctl(fd, op, arg uintptr) {
	_, _, ep := syscall.Syscall(syscall.SYS_IOCTL, fd, op, arg)
	if ep != 0 {
		logrus.Fatalf("ioctl(%d, %d, %d) failed: %s", fd, op, arg, syscall.Errno(ep))
	}
}

func opDeviceRead(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error {
	if err := driver.ReadAt(chunk, request.From); err != nil {
		logrus.Errorf("buseDriver.ReadAt returned an error: %s", err)
		// Reply with an EPERM
		reply.Error = 1
	}
	fpLock.Lock()
	buf := writeNbdReply(reply)
	if _, err := fp.Write(buf); err != nil {
		logrus.Errorf("Write error, when sending reply header: %s", err)
	}
	if _, err := fp.Write(chunk); err != nil {
		logrus.Errorf("Write error, when sending data chunk: %s", err)
	}
	fpLock.Unlock()
	return nil
}

func opDeviceWrite(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error {
	if err := driver.WriteAt(chunk, request.From); err != nil {
		logrus.Errorf("buseDriver.WriteAt returned an error: %s", err)
		reply.Error = 1
	}
	fpLock.Lock()
	buf := writeNbdReply(reply)
	if _, err := fp.Write(buf); err != nil {
		logrus.Errorf("Write error, when sending reply header: %s", err)
	}
	fpLock.Unlock()
	return nil
}

func opDeviceDisconnect(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error {
	logrus.Debug("Calling buseDriver.Disconnect()")
	driver.DriverDisconnect()
	return fmt.Errorf("Received a disconnect")
}

func opDeviceFlush(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error {
	if err := driver.Flush(); err != nil {
		logrus.Errorf("buseDriver.Flush returned an error: %s", err)
		reply.Error = 1
	}
	fpLock.Lock()
	buf := writeNbdReply(reply)
	if _, err := fp.Write(buf); err != nil {
		logrus.Errorf("Write error, when sending reply header: %s", err)
	}
	fpLock.Unlock()
	return nil
}

func opDeviceTrim(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error {
	if err := driver.Trim(request.From, uint64(request.Length)); err != nil {
		logrus.Errorf("buseDriver.Flush returned an error: %s", err)
		reply.Error = 1
	}
	fpLock.Lock()
	buf := writeNbdReply(reply)
	if _, err := fp.Write(buf); err != nil {
		logrus.Errorf("Write error, when sending reply header: %s", err)
	}
	fpLock.Unlock()
	return nil
}

func (bd *buseDevice) startNBDClient() {
	ioctl(bd.deviceFp.Fd(), nbdSetSock, uintptr(bd.socketPair[1]))
	// The call below may fail on some systems (if flags unset), could be ignored
	ioctl(bd.deviceFp.Fd(), nbdSetFlags, nbdFlagSendTrim)
	// The following call will block until the client disconnects
	logrus.Info("Starting NBD client...")
	go ioctl(bd.deviceFp.Fd(), nbdDoIt, 0)
	// Block on the disconnect channel
	<-bd.disconnectChan
}

// Disconnect disconnects the BuseDevice
func (bd *buseDevice) disconnect() {
	bd.stateLock.Lock()
	connected := bd.connected
	if !connected {
		bd.stateLock.Unlock()
		// Disconnecting twice causes the syscalls to hang
		return
	}
	bd.connected = false
	bd.stateLock.Unlock()

	bd.disconnectChan <- 1
	// Ok to fail, ignore errors
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), nbdClearQueue, 0)
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), nbdDisconnect, 0)

	// Cleanup fd
	syscall.Close(bd.socketPair[0])
	syscall.Close(bd.socketPair[1])

	// Clear the socket after both ends are closed
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), nbdClearSock, 0)

	// Finally, release the device fp
	// TODO: This has a data race as the NBD_DO_IT data race will block until we disconnect.
	//       Not sure how big of an issue it is, but it is there.
	bd.deviceFp.Close()
	logrus.Info("NBD client disconnected")
}

func readNbdRequest(buf []byte, request *nbdRequest) {
	request.Magic = binary.BigEndian.Uint32(buf)
	request.Type = binary.BigEndian.Uint32(buf[4:8])
	request.Handle = binary.BigEndian.Uint64(buf[8:16])
	request.From = binary.BigEndian.Uint64(buf[16:24])
	request.Length = binary.BigEndian.Uint32(buf[24:28])
}

func writeNbdReply(reply *nbdReply) []byte {
	buf := make([]byte, unsafe.Sizeof(*reply))
	binary.BigEndian.PutUint32(buf[0:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(buf[4:8], reply.Error)
	binary.BigEndian.PutUint64(buf[8:16], reply.Handle)
	// NOTE: a struct in go has 4 extra bytes, so we skip the last
	return buf[0:16]
}

// Connect connects a BuseDevice to an actual device file
// and starts handling requests. It does not return until it's done serving requests.
func (bd *buseDevice) connect() error {
	go bd.startNBDClient()
	defer bd.disconnect()
	for i := 0; i < requestWorkerCount; i++ {
		go bd.requestWorker()
	}
	//opens the device file at least once, to make sure the partition table is updated
	tmp, err := os.Open(bd.device)
	if err != nil {
		return fmt.Errorf("Cannot reach the device %s: %s", bd.device, err)
	}
	tmp.Close()
	// Start handling requests
	fp := os.NewFile(uintptr(bd.socketPair[0]), "unix")
	for true {
		request := &nbdRequest{}
		reply := &nbdReply{Magic: nbdReplyMagic}
		// NOTE: a struct in go has 4 extra bytes...
		buf := make([]byte, unsafe.Sizeof(*request))
		if _, err := fp.Read(buf[0:28]); err != nil {
			return fmt.Errorf("NBD client stopped: %s", err)
		}
		readNbdRequest(buf, request)
		//fmt.Printf("DEBUG %#v\n", request)
		if request.Magic != nbdRequestMagic {
			return fmt.Errorf("Fatal error: received packet with wrong Magic number")
		}
		reply.Handle = request.Handle
		chunk := bd.bufferPool.get(uint64(request.Length))
		reply.Error = 0
		// Dispatches READ, WRITE, DISC, FLUSH, TRIM to the corresponding implementation
		if request.Type < nbdCmdRead || request.Type > nbdCmdTrim {
			logrus.Infof("Received unknown request: %d", request.Type)
			continue
		}

		if request.Type == nbdCmdWrite {
			if _, err := io.ReadFull(fp, chunk); err != nil {
				return fmt.Errorf("Fatal error, cannot read request packet: %s", err)
			}
		}

		bd.queue <- &queuedNbdRequest{request.Type, fp, chunk, request, reply}
	}
	return nil
}

type queuedNbdRequest struct {
	requestType uint32
	fp          *os.File
	chunk       []byte
	request     *nbdRequest
	reply       *nbdReply
}

func (bd *buseDevice) requestWorker() {
	for true {
		req := <-bd.queue
		var affectedBlocks *BlockRange
		affectedBlocks = nil
		if req.requestType == nbdCmdWrite || req.requestType == nbdCmdRead {
			off := req.request.From
			affectedBlocks = &BlockRange{}
			getAffectedBlockRange(uint64(len(req.chunk)), uint64(off), affectedBlocks)
			bd.rangeLocker.LockRange(affectedBlocks.Start, affectedBlocks.End)
		}
		bd.op[req.requestType](bd.driver, req.fp, &bd.fpLock, req.chunk, req.request, req.reply)
		bd.bufferPool.put(req.chunk)
		if affectedBlocks != nil {
			bd.rangeLocker.UnlockRange(affectedBlocks.Start, affectedBlocks.End)
		}
	}
}

func createNbdDevice(
	device string,
	size uint64,
	buseDriver buseInterface,
	bufferPool *bytePool,
	blockRangePool *sync.Pool,
) (*buseDevice, error) {
	buseDeviceInternal := &buseDevice{
		size:        size,
		device:      device,
		driver:      buseDriver,
		connected:   true,
		queue:       make(chan *queuedNbdRequest, 100),
		rangeLocker: NewRangeLocker(blockRangePool),
	}
	sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("Call to socketpair failed: %s", err)
	}
	fp, err := os.OpenFile(device, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("Cannot open \"%s\". Make sure the `nbd' kernel module is loaded: %s", device, err)
	}
	buseDeviceInternal.deviceFp = fp
	ioctl(buseDeviceInternal.deviceFp.Fd(), nbdSetSize, uintptr(size))
	ioctl(buseDeviceInternal.deviceFp.Fd(), nbdClearQueue, 0)
	ioctl(buseDeviceInternal.deviceFp.Fd(), nbdClearSock, 0)
	buseDeviceInternal.socketPair = sockPair
	buseDeviceInternal.op[nbdCmdRead] = opDeviceRead
	buseDeviceInternal.op[nbdCmdWrite] = opDeviceWrite
	buseDeviceInternal.op[nbdCmdDisc] = opDeviceDisconnect
	buseDeviceInternal.op[nbdCmdFlush] = opDeviceFlush
	buseDeviceInternal.op[nbdCmdTrim] = opDeviceTrim
	buseDeviceInternal.disconnectChan = make(chan int, 5)
	buseDeviceInternal.bufferPool = bufferPool
	return buseDeviceInternal, nil
}
