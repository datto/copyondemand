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
	"os"
	"sync"
)

// Rewrote type definitions for #defines and structs to workaround cgo
// as defined in <linux/nbd.h>

const (
	nbdSetSock       = (0xab<<8 | 0)
	nbdSetBlockSize  = (0xab<<8 | 1)
	nbdSetSize       = (0xab<<8 | 2)
	nbdDoIt          = (0xab<<8 | 3)
	nbdClearSock     = (0xab<<8 | 4)
	nbdClearQueue    = (0xab<<8 | 5)
	nbdPrintDebug    = (0xab<<8 | 6)
	nbdSetSizeBlocks = (0xab<<8 | 7)
	nbdDisconnect    = (0xab<<8 | 8)
	nbdSetTimeout    = (0xab<<8 | 9)
	nbdSetFlags      = (0xab<<8 | 10)
)

const (
	nbdCmdRead  = 0
	nbdCmdWrite = 1
	nbdCmdDisc  = 2
	nbdCmdFlush = 3
	nbdCmdTrim  = 4
)

const (
	nbdFlagHasFlags  = (1 << 0)
	nbdFlagReadOnly  = (1 << 1)
	nbdFlagSendFlush = (1 << 2)
	nbdFlagSendTrim  = (1 << 5)
)

const (
	nbdRequestMagic = 0x25609513
	nbdReplyMagic   = 0x67446698
)

const nbdBlockSize = 1024 // (in bytes)

type nbdRequest struct {
	Magic  uint32
	Type   uint32
	Handle uint64
	From   uint64
	Length uint32
}

type nbdReply struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

type buseInterface interface {
	ReadAt(p []byte, off uint64) error
	WriteAt(p []byte, off uint64) error
	DriverDisconnect()
	Flush() error
	Trim(off uint64, length uint64) error
}

type buseDevice struct {
	size           uint64
	device         string
	driver         buseInterface
	deviceFp       *os.File
	socketPair     [2]int
	op             [5]func(driver buseInterface, fp *os.File, fpLock *sync.Mutex, chunk []byte, request *nbdRequest, reply *nbdReply) error
	disconnectChan chan int
	connected      bool
	stateLock      sync.Mutex
	fpLock         sync.Mutex
	bufferPool     *bytePool
	queue          chan *queuedNbdRequest
	rangeLocker    *RangeLocker
}
