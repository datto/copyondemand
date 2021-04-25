package copyondemand

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// *** DANGER *** DANGER *** DANGER ***
// You're entering the danger zone. Please use extreme care while modifying
// this file. Constructs like syscall.Syscall(syscall.SYS_IOCTL, [descriptor], [opcode], uintptr(unsafe.Pointer([buffer])))
// are crafted such that the go compiler recognizes these calls as a hint to
// leave `buffer` pinned in memory during the course of the syscall.
// Read https://golang.org/pkg/unsafe/ section (4) for additional details.

// Each struct type has its own function. Accepting interface{}
// doesn't work without casting back to a concrete type, which
// defeats the whole purpose. This code is the hottest of hot-path
// anyway so we wouldn't want to do type mangling here.

func (bd *dblockKernelClient) safeOperationIoctl(controlFile *os.File, operation uint64, op *dblockOperation) (syscall.Errno, error) {
	// <*** DANGER *** DANGER *** DANGER ***>
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, controlFile.Fd(), uintptr(operation), uintptr(unsafe.Pointer(op)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		return syscall.Errno(ep), fmt.Errorf("ioctl(%d, %d, %d) failed: %s (%d, %d, %d)", controlFile.Fd(), uintptr(operation), uintptr(unsafe.Pointer(op)), syscall.Errno(ep), r1, r2, ep)
	}

	bd.logger.Debugf("Request syscall return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	return 0, nil
}

func (bd *dblockKernelClient) safeCreateIoctl(controlFile *os.File, createParams *dblockControlCreateDeviceParams) error {
	// <*** DANGER *** DANGER *** DANGER ***>
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, controlFile.Fd(), uintptr(dblockControlCreateDevice), uintptr(unsafe.Pointer(createParams)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		return fmt.Errorf("ioctl(%d, %d, %d) failed: %s (%d, %d, %d)", controlFile.Fd(), uintptr(dblockControlCreateDevice), uintptr(unsafe.Pointer(createParams)), syscall.Errno(ep), r1, r2, ep)
	}

	bd.logger.Debugf("Device create syscall succeeded, return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	return nil
}

func (bd *dblockKernelClient) safeDestroyByIdIoctl(controlFile *os.File, destroyParams *dblockControlDestroyDeviceByIDParams) error {
	// <*** DANGER *** DANGER *** DANGER ***>
	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, controlFile.Fd(), uintptr(dblockControlDestroyDeviceByID), uintptr(unsafe.Pointer(destroyParams)))
	// </*** DANGER *** DANGER *** DANGER ***>

	if ep != 0 {
		return fmt.Errorf("ioctl(%d, %d, %d) failed: %s (%d, %d, %d)", controlFile.Fd(), uintptr(dblockControlDestroyDeviceByID), uintptr(unsafe.Pointer(destroyParams)), syscall.Errno(ep), r1, r2, ep)
	}

	bd.logger.Debugf("Device destroy syscall succeeded, return (r1, r2, err): (%d, %d, %d)", r1, r2, ep)

	return nil
}
