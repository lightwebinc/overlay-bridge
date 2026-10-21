//go:build linux

package uptunnel

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// dialControl sets TCP_USER_TIMEOUT, so bytes the peer never acknowledges fail
// the connection after ut instead of after the kernel's retransmission budget
// (about fifteen minutes by default).
func dialControl(ut time.Duration) func(network, address string, c syscall.RawConn) error {
	ms := int(ut / time.Millisecond)
	return func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, ms)
		}); err != nil {
			return err
		}
		return serr
	}
}

// unsentBytes reports the bytes in the socket's send queue that the peer has
// not acknowledged (SIOCOUTQ: unsent plus unacknowledged).
func unsentBytes(conn net.Conn) (int, bool) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0, false
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var n int
	var ierr error
	if err := rc.Control(func(fd uintptr) {
		n, ierr = unix.IoctlGetInt(int(fd), unix.SIOCOUTQ)
	}); err != nil || ierr != nil {
		return 0, false
	}
	return n, true
}
