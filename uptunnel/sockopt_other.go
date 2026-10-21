//go:build !linux

package uptunnel

import (
	"net"
	"syscall"
	"time"
)

// dialControl is a no-op off Linux; keepalive still bounds a silent peer.
func dialControl(time.Duration) func(network, address string, c syscall.RawConn) error {
	return nil
}

// unsentBytes has no portable reading off Linux.
func unsentBytes(net.Conn) (int, bool) { return 0, false }
