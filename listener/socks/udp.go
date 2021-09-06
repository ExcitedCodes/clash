package socks

import (
	"net"

	"github.com/Dreamacro/clash/adapter/inbound"
	"github.com/Dreamacro/clash/common/pool"
	"github.com/Dreamacro/clash/common/sockopt"
	C "github.com/Dreamacro/clash/constant"
	"github.com/Dreamacro/clash/log"
	"github.com/Dreamacro/clash/transport/socks5"
)

type UDPListener struct {
	packetConn net.PacketConn
	addr       string
	closed     bool
}

// RawAddress implements C.Listener
func (l *UDPListener) RawAddress() string {
	return l.addr
}

// Address implements C.Listener
func (l *UDPListener) Address() string {
	return l.packetConn.LocalAddr().String()
}

// Close implements C.Listener
func (l *UDPListener) Close() error {
	l.closed = true
	return l.packetConn.Close()
}

func NewUDP(addr string, in chan<- *inbound.PacketAdapter) (*UDPListener, error) {
	return newUDP(addr, in, "")
}

func NewUDPWithUser(addr string, in chan<- *inbound.PacketAdapter, users []string) (map[string]*UDPListener, error) {
	userMap := make(map[string]*UDPListener, len(users))
	ip, _, _ := net.SplitHostPort(addr)
	newAddr := net.JoinHostPort(ip, "0")

	for _, user := range users {
		lis, err := newUDP(newAddr, in, user)
		if err != nil {
			for _, lis := range userMap {
				lis.Close()
			}
			return nil, err
		}
		userMap[user] = lis
	}
	return userMap, nil
}

func newUDP(addr string, in chan<- *inbound.PacketAdapter, user string) (*UDPListener, error) {
	l, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}

	if err := sockopt.UDPReuseaddr(l.(*net.UDPConn)); err != nil {
		log.Warnln("Failed to Reuse UDP Address: %s", err)
	}

	sl := &UDPListener{
		packetConn: l,
		addr:       l.LocalAddr().String(),
	}
	go func() {
		for {
			buf := pool.Get(pool.RelayBufferSize)
			n, remoteAddr, err := l.ReadFrom(buf)
			if err != nil {
				pool.Put(buf)
				if sl.closed {
					break
				}
				continue
			}
			handleSocksUDP(l, in, buf[:n], remoteAddr, user)
		}
	}()

	return sl, nil
}

func handleSocksUDP(pc net.PacketConn, in chan<- *inbound.PacketAdapter, buf []byte, addr net.Addr, user string) {
	target, payload, err := socks5.DecodeUDPPacket(buf)
	if err != nil {
		// Unresolved UDP packet, return buffer to the pool
		pool.Put(buf)
		return
	}
	packet := &packet{
		pc:      pc,
		rAddr:   addr,
		payload: payload,
		bufRef:  buf,
	}
	select {
	case in <- inbound.NewPacket(target, packet, C.SOCKS5, user):
	default:
	}
}
