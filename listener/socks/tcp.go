package socks

import (
	"io"
	"net"

	"github.com/Dreamacro/clash/adapter/inbound"
	N "github.com/Dreamacro/clash/common/net"
	C "github.com/Dreamacro/clash/constant"
	authStore "github.com/Dreamacro/clash/listener/auth"
	"github.com/Dreamacro/clash/transport/socks4"
	"github.com/Dreamacro/clash/transport/socks5"
)

type Listener struct {
	listener net.Listener
	addr     string
	closed   bool

	udpUserMap map[string]string
}

// RawAddress implements C.Listener
func (l *Listener) RawAddress() string {
	return l.addr
}

// Address implements C.Listener
func (l *Listener) Address() string {
	return l.listener.Addr().String()
}

// Close implements C.Listener
func (l *Listener) Close() error {
	l.closed = true
	return l.listener.Close()
}

// SetUDPUserMap inject user to mapping for socks5 udp associate
func (l *Listener) SetUDPUserMap(userMap map[string]*UDPListener) {
	l.udpUserMap = make(map[string]string, len(userMap))
	for user, lis := range userMap {
		l.udpUserMap[user] = lis.addr
	}
}

func (l *Listener) handleSocks(conn net.Conn, in chan<- C.ConnContext) {
	bufConn := N.NewBufferedConn(conn)
	head, err := bufConn.Peek(1)
	if err != nil {
		conn.Close()
		return
	}

	switch head[0] {
	case socks4.Version:
		HandleSocks4(bufConn, in)
	case socks5.Version:
		HandleSocks5(bufConn, in, l.udpUserMap)
	default:
		conn.Close()
	}
}

func New(addr string, in chan<- C.ConnContext) (*Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	sl := &Listener{
		listener: l,
		addr:     addr,
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				if sl.closed {
					break
				}
				continue
			}
			go sl.handleSocks(c, in)
		}
	}()

	return sl, nil
}

func HandleSocks4(conn net.Conn, in chan<- C.ConnContext) {
	addr, _, user, err := socks4.ServerHandshake(conn, authStore.Authenticator())
	if err != nil {
		conn.Close()
		return
	}
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}
	in <- inbound.NewSocket(socks5.ParseAddr(addr), conn, C.SOCKS4, user)
}

func HandleSocks5(conn net.Conn, in chan<- C.ConnContext, userMapping map[string]string) {
	target, command, user, err := socks5.ServerHandshake(conn, authStore.Authenticator(), userMapping)
	if err != nil {
		conn.Close()
		return
	}
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}
	if command == socks5.CmdUDPAssociate {
		defer conn.Close()
		io.Copy(io.Discard, conn)
		return
	}
	in <- inbound.NewSocket(target, conn, C.SOCKS5, user)
}
