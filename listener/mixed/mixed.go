package mixed

import (
	"net"
	"time"

	"github.com/Dreamacro/clash/common/cache"
	N "github.com/Dreamacro/clash/common/net"
	C "github.com/Dreamacro/clash/constant"
	"github.com/Dreamacro/clash/listener/http"
	"github.com/Dreamacro/clash/listener/socks"
	"github.com/Dreamacro/clash/transport/socks4"
	"github.com/Dreamacro/clash/transport/socks5"
)

type Listener struct {
	listener net.Listener
	addr     string
	cache    *cache.Cache
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
func (l *Listener) SetUDPUserMap(userMap map[string]*socks.UDPListener) {
	l.udpUserMap = make(map[string]string, len(userMap))
	for user, lis := range userMap {
		l.udpUserMap[user] = lis.RawAddress()
	}
}

func (l *Listener) handleConn(conn net.Conn, in chan<- C.ConnContext) {
	bufConn := N.NewBufferedConn(conn)
	head, err := bufConn.Peek(1)
	if err != nil {
		return
	}

	switch head[0] {
	case socks4.Version:
		socks.HandleSocks4(bufConn, in)
	case socks5.Version:
		socks.HandleSocks5(bufConn, in, l.udpUserMap)
	default:
		http.HandleConn(bufConn, in, l.cache)
	}
}

func New(addr string, in chan<- C.ConnContext) (*Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	ml := &Listener{
		listener: l,
		addr:     addr,
		cache:    cache.New(30 * time.Second),
	}
	go func() {
		for {
			c, err := ml.listener.Accept()
			if err != nil {
				if ml.closed {
					break
				}
				continue
			}
			go ml.handleConn(c, in)
		}
	}()

	return ml, nil
}
