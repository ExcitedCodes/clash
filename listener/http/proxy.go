package http

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Dreamacro/clash/adapter/inbound"
	"github.com/Dreamacro/clash/common/cache"
	N "github.com/Dreamacro/clash/common/net"
	C "github.com/Dreamacro/clash/constant"
	authStore "github.com/Dreamacro/clash/listener/auth"
	"github.com/Dreamacro/clash/log"
)

type authCache struct {
	success bool
	user    string
}

func HandleConn(c net.Conn, in chan<- C.ConnContext, cache *cache.Cache) {
	client := newClient(c.RemoteAddr(), in)
	defer client.client.CloseIdleConnections()

	conn := N.NewBufferedConn(c)

	keepAlive := true
	trusted := cache == nil // disable authenticate if cache is nil

	for keepAlive {
		request, err := ReadRequest(conn.Reader())
		if err != nil {
			break
		}

		request.RemoteAddr = conn.RemoteAddr().String()

		keepAlive = strings.TrimSpace(strings.ToLower(request.Header.Get("Proxy-Connection"))) == "keep-alive"

		var resp *http.Response
		var user string

		if !trusted {
			resp, user = authenticate(request, cache)

			trusted = resp == nil
		}

		if trusted {
			if request.Method == http.MethodConnect {
				resp = responseWith(200)
				resp.Status = "Connection established"
				resp.ContentLength = -1

				if resp.Write(conn) != nil {
					break // close connection
				}

				in <- inbound.NewHTTPS(request, conn, user)

				return // hijack connection
			}

			host := request.Header.Get("Host")
			if host != "" {
				request.Host = host
			}

			request.RequestURI = ""

			RemoveHopByHopHeaders(request.Header)
			RemoveExtraHTTPHostPort(request)

			if request.URL.Scheme == "" || request.URL.Host == "" {
				resp = responseWith(http.StatusBadRequest)
			} else {
				client.user = user
				resp, err = client.client.Do(request)
				if err != nil {
					resp = responseWith(http.StatusBadGateway)
				}
			}
		}

		RemoveHopByHopHeaders(resp.Header)

		if keepAlive {
			resp.Header.Set("Proxy-Connection", "keep-alive")
			resp.Header.Set("Connection", "keep-alive")
			resp.Header.Set("Keep-Alive", "timeout=4")
		}

		resp.Close = !keepAlive

		err = resp.Write(conn)
		if err != nil {
			break // close connection
		}
	}

	conn.Close()
}

func authenticate(request *http.Request, cache *cache.Cache) (resp *http.Response, user string) {
	authenticator := authStore.Authenticator()
	if authenticator != nil {
		credential := ParseBasicProxyAuthorization(request)
		if credential == "" {
			resp = responseWith(http.StatusProxyAuthRequired)
			resp.Header.Set("Proxy-Authenticate", "Basic")
			return
		}

		var authed interface{}
		if authed = cache.Get(credential); authed == nil {
			user, pass, err := DecodeBasicProxyAuthorization(credential)
			authed = &authCache{
				success: err == nil && authenticator.Verify(user, pass),
				user:    user,
			}
			cache.Put(credential, authed, time.Minute)
		}
		if !authed.(*authCache).success {
			log.Infoln("Auth failed from %s", request.RemoteAddr)
			return responseWith(http.StatusForbidden), ""
		}
		user = authed.(*authCache).user
		return
	}

	return
}

func responseWith(statusCode int) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
	}
}
