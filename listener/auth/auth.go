package auth

import (
	"github.com/Dreamacro/clash/component/auth"
)

var (
	authenticator auth.Authenticator
)

func Authenticator() auth.Authenticator {
	return authenticator
}

func Users() []string {
	if authenticator == nil {
		return []string{}
	}
	return authenticator.Users()
}

func SetAuthenticator(au auth.Authenticator) {
	authenticator = au
}
