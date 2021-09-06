package rules

import (
	C "github.com/Dreamacro/clash/constant"
)

type AuthUser struct {
	user    string
	adapter string
}

func (au *AuthUser) RuleType() C.RuleType {
	return C.AuthUser
}

func (au *AuthUser) Match(metadata *C.Metadata) bool {
	return metadata.AuthUser == au.user
}

func (au *AuthUser) Adapter() string {
	return au.adapter
}

func (au *AuthUser) Payload() string {
	return au.user
}

func (au *AuthUser) ShouldResolveIP() bool {
	return false
}

func NewAuthUser(user string, adapter string) *AuthUser {
	return &AuthUser{
		user:    user,
		adapter: adapter,
	}
}
