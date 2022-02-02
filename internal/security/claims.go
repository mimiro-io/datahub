package security

import (
	"github.com/golang-jwt/jwt"
	"strings"
)

type CustomClaims struct {
	Scope string `json:"scope"`
	Gty   string `json:"gty"`
	Adm   bool   `json:"adm"`
	Roles []string `json:"roles"`
	jwt.StandardClaims
}

func (claims CustomClaims) Scopes() []string {
	return strings.Split(claims.Scope, ",")
}

