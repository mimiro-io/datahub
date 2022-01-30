package web

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"github.com/golang-jwt/jwt"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/security"
	"io/ioutil"
	"net/http"
)

func NewWebService() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	return e
}

func extractToken(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	l := len("Bearer")
	if len(auth) > l+1 && auth[:l] == "Bearer" {
		return auth[l+1:], nil
	}
	return "", errors.New("no token")
}

func MakeRoleCheckMiddleware(expectedRoles ...string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token := c.Get("user").(*jwt.Token)
			claims := token.Claims.(*security.CustomClaims)

			for _,role := range claims.Roles {
				for _,expectedRole := range expectedRoles {
					if role == expectedRole {
						return nil
					}
				}
			}
			return echo.NewHTTPError(http.StatusUnauthorized, "missing correct role")
		}
	}
}

func MakeJWTMiddleware(publicKey *rsa.PublicKey, expectedAudience string, expectedIssuer string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {

			auth, err := extractToken(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			token, err := jwt.ParseWithClaims(auth, &security.CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
				return publicKey, nil
			})

			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}

			claims := token.Claims.(*security.CustomClaims)

			// verify the audience is correct, audience must be set
			checkAud := claims.VerifyAudience(expectedAudience, true)
			if !checkAud {
				err = errors.New("invalid audience")
			}

			// verify the issuer of the token, issuer must be set
			checkIss := claims.VerifyIssuer(expectedIssuer, true)
			if !checkIss {
				err = errors.New("invalid issuer")
			}

			if err == nil {
				c.Set("user", token)
				return next(c)
			}

			return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
		}
	}
}

func RegisterHandlers(echo *echo.Echo, serviceCore *security.ServiceCore) {
	handler := NewServiceHandler(serviceCore)

	echo.POST("/security/token", handler.tokenRequestHandler)

	// requires middleware for JWT is valid and checking admin role
	jwtMiddleware := MakeJWTMiddleware(serviceCore.GetActiveKeyPair().PublicKey, "node:" + serviceCore.NodeInfo.NodeId, "node:" + serviceCore.NodeInfo.NodeId)

	echo.POST("/security/clients", handler.registerClientRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
	echo.POST("/security/clientclaims", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
	echo.POST("/security/clientacl", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))

	// requires middleware for JWT is valid and checking client role
	// echo.POST("/security/isallowed", handler.checkClientResourceActionRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("client", "admin"))
	// echo.POST("/security/isinrole", handler.checkClientResourceActionRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("client", "admin"))

	// create signed token request
	// echo.GET("/security/signedtokenrequest", handler.createSignedTokenRequest, jwtMiddleware, MakeRoleCheckMiddleware("admin"))

	// get the jwks
	echo.GET("/.well-known/jwks", handler.tokenRequestHandler)
}

type ServiceHandler struct {
	serviceCore *security.ServiceCore
}

func NewServiceHandler(serviceCore *security.ServiceCore) *ServiceHandler {
	serviceHandler := &ServiceHandler{}
	serviceHandler.serviceCore = serviceCore
	return serviceHandler
}

type TokenRequest struct {
	RequestType    string // either key or pke
	ClientKey      string
	ClientSecret   string

	Message        string
	Signature      string
	Algorithm      string
}

// TokenResponse is a OAuth2 JWT Token Response
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType string `json:"token_type"`
}

type SignedClientRequest struct {
	Token string
}

func (handler *ServiceHandler) createSignedTokenRequest(c echo.Context) error {
	// fix me: fetch param for the audience
	token, err := handler.serviceCore.CreateJWTForTokenRequest("node1")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "error creating token " + err.Error())
	}
	response := &SignedClientRequest{}
	response.Token = token
	return c.JSON(http.StatusOK, response)
}

// oidc - oauth2 compliant token request handler
func (handler *ServiceHandler) tokenRequestHandler(c echo.Context) error {
	// url form encoded params
	var grantType string
	// var audience string

	grantType = c.FormValue("grant_type")
	clientAssertionType := c.FormValue("client_assertion_type")

	if grantType == "client_credentials" && clientAssertionType == "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		// assertion = c.FormValue("client_assertion")

	} else if grantType == "client_credentials" {
		clientId := c.FormValue("client_id")
		clientSecret := c.FormValue("client_secret")

		token, err := handler.serviceCore.MakeAdminJWT(clientId, clientSecret)
		if err != nil {
			return err
		}
		response := &TokenResponse{}
		response.AccessToken = token
		response.TokenType = "Bearer"
		return c.JSON(http.StatusOK, response)
	}

	return nil
}

func (handler *ServiceHandler) jwksRequestHandler(c echo.Context) error {
	return nil
}

func (handler *ServiceHandler) checkClientResourceActionRequestHandler(c echo.Context) error {
	return nil
}

func (handler *ServiceHandler) registerClientRequestHandler(c echo.Context) error {
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}

	clientInfo := &security.ClientInfo{}
	err = json.Unmarshal(body, clientInfo)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing body")
	}
	handler.serviceCore.RegisterClient(clientInfo)

	return c.NoContent(http.StatusOK)
}

func (handler *ServiceHandler) setClientClaimsRequestHandler(c echo.Context) error {
	return nil
}

func (handler *ServiceHandler) setClientAccessControlsRequestHandler(c echo.Context) error {
	return nil
}


