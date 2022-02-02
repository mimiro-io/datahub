package web

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"github.com/golang-jwt/jwt"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/security"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
)

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

func NewSecurityHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, core *security.ServiceCore) {
	log := logger.Named("web")
	handler := &SecurityHandler{}
	handler.serviceCore = core
	handler.logger = log

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// query
			e.POST("/security/token", handler.tokenRequestHandler)
			e.POST("/security/clients", handler.clientRegistrationRequestHandler, mw.authorizer(log, datahubWrite))

			/* jwtMiddleware := MakeJWTMiddleware(core.GetActiveKeyPair().PublicKey, "node:" + core.NodeInfo.NodeId, "node:" + core.NodeInfo.NodeId)

			e.POST("/security/clients", handler.registerClientRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			e.POST("/security/clientclaims", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			e.POST("/security/clientacl", handler.setClientClaimsRequestHandler, jwtMiddleware, MakeRoleCheckMiddleware("admin"))
			*/

			return nil
		},
	})
}

type SecurityHandler struct {
	serviceCore *security.ServiceCore
	logger *zap.SugaredLogger
}

// TokenResponse is a OAuth2 JWT Token Response
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType string `json:"token_type"`
}

// oidc - oauth2 compliant token request handler
func (handler *SecurityHandler) tokenRequestHandler(c echo.Context) error {
	// url form encoded params
	var grantType string
	// var audience string

	grantType = c.FormValue("grant_type")
	clientAssertionType := c.FormValue("client_assertion_type")

	if grantType == "client_credentials" && clientAssertionType == "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		assertion := c.FormValue("client_assertion")
		token, err:= handler.serviceCore.ValidateClientJWTMakeJWTAccessToken(assertion)

		if err != nil {
			// replace with not authorised
			return err
		}
		response := &TokenResponse{}
		response.AccessToken = token
		response.TokenType = "Bearer"
		return c.JSON(http.StatusOK, response)

	} else if grantType == "client_credentials" {
		clientId := c.FormValue("client_id")
		clientSecret := c.FormValue("client_secret")

		token, err := handler.serviceCore.MakeAdminJWT(clientId, clientSecret)
		if err != nil {
			// replace with not authorised
			return err
		}
		response := &TokenResponse{}
		response.AccessToken = token
		response.TokenType = "Bearer"
		return c.JSON(http.StatusOK, response)
	}

	return nil
}

func (handler *SecurityHandler) clientRegistrationRequestHandler(c echo.Context) error {
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

func (handler *SecurityHandler) setClientClaimsRequestHandler(c echo.Context) error {
	return nil
}

func (handler *SecurityHandler) setClientAccessControlsRequestHandler(c echo.Context) error {
	return nil
}


