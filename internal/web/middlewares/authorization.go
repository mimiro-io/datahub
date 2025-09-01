package middlewares

import (
	"net/http"

	"github.com/spf13/viper"

	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/security"
)

func Authorizer(core *security.ServiceCore) func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
	opaEndpoint := viper.GetString("OPA_ENDPOINT")
	return func(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
		return func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				// get user token
				token := c.Get("user").(*jwt.Token)

				// check OPA
				datasets, err := doOpaCheck(logger, c.Request().Method, c.Request().URL.Path, token, scopes, opaEndpoint)
				if err != nil {
					// if OPA failed, check ACL
					err = doAclCheck(c.Request().Method, c.Request().URL.Path, token, core)
					if err != nil {
						return err
					}
				} else {
					c.Set("datasets", datasets)
				}

				// if all above checks passed, continue
				return next(c)
			}
		}
	}
}

func doAclCheck(method string, path string, token *jwt.Token, core *security.ServiceCore) error {
	claims := token.Claims.(*security.CustomClaims)
	roles := claims.Roles

	for _, role := range roles {
		if role == "admin" {
			return nil
		}
	}

	// get subject
	subject := claims.Subject
	acl := core.GetAccessControls(subject)
	if acl == nil {
		return echo.NewHTTPError(http.StatusForbidden, "user does not have permission")
	}

	// get the method
	action := "read"
	if method == "DELETE" || method == "POST" {
		action = "write"
	}

	for _, ac := range acl {
		if core.CheckGranted(ac, path, action) {
			return nil
		}
	}

	return echo.NewHTTPError(http.StatusForbidden, "user does not have permission")
}

func NoOpAuthorizer(logger *zap.SugaredLogger, scopes ...string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return next(c)
		}
	}
}
