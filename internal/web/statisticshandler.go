package web

import (
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
)

func RegisterStatisticsHandler(e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, store *server.Store) {
	log := logger.Named("web")

	statistics := &server.Statistics{store, logger.Named("statistics")}

	e.GET("/statistics", func(c echo.Context) error {
		return statistics.GetStatistics(c.Response(), c.Request().Context())
	}, mw.authorizer(log, datahubRead))

	e.GET("/statistics/:ds", func(c echo.Context) error {
		datasetName := c.Param("ds")
		return statistics.GetStatisticsForDs(datasetName, c.Response(), c.Request().Context())
	}, mw.authorizer(log, datahubRead))
}
