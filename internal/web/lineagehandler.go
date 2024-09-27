package web

import (
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/dataset"
	"go.uber.org/zap"
)

func RegisterLineageHandler(e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, store *server.Store, datasetManager *server.DsManager) {

	lineage := dataset.NewLineageBuilder(store, datasetManager, logger.Named("lineage"))
	log := logger.Named("web")
	e.GET("/lineage/:dataset", func(c echo.Context) error {
		datasetName := c.Param("dataset")
		if datasetName == "" {
			return echo.NewHTTPError(400, "dataset name is required")
		}
		// check dataset exists
		ds := datasetManager.GetDataset(datasetName)
		if ds == nil {
			return echo.NewHTTPError(404, "dataset is not found")
		}
		res, err := lineage.ForDataset(datasetName)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, res)
	}, mw.authorizer(log, datahubRead))

	e.GET("/lineage", func(c echo.Context) error {
		res, err := lineage.ForAll()
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, res)
	}, mw.authorizer(log, datahubRead))
}
