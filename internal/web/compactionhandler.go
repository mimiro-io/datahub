package web

import (
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	ds "github.com/mimiro-io/datahub/internal/service/dataset"
	"go.uber.org/zap"
	"io"
	"net/http"
)

type compactionRequest struct {
	Dataset  string
	Strategy string
}

func RegisterCompactionHandler(e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, dm *server.DsManager, store *server.Store) {

	log := logger.Named("web")
	compactionWorker := ds.NewCompactor(store, dm, logger.Named("compaction-worker"))
	e.POST("/compact", func(c echo.Context) error {
		r := &compactionRequest{}
		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HTTPBodyMissingErr(err).Error())
		}
		err = json.Unmarshal(body, &r)

		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HTTPJsonParsingErr(err).Error())
		}
		if r.Dataset == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "dataset not provided")
		}
		if r.Strategy == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "strategy not provided")
		}
		if r.Strategy != "deduplication" {
			return echo.NewHTTPError(http.StatusBadRequest, "strategy not supported. allowed values: deduplication")
		}
		store := server.NewBadgerAccess(store, dm)
		dsID := r.Dataset
		if _, ok := store.LookupDatasetID(dsID); ok {
			err := compactionWorker.CompactAsync(dsID, ds.DeduplicationStrategy())
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}

			return c.NoContent(http.StatusOK)
		}
		return echo.NewHTTPError(http.StatusNotFound, "dataset not found")
	}, mw.authorizer(log, datahubWrite))
}
