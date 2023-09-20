// Copyright 2021 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package web

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
)

// This is used to give the web handler a type, instead of just a map
// The reason for this, is that it makes it less random how the json
// gets transformed. This should probably be done with Source and Sink as well

type JobResponse struct {
	JobID string `json:"jobId"`
}

type jobsHandler struct {
	jobScheduler *jobs.Scheduler
}

func NewJobsHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, js *jobs.Scheduler) {
	log := logger.Named("web")
	handler := &jobsHandler{
		jobScheduler: js,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			// jobs
			e.GET("/jobs", handler.jobsList, mw.authorizer(log, datahubRead)) // list of all defined jobs

			// internal usage
			e.GET("/jobs/_/schedules", handler.jobsListSchedules, mw.authorizer(log, datahubRead))
			e.GET("/jobs/_/status", handler.jobsListStatus, mw.authorizer(log, datahubRead))
			e.GET("/jobs/_/history", handler.jobsListHistory, mw.authorizer(log, datahubRead))

			e.GET(
				"/jobs/:jobid",
				handler.jobsGetDefinition,
				mw.authorizer(log, datahubRead),
			) // the json used to define it
			e.DELETE("/jobs/:jobid", handler.jobsDelete, mw.authorizer(log, datahubWrite)) // remove an existing job
			e.POST("/jobs", handler.jobsAdd, mw.authorizer(log, datahubWrite))

			return nil
		},
	})
}

func (handler *jobsHandler) jobsList(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.jobScheduler.ListJobs())
}

func (handler *jobsHandler) jobsAdd(c echo.Context) error {
	// read json
	body, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HTTPBodyMissingErr(err).Error())
	}

	config, err := handler.jobScheduler.Parse(body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HTTPJobParsingErr(err).Error())
	}

	err = handler.jobScheduler.AddJob(config)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, &JobResponse{JobID: config.ID})
}

func (handler *jobsHandler) jobsGetDefinition(c echo.Context) error {
	jobID := c.Param("jobid")

	res, err := handler.jobScheduler.LoadJob(jobID)
	if err != nil || res.ID == "" {
		return c.NoContent(http.StatusNotFound)
	}
	state, err := handler.jobScheduler.GetJobState(jobID)
	if err != nil {
		handler.jobScheduler.Logger.Errorf("Failed to get job state for job %s: %s", jobID, err.Error())
		return c.NoContent(http.StatusInternalServerError)
	}
	res.Source["continuation"] = state.ContinuationToken

	return c.JSON(http.StatusOK, res)
}

func (handler *jobsHandler) jobsListSchedules(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.jobScheduler.GetScheduleEntries())
}

func (handler *jobsHandler) jobsListStatus(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.jobScheduler.GetRunningJobs())
}

func (handler *jobsHandler) jobsListHistory(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.jobScheduler.GetJobHistory())
}

// jobsDelete will delete a job with the given jobid if it exists
// it should return 200 OK when successful, but 404 if the job id
// does not exists
func (handler *jobsHandler) jobsDelete(c echo.Context) error {
	jobID, _ := url.QueryUnescape(c.Param("jobid"))

	err := handler.jobScheduler.DeleteJob(jobID)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}
	return c.NoContent(http.StatusOK)
}