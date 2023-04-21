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
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/jobs"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type jobOperationHandler struct {
	jobScheduler *jobs.Scheduler
}

func NewJobOperationHandler(
	lc fx.Lifecycle,
	e *echo.Echo,
	logger *zap.SugaredLogger,
	mw *Middleware,
	js *jobs.Scheduler,
) {
	log := logger.Named("web")
	handler := &jobOperationHandler{
		jobScheduler: js,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			e.PUT("/job/:jobid/pause", handler.jobsPause, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/resume", handler.jobsUnpause, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/kill", handler.jobsKill, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/run", handler.jobsRun, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/reset", handler.jobsReset, mw.authorizer(log, datahubWrite))
			e.GET("/job/:jobid/status", handler.jobsGetStatus, mw.authorizer(log, datahubRead)) // is it running

			return nil
		},
	})
}

func (handler *jobOperationHandler) jobsPause(c echo.Context) error {
	jobID := c.Param("jobid")
	err := handler.jobScheduler.PauseJob(jobID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "could not pause job")
	}
	return c.JSON(http.StatusOK, &JobResponse{JobID: jobID})
}

func (handler *jobOperationHandler) jobsKill(c echo.Context) error {
	jobID := c.Param("jobid")
	handler.jobScheduler.KillJob(jobID)
	return c.JSON(http.StatusOK, &JobResponse{JobID: jobID})
}

func (handler *jobOperationHandler) jobsUnpause(c echo.Context) error {
	jobID := c.Param("jobid")
	err := handler.jobScheduler.UnpauseJob(jobID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "could not un-pause job")
	}
	return c.JSON(http.StatusOK, &JobResponse{JobID: jobID})
}

func (handler *jobOperationHandler) jobsRun(c echo.Context) error {
	jobID := c.Param("jobid")
	jobType := c.QueryParam("jobType")
	if len(jobType) == 0 {
		jobType = jobs.JobTypeIncremental
	}
	tempID, err := handler.jobScheduler.RunJob(jobID, jobType)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "could not un-pause job")
	}
	if tempID == "" {
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, &JobResponse{JobID: tempID})
}

func (handler *jobOperationHandler) jobsReset(c echo.Context) error {
	jobID := c.Param("jobid")
	since := c.QueryParam("since")

	err := handler.jobScheduler.ResetJob(jobID, since)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "internal error")
	}
	return c.JSON(http.StatusOK, &JobResponse{JobID: jobID})
}

func (handler *jobOperationHandler) jobsGetStatus(c echo.Context) error {
	jobID := c.Param("jobid")

	status := handler.jobScheduler.GetRunningJob(jobID)
	if status == nil {
		return c.JSON(http.StatusOK, []*jobs.JobStatus{})
	}

	return c.JSON(http.StatusOK, []*jobs.JobStatus{status}) // converted to a list
}
