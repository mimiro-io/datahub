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

package jobs

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/bamzi/jobrunner"

	"github.com/mimiro-io/datahub/internal/server"
)

type job struct {
	id            string
	title         string
	pipeline      Pipeline
	schedule      string
	topic         string
	isEvent       bool
	runner        *Runner
	errorHandlers []*ErrorHandler
}

type jobResult struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	LastError string    `json:"lastError"`
	Processed int       `json:"processed"`
}

// Run is implementing the jobrunner.Run() interface. It will automatically be called when
// the scheduled job is started.
// When starting, a runState is added to the list of runningJobs. Once the job is finished, this will be removed.
// The method will never return an error, as it should not fail (however individual jobs can fail)
// It will use a ticket system to get a ticket to run. If an incr job doesnt get a ticket, it will be postponed until
// next run, if a full job doesn't get one, we assume it is waiting for an incr job to finnish, so it gets postponed for
// 5s
func (job *job) Run() {
	ticket := job.runner.raffle.borrowTicket(job)
	if ticket == nil {
		if job.pipeline.isFullSync() { // reschedule to try again in a bit
			duration := 5 * time.Second
			// TODO: read this through viper into env.Config? curretnly only used in unit test but could be useful as general config?
			durationOverride, found := os.LookupEnv("JOB_FULLSYNC_RETRY_INTERVAL")
			if found {
				d, err := time.ParseDuration(durationOverride)
				if err == nil {
					duration = d
				}
			}
			job.runner.logger.Infow(fmt.Sprintf("Job %v (%s) is running or could not get a ticket (%v avail). "+
				"queuing for retry in %v", job.title, job.id, job.runner.raffle.ticketsFull, duration),
				"job.jobId", job.id,
				"job.jobTitle", job.title,
				"job.state", "Running")
			if queueRetry(duration, job) { // reschedule the full sync again in 5 seconds
				_ = job.runner.statsdClient.Count("jobs.backpressure", 1, []string{"application:datahub"}, 1)
			}
			return
		}
		// could not obtain ticket. This indicates a job with the same jobId is running already. Or tickets are empty.
		// We skip this execution request
		job.runner.logger.Infow(fmt.Sprintf("Job %v (%s) running or did not get a ticket (%v avail), skipping.",
			job.title, job.id, job.runner.raffle.ticketsIncr), "job.jobId", job.id, "job.jobTitle", job.title)
		return
	}
	var pipelineErr error
	defer job.handleJobError(&pipelineErr)
	defer job.runner.raffle.returnTicket(ticket)
	msg := "job"
	if job.isEvent {
		msg = "event"
	}
	jobType := "incremental"
	if job.pipeline.isFullSync() {
		jobType = "fullsync"
	}

	job.runner.logger.Infow(fmt.Sprintf("Starting %v %s with id '%s' (%s)", jobType, msg, job.title, job.id),
		"job.jobId", job.id,
		"job.jobTitle", job.title,
		"job.state", "Starting",
		"job.jobType", jobType)

	tags := []string{
		"application:datahub",
		fmt.Sprintf("jobs:job-%s", job.title),
		fmt.Sprintf("jobtype:%v", jobType),
	}
	_ = job.runner.statsdClient.Count("jobs.count", 1, tags, 1)

	sourceType := job.pipeline.spec().source.GetConfig()["Type"]
	sinkType := job.pipeline.spec().sink.GetConfig()["Type"]
	job.runner.logger.Infow(fmt.Sprintf(" > Running task '%s' (%s): %s -> %s", job.title, job.id, sourceType, sinkType),
		"job.jobId", job.id,
		"job.jobTitle", job.title,
		"job.state", "Running",
		"job.jobType", jobType)
	processed, err := job.pipeline.sync(job, ticket.runState.ctx)
	pipelineErr = err
	timed := time.Since(ticket.runState.started)
	if err != nil {
		if err.Error() == "got job interrupt" { // if a job gets killed, this will trigger
			_ = job.runner.statsdClient.Count("jobs.cancelled", timed.Nanoseconds(), tags, 1)
			job.runner.logger.Infow(fmt.Sprintf("Job '%s' (%s) was terminated", job.title, job.id),
				"job.jobId", job.id,
				"job.jobTitle", job.title,
				"job.state", "Terminated",
				"job.jobType", jobType)
		} else {
			_ = job.runner.statsdClient.Count("jobs.error", timed.Nanoseconds(), tags, 1)
			job.runner.logger.Warnw(fmt.Sprintf("Failed running task for job '%s' (%s): %s", job.title, job.id, err.Error()),
				"job.jobId", job.id,
				"job.jobTitle", job.title,
				"job.state", "Failed",
				"job.jobType", jobType,
				"job.executionErrorMessage", err.Error())
		}
	} else {
		_ = job.runner.statsdClient.Count("jobs.success", timed.Nanoseconds(), tags, 1)
	}

	job.runner.logger.Infow(
		fmt.Sprintf("Finished %s with id '%s' (%s) - duration was %s. processed %v entities",
			msg, job.title, job.id, timed, processed),
		"job.jobId", job.id,
		"job.jobTitle", job.title,
		"job.state", "Finished",
		"job.jobType", jobType,
	)

	// we store the last run info
	lastRun := &jobResult{
		ID:        job.id,
		Title:     job.title,
		Start:     ticket.runState.started,
		End:       time.Now(),
		Processed: processed,
		LastError: "",
	}
	if err != nil {
		lastRun.LastError = err.Error()
	}
	// its not really a problem to ignore this error
	_ = job.runner.store.StoreObject(server.JobResultIndex, job.id, lastRun)
}

var retryJobIds sync.Map

func queueRetry(duration time.Duration, j *job) bool {
	if _, alreadyQueued := retryJobIds.LoadOrStore(j.id, true); alreadyQueued {
		j.runner.logger.Infow(fmt.Sprintf("could not queue, job already queued for retry: %v - %v", j.title, j.id),
			"job.jobId", j.id,
			"job.jobTitle", j.title,
			"job.jobState", "Running")
		return false
	}
	go func() {
		time.Sleep(duration)
		retryJobIds.Delete(j.id)
		jobrunner.New(j).Run()
	}()
	return true
}
