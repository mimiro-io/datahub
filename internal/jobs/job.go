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
	"time"

	"github.com/bamzi/jobrunner"
	"github.com/mimiro-io/datahub/internal/server"
)

type job struct {
	id       string
	pipeline Pipeline
	schedule string
	topic    string
	isEvent  bool
	runner   *Runner
}

type jobResult struct {
	Id        string    `json:"id"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	LastError string    `json:"lastError"`
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
			//TODO: read this through viper into env.Config? curretnly only used in unit test but could be useful as general config?
			durationOverride, found := os.LookupEnv("JOB_FULLSYNC_RETRY_INTERVAL")
			if found {
				d, err := time.ParseDuration(durationOverride)
				if err == nil {
					duration = d
				}
			}
			job.runner.logger.Infof("Job %v is running, queuing current run request for retry in %v", job.id, duration)
			jobrunner.In(duration, job) // reschedule the full sync again in 5 seconds
			return
		}
		// could not obtain ticket. This indicates a job with the same jobId is running already.
		// We skip this execution request
		return
	}
	defer job.runner.raffle.returnTicket(ticket)

	msg := "job"
	if job.isEvent {
		msg = "event"
	}
	jobType := "incremental"
	if job.pipeline.isFullSync() {
		jobType = "fullsync"
	}

	job.runner.logger.Infof("Starting %v %s with id '%s'", jobType, msg, job.id)

	tags := []string{
		"application:datahub",
		fmt.Sprintf("jobs:job-%s", job.id),
	}
	_ = job.runner.statsdClient.Incr("jobs.count", tags, 1)

	sourceType := job.pipeline.spec().source.GetConfig()["Type"]
	sinkType := job.pipeline.spec().sink.GetConfig()["Type"]
	job.runner.logger.Infof(" > Running task '%s': %s -> %s", job.id, sourceType, sinkType)
	err := job.pipeline.sync(job, ticket.runState.ctx)
	if err != nil {
		if err.Error() == "got job interrupt" { // if a job gets killed, this will trigger
			job.runner.logger.Infof("Job '%s' was terminated", job.id)
		} else {
			_ = job.runner.statsdClient.Count("jobs.error", 1, tags, 1) // we only want to count runtime errors
			job.runner.logger.Warnf("Failed running task for job '%s': %s", job.id, err.Error())
		}
	} else {
		_ = job.runner.statsdClient.Count("jobs.success", 1, tags, 1)
	}

	timed := time.Since(ticket.runState.started)

	job.runner.logger.Infof("Finished %s with id '%s' - duration was %s", msg, job.id, timed)

	// we store the last run info
	lastRun := &jobResult{
		Id:        job.id,
		Start:     ticket.runState.started,
		End:       time.Now(),
		LastError: "",
	}
	if err != nil {
		lastRun.LastError = err.Error()
	}
	// its not really a problem to ignore this error
	_ = job.runner.store.StoreObject(server.JOB_RESULT_INDEX, job.id, lastRun)
}
