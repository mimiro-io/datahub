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
	"github.com/DataDog/datadog-go/statsd"
	"github.com/bamzi/jobrunner"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mustafaturan/bus"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// The Runner is used to organize and keep track of configured jobs. It is also responsible for running (duh) jobs.
// It will also pretty log everything it is doing. The Runner should only be interacted with from the Scheduler.
type Runner struct {
	logger         *zap.SugaredLogger
	store          *server.Store
	scheduledJobs  map[string][]cron.EntryID
	statsdClient   statsd.ClientInterface
	tokenProviders *security.TokenProviders
	raffle         *raffle
	eventBus       server.EventBus
}

// RunnerConfig sets the initial config for the underlying job runner.
// PoolIncremental defines the max number of jobs that can be ran at once
// Concurrent defines how many of the same EntryID should be allowed, this should always be 0 in the datahub
type RunnerConfig struct {
	PoolIncremental int
	PoolFull        int
	Concurrent      int
}

// SyncJobState used to capture the state of a running job
type SyncJobState struct {
	ID                 string `json:"id"`
	ContinuationToken  string `json:"token"`
	LastRunCompletedOk bool   `json:"lastrunok"`
	LastRunError       string `json:"lastrunerror"`
}

func NewRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PoolIncremental: 10,
		PoolFull:        10,
		Concurrent:      1,
	}
}

// NewRunner creates a new job runner. It should only be used from the main.go
func NewRunner(config *RunnerConfig, env *conf.Env, store *server.Store, tokenProviders *security.TokenProviders, eb server.EventBus, statsdClient statsd.ClientInterface) *Runner {
	logger := env.Logger.Named("jobrunner")
	logger.Infof("Starting the JobRunner")
	jobrunner.Start(config.PoolIncremental+config.PoolFull+3, config.Concurrent) // we add 3 extra to be able to postpone pipelines
	return &Runner{
		logger:         logger,
		store:          store,
		scheduledJobs:  make(map[string][]cron.EntryID),
		tokenProviders: tokenProviders,
		statsdClient:   statsdClient,
		raffle:         NewRaffle(config.PoolFull, config.PoolIncremental, logger, statsdClient),
		eventBus:       eb,
	}
}

// Stop calls the cron stop method to stop all future scheduled jobs. It will also go trough the list of running jobs
// and cancel them.
func (runner *Runner) Stop() {
	jobrunner.Stop()
	for _, v := range runner.raffle.runningJobs {
		v.cancel()
	}
}

// addJob adds a job, and depending on the type, it delegates to the correct add func
func (runner *Runner) addJob(job *job) error {
	if job.isEvent {
		return runner.addEventJob(job)
	} else {
		return runner.addScheduledJob(job)
	}
}

func (runner *Runner) startJob(j *job) {
	runner.logger.Infof("Starting job with id '%s'(%s) to run once", j.id, j.title)
	jobrunner.Now(j)
}

// addScheduledJob adds a job to be scheduled. It does this by first clearing the existing schedule,
// and then re-adds it to the cron.
// If either incremental or full schedules are added, the list
// of scheduledJobs will be updated with the cron.EntryID. Jobs added here should be validated properly.
// TODO: add protection against adding an already processing job
func (runner *Runner) addScheduledJob(job *job) error {
	runner.logger.Infof("Adding job with id '%s'(%s) to schedule '%s'", job.id, job.title, job.schedule)
	entryId, err := runner.schedule(job.schedule, job)
	if err != nil {
		runner.logger.Errorf("Error scheduling job %v (%s): %w", job.id, job.title, err)
		return err
	}
	runner.scheduledJobs[job.id] = append(runner.scheduledJobs[job.id], entryId)

	return nil
}

// addEventJob adds a event subscription to run the job on an event
func (runner *Runner) addEventJob(job *job) error {
	runner.eventBus.SubscribeToDataset(job.id, job.topic, func(e *bus.Event) {
		go func() { // this prevents blocking on the event bus
			job.Run()
		}()

	})
	return nil
}

// deleteJob deletes a job with the given jobId. It will delete the job configuration
// from the store, and clean up future scheduled jobs from the cron.
// TODO: extend to interrupt running jobs
func (runner *Runner) deleteJob(jobId string) error {
	runner.logger.Infof("Deleting job with id '%s'", jobId)
	defer func() {
		// make sure the schedules are removed from the crontab
		clearCrontab(runner.scheduledJobs, jobId)
		runner.eventBus.UnsubscribeToDataset(jobId)
	}()
	err := runner.store.DeleteObject(server.JOB_CONFIGS_INDEX, jobId)
	if err != nil {
		return err
	}
	return nil
}

// killJob stops a running job as soon as possible
func (runner *Runner) killJob(jobId string) {
	running := runner.raffle.runningJob(jobId)
	if running != nil {
		runner.logger.Infof("Killing job with id '%s'", jobId)
		running.cancel()
	}

}

// clearCrontab makes sure old entries are removed from the list before new are added
func clearCrontab(jobs map[string][]cron.EntryID, jobId string) {
	entryIds, ok := jobs[jobId]
	if ok {
		for _, id := range entryIds {
			jobrunner.Remove(id)
		}
		delete(jobs, jobId)
	}
}

// schedule is an implementation of jobrunner.Schedule, but changed to return the EntryID
// from the cron
func (runner *Runner) schedule(spec string, job cron.Job) (cron.EntryID, error) {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return -1, err
	}
	id := jobrunner.MainCron.Schedule(sched, jobrunner.New(job))
	return id, nil
}
