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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/security"

	"github.com/bamzi/jobrunner"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

// The Scheduler deals with reading and writing jobs and making sure they get added to the
// job Runner. It also deals with translating between the external JobConfiguration and the
// internal job format.
type Scheduler struct {
	Logger         *zap.SugaredLogger
	Store          *server.Store
	Runner         *Runner
	DatasetManager *server.DsManager
}

const TriggerTypeCron = "cron"
const TriggerTypeOnChange = "onchange"
const JobTypeFull = "fullsync"
const JobTypeIncremental = "incremental"

var TriggerTypes = map[string]bool{TriggerTypeOnChange: true, TriggerTypeCron: true}
var JobTypes = map[string]bool{JobTypeFull: true, JobTypeIncremental: true}

type JobTrigger struct {
	TriggerType      string `json:"triggerType"`
	JobType          string `json:"jobType"`
	Schedule         string `json:"schedule"`
	MonitoredDataset string `json:"monitoredDataset"`
}

// JobConfiguration is the external interfacing object to configure a job. It is also the one that gets persisted
// in the store.
type JobConfiguration struct {
	Id          string                 `json:"id"`
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Tags        []string               `json:"tags"`
	Source      map[string]interface{} `json:"source"`
	Sink        map[string]interface{} `json:"sink"`
	Transform   map[string]interface{} `json:"transform"`
	Triggers    []JobTrigger           `json:"triggers"`
	Paused      bool                   `json:"paused"`
	BatchSize   int                    `json:"batchSize"`
}

type ScheduleEntries struct {
	Entries []ScheduleEntry `json:"entries"`
}

type ScheduleEntry struct {
	Id       int       `json:"id"`
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Next     time.Time `json:"next"`
	Prev     time.Time `json:"prev"`
}

// NewScheduler returns a new Scheduler. When started, it will load all existing JobConfiguration's from the store,
// and schedule this with the runner.
func NewScheduler(lc fx.Lifecycle, env *conf.Env, store *server.Store, dsm *server.DsManager, runner *Runner) *Scheduler {
	scheduler := &Scheduler{
		Logger:         env.Logger.Named("scheduler"),
		Store:          store,
		Runner:         runner,
		DatasetManager: dsm,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			scheduler.Logger.Infof("Starting the JobScheduler")

			for _, j := range scheduler.loadConfigurations() {
				err := scheduler.AddJob(j)
				if err != nil {
					scheduler.Logger.Warnf("Error loading job with id %s (%s), err: %v", j.Id, j.Title, err)
				}
			}

			return nil
		},
		OnStop: func(ctx context.Context) error {
			scheduler.Logger.Infof("Stopping job runner")
			runner.Stop()
			return nil
		},
	})

	return scheduler
}

// AddJob takes an incoming JobConfiguration and stores it in the store
// Once it has stored it, it will transform it to a Pipeline and add it to the scheduler
// It is important that jobs are valid, so care is taken to validate the JobConfiguration before
// it can be scheduled.
func (s *Scheduler) AddJob(jobConfig *JobConfiguration) error {
	err := s.verify(jobConfig)
	if err != nil {
		return err
	}

	// this also verifies that it can be parsed, so do this first
	triggeredJobs, err := s.toTriggeredJobs(jobConfig)
	if err != nil {
		return err
	}

	err = s.Store.StoreObject(server.JOB_CONFIGS_INDEX, jobConfig.Id, jobConfig) // store it for the future
	if err != nil {
		return err
	}

	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		// make sure we clear up before adding
		clearCrontab(s.Runner.scheduledJobs, jobConfig.Id)
		s.Runner.eventBus.UnsubscribeToDataset(jobConfig.Id)
		for _, job := range triggeredJobs {
			if !jobConfig.Paused { // only add the job if it is not paused
				err := s.Runner.addJob(job)
				if err != nil {
					return err
				}
			} else {
				s.Logger.Infof("Job '%s' is currently paused, it will not be started automatically", jobConfig.Id)
			}
		}
		return nil
	})

	return g.Wait()
}

// extractJobs extracts the jobs configured in the JobConfiguration and extracts them as a
// list of jobs to be scheduled, depending on the type
func (s *Scheduler) toTriggeredJobs(jobConfig *JobConfiguration) ([]*job, error) {
	var result []*job
	for _, t := range jobConfig.Triggers {
		pipeline, err := s.toPipeline(jobConfig, t.JobType) // this also verifies that it can be parsed, so do this first
		if err != nil {
			return nil, err
		}
		switch t.TriggerType {
		case TriggerTypeOnChange:
			result = append(result, &job{
				id:       jobConfig.Id,
				title:    jobConfig.Title,
				pipeline: pipeline,
				topic:    t.MonitoredDataset,
				isEvent:  true,
				runner:   s.Runner,
			})
		case TriggerTypeCron:
			result = append(result, &job{
				id:       jobConfig.Id,
				title:    jobConfig.Title,
				pipeline: pipeline,
				schedule: t.Schedule,
				runner:   s.Runner,
			})
		default:
			return nil, errors.New(fmt.Sprintf("could not map trigger configuration to job: %v", t))
		}
	}
	return result, nil
}

// DeleteJob deletes a JobConfiguration, and calls out to the Runner to make sure it also gets removed from
// the running jobs.
// It will attempt to load the job before it deletes it, to validate it's existence.
func (s *Scheduler) DeleteJob(jobId string) error {
	jobConfig, err := s.LoadJob(jobId)
	if err != nil {
		return err
	}
	err = s.Runner.deleteJob(jobConfig.Id)
	if err != nil {
		return err
	}

	return nil
}

// LoadJob will attempt to load a JobConfiguration based on a jobId. Because of the GetObject method currently
// works, it will not return nil when not found, but an empty jobConfig object.
func (s *Scheduler) LoadJob(jobId string) (*JobConfiguration, error) {
	jobConfig := &JobConfiguration{}
	err := s.Store.GetObject(server.JOB_CONFIGS_INDEX, jobId, jobConfig)
	if err != nil {
		return nil, err
	}
	return jobConfig, nil
}

// ListJobs returns a list of all stored configurations
func (s *Scheduler) ListJobs() []*JobConfiguration {
	return s.loadConfigurations()
}

// Parse is a convenience method to parse raw config json into a JobConfiguration
func (s *Scheduler) Parse(rawJson []byte) (*JobConfiguration, error) {
	config := &JobConfiguration{}
	err := json.Unmarshal(rawJson, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetScheduleEntries returns a cron list of all scheduled entries currently scheduled.
// Paused jobs are not part of this list
func (s *Scheduler) GetScheduleEntries() ScheduleEntries {
	jobs := s.Runner.scheduledJobs
	lookup := map[int]string{}
	for k, v := range jobs {
		for id := range v {
			lookup[id] = k
		}
	}

	se := []ScheduleEntry{}
	for _, e := range jobrunner.Entries() {
		jobId := lookup[int(e.ID)]
		jobTitle, err := s.resolveJobTitle(jobId)
		if err != nil {
			s.Logger.Warnf("Failed to resolve title for job id '%s'", jobId)
		}
		se = append(se, ScheduleEntry{
			Id:       int(e.ID),
			JobId:    jobId,
			JobTitle: jobTitle,
			Next:     e.Next,
			Prev:     e.Prev,
		})
	}

	entries := ScheduleEntries{
		Entries: se,
	}

	return entries
}

type JobStatus struct {
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Started  time.Time `json:"started"`
}

// GetRunningJobs gets the status for all running jobs. It can be used to see
// what the job system is currently doing.
func (s *Scheduler) GetRunningJobs() []JobStatus {
	runningJobs := s.Runner.raffle.getRunningJobs()
	jobs := make([]JobStatus, 0)

	for k, v := range runningJobs {
		jobs = append(jobs, JobStatus{
			JobId:    k,
			JobTitle: v.title,
			Started:  v.started,
		})
	}
	return jobs
}

// GetRunningJob gets the status for a single running job. This can be used
// to see if a job is still running, and is currently used by the cli to follow
// a job run operation.
func (s *Scheduler) GetRunningJob(jobid string) *JobStatus {
	runningJob := s.Runner.raffle.runningJob(jobid)
	if runningJob == nil {
		return nil
	}
	return &JobStatus{
		JobId:    jobid,
		JobTitle: runningJob.title,
		Started:  runningJob.started,
	}

}

// GetJobHistory returns a list of history for all jobs that have ever been run on the server. It could be that in the
// future this will only return the history of the currently registered jobs.
// Each job stores its Start and End time, together with the last error if any.
func (s *Scheduler) GetJobHistory() []*jobResult {
	results := make([]*jobResult, 0)
	_ = s.Store.IterateObjectsRaw(server.JOB_RESULT_INDEX_BYTES, func(jsonData []byte) error {
		jobResult := &jobResult{}
		err := json.Unmarshal(jsonData, jobResult)

		if err != nil {
			s.Logger.Warnf(" > Error parsing job from store - aborting start: %s", err)
			return err
		}
		results = append(results, jobResult)

		return nil
	})
	return results
}

// PauseJob pauses a job. It will not stop a running job, but it will prevent the
// job from running on the next schedule.
func (s *Scheduler) PauseJob(jobid string) error {
	jobTitle, err := s.resolveJobTitle(jobid)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobid)
	}
	s.Logger.Infof("Pausing job with id %s (%s)", jobid, jobTitle)
	return s.changeStatus(jobid, true)
}

// UnpauseJob resumes a paused job. It will not run a job, however it will add it to
// the scheduler so that it can be ran on next schedule.
func (s *Scheduler) UnpauseJob(jobid string) error {
	jobTitle, err := s.resolveJobTitle(jobid)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobid)
	}
	s.Logger.Infof("Un-pausing job with id %s (%s)", jobid, jobTitle)
	return s.changeStatus(jobid, false)
}

// KillJob will stop a job stat is currently running. If the job is not running, it will do
// nothing. If the job that is running is RunOnce, then it will be deleted afterwards.
func (s *Scheduler) KillJob(jobid string) {
	jobTitle, err := s.resolveJobTitle(jobid)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobid)
	}
	s.Logger.Infof("Attempting to stop job with id %s (%s)", jobid, jobTitle)
	s.Runner.killJob(jobid)
}

// ResetJob will reset the job since token. This allows the job to be rerun from the beginning
func (s *Scheduler) ResetJob(jobid string, since string) error {
	jobTitle, err := s.resolveJobTitle(jobid)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobid)
	}
	s.Logger.Infof("Resetting since token for job with id '%s' (%s)", jobid, jobTitle)

	syncJobState := &SyncJobState{}
	err = s.Store.GetObject(server.JOB_DATA_INDEX, jobid, syncJobState)
	if err != nil {
		return err
	}

	if syncJobState.ID == "" {
		return nil
	}

	syncJobState.ContinuationToken = since
	err = s.Store.StoreObject(server.JOB_DATA_INDEX, jobid, syncJobState)
	if err != nil {
		return err
	}

	return nil
}

// RunJob runs an existing job, if not already running. It does so by adding a temp job to the scheduler, without saving it.
// The temp job is added with the RunOnce flag set to true
func (s *Scheduler) RunJob(jobid string, jobType string) (string, error) {
	jobTitle, err := s.resolveJobTitle(jobid)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobid)
	}
	s.Logger.Infof("Running job with id '%s' (%s)", jobid, jobTitle)

	jobConfig, err := s.LoadJob(jobid)
	if jobConfig == nil || jobConfig.Id == "" { // not found
		return "", errors.New("could not load job with id " + jobid)
	}

	if err != nil {
		return "", err
	}
	pipeline, err := s.toPipeline(jobConfig, jobType) // this also verifies that it can be parsed, so do this first
	if err != nil {
		return "", err
	}

	job := &job{
		id:       jobConfig.Id,
		title:    jobConfig.Title,
		pipeline: pipeline,
		runner:   s.Runner,
	}

	// is the job running?
	running := s.Runner.raffle.runningJob(jobConfig.Id)
	if running != nil {
		return "", errors.New(fmt.Sprintf("job with id '%s' (%s) already running", jobid, jobConfig.Title))
	}

	// start the job run
	s.Runner.startJob(job)

	return jobConfig.Id, nil
}

// changeStatus is the internal function to change status from/to paused/un-paused
func (s *Scheduler) changeStatus(jobid string, pause bool) error {
	jobConfig, err := s.LoadJob(jobid)
	if err != nil {
		return err
	}
	// just change the Paused flag, and add the job to the schedule.
	jobConfig.Paused = pause
	return s.AddJob(jobConfig)
}

// loadConfigurations is the internal method to load JobConfiguration's from the store, it is currently different
// from the needs of the ListJobs call, but I wanted to separate them.
func (s *Scheduler) loadConfigurations() []*JobConfiguration {
	jobConfigs := []*JobConfiguration{}

	_ = s.Store.IterateObjectsRaw(server.JOB_CONFIGS_INDEX_BYTES, func(jsonData []byte) error {
		jobConfig := &JobConfiguration{}
		err := json.Unmarshal(jsonData, jobConfig)

		if err != nil {
			s.Logger.Warnf(" > Error parsing job from store - aborting start: %s", err)
			return err
		}
		jobConfigs = append(jobConfigs, jobConfig)

		return nil
	})
	return jobConfigs
}

// verify makes sure a JobConfiguration is valid
func (s *Scheduler) verify(jobConfiguration *JobConfiguration) error {
	if len(jobConfiguration.Id) <= 0 {
		return errors.New("job configuration needs an id")
	}
	// we need to have at least 1 sink & 1 source
	if len(jobConfiguration.Source) <= 0 {
		return errors.New("you must configure a source")
	}
	if len(jobConfiguration.Sink) <= 0 {
		return errors.New("you must configure a sink")
	}
	if len(jobConfiguration.Triggers) <= 0 {
		return errors.New("Job Configuration needs at least 1 trigger")
	}
	for _, trigger := range jobConfiguration.Triggers {
		if _, ok := TriggerTypes[trigger.TriggerType]; !ok {
			return errors.New("need to set 'triggerType'. must be one of: cron, onchange")
		}
		if _, ok := JobTypes[trigger.JobType]; !ok {
			return errors.New("need to set 'jobType'. must be one of: fullsync, incremental")
		}
		if trigger.TriggerType == TriggerTypeOnChange {
			// if an event handler is given, that is ok in this context, so we just pass it on
			if trigger.MonitoredDataset != "" {
				return nil
			}
			return errors.New("trigger type 'onchange' requires that 'MonitoredDataset' parameter also is set")
		}

		_, err := cron.ParseStandard(trigger.Schedule)
		if err != nil {
			return errors.New("trigger type " + trigger.TriggerType + " requires a valid 'Schedule' expression. But: " + err.Error())
		}
	}
	return nil
}

// toPipeline converts the json in the JobConfiguration to concrete types.
// A Pipeline is basically a Source -> Transform -> Sink
func (s *Scheduler) toPipeline(jobConfig *JobConfiguration, jobType string) (Pipeline, error) {
	sink, err := s.parseSink(jobConfig)
	if err != nil {
		return nil, err
	}
	source, err := s.parseSource(jobConfig)
	if err != nil {
		return nil, err
	}

	transform, err := s.parseTransform(jobConfig)
	if err != nil {
		return nil, err
	}

	batchSize := jobConfig.BatchSize
	if batchSize < 1 {
		batchSize = defaultBatchSize // this is the default batch size
	}

	pipeline := PipelineSpec{
		source:    source,
		sink:      sink,
		transform: transform,
		batchSize: batchSize,
	}

	if jobType == JobTypeFull {
		return &FullSyncPipeline{pipeline}, nil
	} else {
		return &IncrementalPipeline{pipeline}, nil
	}
}

func (s *Scheduler) parseSource(jobConfig *JobConfiguration) (source.Source, error) {
	sourceConfig := jobConfig.Source
	if sourceConfig != nil {
		sourceTypeName := sourceConfig["Type"]
		if sourceTypeName != nil {
			if sourceTypeName == "HttpDatasetSource" {
				src := &source.HttpDatasetSource{}
				src.Store = s.Store
				src.Logger = s.Runner.logger.Named("HttpDatasetSource")
				endpoint, ok := sourceConfig["Url"]
				if ok && endpoint != "" {
					src.Endpoint = endpoint.(string)
				}
				tokenProviderRaw, ok := sourceConfig["TokenProvider"]
				if ok {
					tokenProviderName := tokenProviderRaw.(string)
					// security
					if tokenProviderName != "" {
						// attempt to parse the token provider
						provider, ok := s.Runner.tokenProviders.Providers[strings.ToLower(tokenProviderName)]
						if ok {
							src.TokenProvider = provider.(security.TokenProvider)
						}
					}
				}
				return src, nil
			} else if sourceTypeName == "DatasetSource" {
				src := &source.DatasetSource{}
				src.Store = s.Store
				src.DatasetManager = s.DatasetManager
				src.DatasetName = (sourceConfig["Name"]).(string)
				return src, nil
			} else if sourceTypeName == "MultiSource" {
				src := &source.MultiSource{}
				src.Store = s.Store
				src.DatasetManager = s.DatasetManager
				src.DatasetName = (sourceConfig["Name"]).(string)
				err := src.ParseDependencies(sourceConfig["Dependencies"])
				if err != nil {
					return nil, err
				}
				return src, nil
			} else if sourceTypeName == "SampleSource" {
				src := &source.SampleSource{}
				src.Store = s.Store
				numEntities := sourceConfig["NumberOfEntities"]
				if numEntities != nil {
					src.NumberOfEntities = int(numEntities.(float64))
				}
				return src, nil
			} else if sourceTypeName == "SlowSource" {
				src := &source.SlowSource{}
				src.Sleep = sourceConfig["Sleep"].(string)
				batch := sourceConfig["BatchSize"]
				if batch != nil {
					src.BatchSize = int(batch.(float64))
				}
				return src, nil
			} else {
				return nil, errors.New("unknown source type: " + sourceTypeName.(string))
			}
		}
		return nil, errors.New("missing source type")
	}
	return nil, errors.New("missing source config")

}

func (s *Scheduler) resolveJobTitle(jobId string) (string, error) {
	jobConfig, err := s.LoadJob(jobId)
	if err != nil {
		return "", err
	}
	return jobConfig.Title, nil
}
