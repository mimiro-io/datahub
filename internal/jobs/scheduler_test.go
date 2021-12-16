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
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bamzi/jobrunner"
	"github.com/robfig/cron/v3"

	"github.com/franela/goblin"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/zap"
)

var logger = zap.NewNop().Sugar()

func TestScheduler(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The Scheduler", func() {
		testCnt := 0
		var dsm *server.DsManager
		var scheduler *Scheduler
		var store *server.Store
		var runner *Runner
		var storeLocation string
		var statsdClient *StatsDRecorder
		g.BeforeEach(func() {
			testCnt += 1
			storeLocation = fmt.Sprintf("./testscheduler_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			scheduler, store, runner, dsm, statsdClient = setupScheduler(storeLocation, t)
		})
		g.AfterEach(func() {
			statsdClient.Reset()
			runner.Stop()
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})
		g.It("Should return a job's history", func() {
			sj, err := scheduler.Parse([]byte((`{
			"id" : "sync-samplesource-to-datasetsink",
			"title" : "sync-samplesource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"paused": true,
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities" : 50
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			}}`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")

			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			_, ok := runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsFalse("Does not have job id, because it is paused")

			//register callback to get notified when job is done
			wg := sync.WaitGroup{}
			wg.Add(2) // ticket is borrowed and returned = 2 callback calls
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				if data["name"] == "jobs.tickets.incr" {
					wg.Done()
				}
			}

			id, err := scheduler.RunJob(sj.Id, JobTypeIncremental)
			g.Assert(err).IsNil("RunJob returns no error")
			g.Assert(id).Eql(sj.Id, "Correct job id is returned from RunJob")

			wg.Wait()
			history := scheduler.GetJobHistory()
			g.Assert(len(history)).IsNotZero("We found something in the job history")
			g.Assert(history[0].Id).Eql(sj.Id, "History contains only our job in first place")
		})

		g.It("Should reset a job when asked to", func() {
			syncJobState := &SyncJobState{
				ID:                "job-1",
				ContinuationToken: "cont-token-123",
			}
			err := store.StoreObject(server.JOB_DATA_INDEX, "job-1", syncJobState)
			g.Assert(err).IsNil("We could store a syncJobState")

			err = scheduler.ResetJob("job-1", "hello-world")
			g.Assert(err).IsNil("We called ResetJob without error")

			s2 := &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, "job-1", s2)
			g.Assert(err).IsNil("We could load the syncJobState back")
			g.Assert(s2.ContinuationToken).Eql("hello-world",
				"We find the continuation token that was injected with ResetJob in the syncState")
		})

		g.It("Should kill a job when asked to", func() {
			//install a job that runs 50*100 ms (6 sec, exceeding goblins 5s timeout)
			sj, err := scheduler.Parse([]byte((`{
			"id" : "sync-slowsource-to-null",
			"title" : "sync-slowsource-to-null",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2h"}],
			"paused": false,
			"source" : {
				"Type" : "SlowSource",
				"BatchSize" : 60,
				"Sleep" : "100ms"
			},
			"sink" : {
				"Type" : "ConsoleSink"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			//register callback to get notified when job is started and done
			startWg := sync.WaitGroup{}
			startWg.Add(1)
			doneWg := sync.WaitGroup{}
			doneWg.Add(1)
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				//ticket is borrowed -> available ticket count decreases to 9
				if data["name"] == "jobs.tickets.incr" && data["value"] == float64(9) {
					startWg.Done()
				}
				//ticket is returned -> available ticket count back to 10
				if data["name"] == "jobs.tickets.incr" && data["value"] == float64(10) {
					doneWg.Done()
				}
			}

			_, err = scheduler.RunJob(sj.Id, JobTypeIncremental)
			g.Assert(err).IsNil()

			startWg.Wait()

			g.Assert(scheduler.GetRunningJob(sj.Id)).IsNotNil("Our job is running now")

			scheduler.KillJob(sj.Id)

			//wait until our job is not running anymore
			doneWg.Wait()
			g.Assert(scheduler.GetRunningJob(sj.Id)).IsNotNil("Our job is killed now")
		})

		g.It("Should pause a job when asked to", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-customer-from-adventure-works-to-datahub",
			"title" : "sync-customer-from-adventure-works-to-datahub",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:4343/datasets/customer"
			},
			"sink" : {
				"Type" : "DevNullSink"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			_, ok := runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsTrue("Our job is registered as schedule")

			err = scheduler.PauseJob(sj.Id)
			g.Assert(err).IsNil("we could call PauseJob without error")

			_, ok = runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsFalse("Our job is no longer registered as schedule")
		})

		g.It("Should unpause a job when asked to", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-customer-from-adventure-works-to-datahub",
			"title" : "sync-customer-from-adventure-works-to-datahub",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"paused": true,
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:4343/datasets/customer"
			},
			"sink" : {
				"Type" : "DevNullSink"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			_, ok := runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsFalse("Our job is not registered as schedule (paused in json)")

			err = scheduler.UnpauseJob(sj.Id)
			g.Assert(err).IsNil("we could call UnpauseJob without error")

			_, ok = runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsTrue("Our job is registered as schedule now")
		})
		g.It("Should immediately run a job when asked to", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-samplesource-to-datasetsink",
			"title" : "sync-samplesource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"paused": true,
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities" : 50
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			_, ok := runner.scheduledJobs[sj.Id]
			g.Assert(ok).IsFalse("Our job is not registered as schedule (paused in json)")

			_, _ = dsm.CreateDataset("People")

			//register callback to get notified when job is done
			wg := sync.WaitGroup{}
			wg.Add(2)
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				if data["name"] == "jobs.tickets.incr" {
					wg.Done()
				}
			}

			id, err := scheduler.RunJob(sj.Id, JobTypeIncremental)
			g.Assert(err).IsNil("We could invoke RunJob without error")
			g.Assert(id).Eql(sj.Id, "RunJob returned the correct job id")

			wg.Wait()

			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("We could read entities without error")
			g.Assert(len(result.Entities)).Eql(50, "We see all entities in the sink")
		})

		g.It("Should delete a job when asked to", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-samplesource-to-datasetsink",
			"title" : "sync-samplesource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities" : 1
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")

			job, err := scheduler.LoadJob(sj.Id)
			g.Assert(err).IsNil()
			g.Assert(job).IsNotNil()
			g.Assert(job.Id).Eql("sync-samplesource-to-datasetsink", "We could read back the job")
			_, ok := runner.scheduledJobs[job.Id]
			g.Assert(ok).IsTrue("our job is registered as schedule")

			err = scheduler.DeleteJob(job.Id)
			g.Assert(err).IsNil("Could invoke DeleteJob without error")

			job, err = scheduler.LoadJob(sj.Id)
			g.Assert(err).IsNil()
			g.Assert(job).IsNotNil() // not ideal detail - we get an empty object back if not found
			g.Assert(*job).IsZero("We get an empty configuration back, confirming deletion")
			_, ok = runner.scheduledJobs[job.Id]
			g.Assert(ok).IsFalse("our job is no longer registered as schedule")
		})

		g.It("Should accept jobs with both incremental and fullsync schedule", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-customer-from-adventure-works-to-datahub",
			"title" : "sync-customer-from-adventure-works-to-datahub",
			"triggers": [
                {"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"},
                {"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2h"}
            ],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:4343/datasets/customer"
			},
			"sink" : {
				"Type" : "DevNullSink"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			g.Assert(sj).IsNotNil("Could parse jobConfiguration json")
			g.Assert(sj.Id).Eql("sync-customer-from-adventure-works-to-datahub",
				"The produced configuration object is not empty")
			g.Assert(len(sj.Triggers)).Eql(2)
		})

		g.It("Should persist and reload job configuration after a restart", func() {
			sj, err := scheduler.Parse([]byte((` {
			"id" : "sync-samplesource-to-datasetsink",
			"title" : "sync-samplesource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities" : 1
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			} }`)))
			g.Assert(err).IsNil("Error free parsing of jobConfiguration json")
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil("Could add job to scheduler")
			g.Assert(runner.scheduledJobs[sj.Id]).Eql([]cron.EntryID{cron.EntryID(1)}, "Our job has received internal id 1")

			// close
			runner.Stop()
			err = store.Close()
			g.Assert(err).IsNil("We could close the datahub store without error")

			// reopen
			scheduler, store, runner, dsm, statsdClient = setupScheduler(storeLocation, t)
			g.Assert(runner.scheduledJobs[sj.Id]).Eql([]cron.EntryID{cron.EntryID(1)}, "Our job has received internal id 1")
		})

		g.It("Should marshal entities back and forth without data loss", func() {
			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity
			data, _ := json.Marshal(entities)

			var transformedEntities []*server.Entity
			err := json.Unmarshal(data, &transformedEntities)
			g.Assert(err).IsNil()
			g.Assert(len(transformedEntities)).Eql(1)
			g.Assert(*transformedEntities[0]).Eql(*entity,
				"after going through marshal and unmarshal, the result should equal the input")
		})

		g.Describe("Should handle concurrent run requests", func() {
			g.It("Should ignore RunJob (return an error message), if job is running", func() {
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "300ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					}}
				err := scheduler.AddJob(config)
				g.Assert(err).IsNil("could add job config without error")

				//add additional schedule which triggers "now"
				js, err := scheduler.toTriggeredJobs(config)
				g.Assert(err).IsNil()
				j := js[0]
				//register callback to get notified when job is started
				wg := sync.WaitGroup{}
				wg.Add(1) // we will never see the return of the borrowed ticket since the job is longrunning
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.incr" {
						wg.Done()
					}
				}

				id := jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
				g.Assert(id).Eql(cron.EntryID(2), "this should be the second schedule (same job though)")

				//wait for schedule to start
				wg.Wait()

				g.Assert(runner.raffle.runningJob(j.id).id).Eql(j.id, "scheduled job has started")
				runJobId, err := scheduler.RunJob(j.id, JobTypeIncremental)
				g.Assert(runJobId).IsZero()
				g.Assert(err).IsNotNil()
				g.Assert(err.Error()).Eql("job with id 'j1' (j1) already running")

				//Also try to shortcut "running" check in scheduler.RunJob. tickets still should prevent concurrent run
				g.Assert(len(scheduler.GetRunningJobs())).Eql(1, "there is one (scheduled) job running")
				originalStartTime := runner.raffle.runningJob(j.id).started
				scheduler.Runner.startJob(j)
				//ideally we need to wait for the job runner to go through raffle etc.
				g.Assert(runner.raffle.runningJob(j.id).started).Eql(originalStartTime, "runState did not change")
				scheduler.KillJob(j.id)
			})

			g.It("Should skip a scheduled run, while a RunJob is active", func() {
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "100ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					}}
				err := scheduler.AddJob(config)
				g.Assert(err).IsNil("could add job config without error")

				//add additional schedule which triggers "now"
				js, err := scheduler.toTriggeredJobs(config)
				g.Assert(err).IsNil()
				j := js[0]
				schedwg := sync.WaitGroup{}
				schedwg.Add(1)
				id := jobrunner.MainCron.Schedule(&TestSched{Callback: func() { schedwg.Done() }}, jobrunner.New(j))
				g.Assert(id).Eql(cron.EntryID(2), "this should be the second schedule (same job though)")

				//register callback to get notified when job is started and done
				wg := sync.WaitGroup{}
				wg.Add(1)
				donewg := sync.WaitGroup{}
				donewg.Add(1)
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					// gauge is decreased by 1 on start
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(4) {
						wg.Done()
					}
					// gauge is increased back to 5 on finish
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(5) {
						donewg.Done()
					}
				}
				runJobId, err := scheduler.RunJob(j.id, JobTypeFull)
				g.Assert(runJobId).Eql(j.id, "The RunJob has succeeded")
				g.Assert(err).IsNil()
				//wait for RunJob to reach running state
				wg.Wait()

				originalStartTime := runner.raffle.runningJob(j.id).started
				//wait for schedule to trigger another run
				schedwg.Wait()
				state := runner.raffle.runningJob(j.id)
				g.Assert(state.started).Eql(originalStartTime, "runState did not change")
				g.Assert(state).IsNotZero("there is a runstate")
				//wait for RunJob to finish (should run more 100ms)
				donewg.Wait()
				state = runner.raffle.runningJob(j.id)
				g.Assert(state).IsZero("no more runstate")
			})
			g.It("Should update the syncState and history of an incremental job after RunJob", func() {
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SampleSource"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					}}
				err := scheduler.AddJob(config)
				g.Assert(err).IsNil("could add job config without error")
				js, err := scheduler.toTriggeredJobs(config)
				g.Assert(err).IsNil()
				j := js[0]

				//register callback to get notified when job is done
				donewg := sync.WaitGroup{}
				donewg.Add(2)
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.incr" {
						donewg.Done()
					}
				}
				runJobId, err := scheduler.RunJob(j.id, JobTypeIncremental)
				g.Assert(runJobId).Eql(j.id, "The RunJob has succeeded")
				g.Assert(err).IsNil()

				donewg.Wait()

				hist := scheduler.GetJobHistory()
				g.Assert(len(hist)).Eql(1, "our RunJob is in history")
				syncJobState := &SyncJobState{}
				err = runner.store.GetObject(server.JOB_DATA_INDEX, j.id, syncJobState)
				g.Assert(err).IsNil()
				g.Assert(syncJobState).Eql(&SyncJobState{ID: j.id, ContinuationToken: "0"})
			})
			g.It("Should let a fullsync  RunJob fail, while a scheduled run is active (user can try again soon)", func() {
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "300ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					}}
				err := scheduler.AddJob(config)
				g.Assert(err).IsNil("could add job config without error")

				//add additional schedule which triggers "now"
				js, err := scheduler.toTriggeredJobs(config)
				g.Assert(err).IsNil()
				j := js[0]

				//register callback to get notified when job is started
				startWg := sync.WaitGroup{}
				startWg.Add(1)
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.incr" && data["value"] == float64(9) {
						startWg.Done()
					}
				}

				id := jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
				g.Assert(id).Eql(cron.EntryID(2), "this should be the second schedule (same job though)")

				//wait for schedule to start
				startWg.Wait()
				g.Assert(runner.raffle.runningJob(j.id).id).Eql(j.id, "scheduled job has started")

				//Now start a fullSync RunJob
				runJobId, err := scheduler.RunJob(j.id, JobTypeFull)
				g.Assert(runJobId).IsZero()
				g.Assert(err).IsNotNil()
				g.Assert(err.Error()).Eql("job with id 'j1' (j1) already running")

				//Also try to shortcut "running" check in scheduler.RunJob. tickets still should prevent concurrent run
				g.Assert(len(scheduler.GetRunningJobs())).Eql(1, "there is one (scheduled) job running")
				originalStartTime := runner.raffle.runningJob(j.id).started
				scheduler.Runner.startJob(j)
				g.Assert(runner.raffle.runningJob(j.id).started).Eql(originalStartTime, "runState did not change")
			})

			g.It("Should start a scheduled fullsync after a RunJob if the schedule triggers during RunJob", func() {
				_ = os.Setenv("JOB_FULLSYNC_RETRY_INTERVAL", "100ms")
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "200ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeFull, Schedule: "@midnight"},
					}}
				err := scheduler.AddJob(config)
				g.Assert(err).IsNil("could add job config without error")

				//add additional schedule which triggers "now"
				js, err := scheduler.toTriggeredJobs(config)
				g.Assert(err).IsNil()
				j := js[0]

				//register callback to get notified when job is started and done
				startWg := sync.WaitGroup{}
				startWg.Add(1)
				var started bool
				doneWg := sync.WaitGroup{}
				doneWg.Add(1)
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(4) && !started {
						started = true
						startWg.Done()
					}
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(5) && started {
						doneWg.Done()
					}
				}

				schedWg := sync.WaitGroup{}
				schedWg.Add(1)
				id := jobrunner.MainCron.Schedule(&TestSched{Callback: func() { schedWg.Done() }}, jobrunner.New(j))
				g.Assert(id).Eql(cron.EntryID(2), "this should be the second schedule (same job though)")

				runJobId, err := scheduler.RunJob(j.id, JobTypeFull)
				g.Assert(runJobId).Eql(j.id, "The RunJob has succeeded")
				g.Assert(err).IsNil()
				//wait for RunJob to reach running state
				startWg.Wait()
				//capture startingTime of RunJob for comparison
				originalStartTime := runner.raffle.runningJob(j.id).started

				//wait for schedule to trigger another run
				schedWg.Wait()

				state := runner.raffle.runningJob(j.id)
				g.Assert(state.started).Eql(originalStartTime, "runState should not change")
				g.Assert(state).IsNotZero("there should be a runstate")

				//wait for RunJob to finish
				doneWg.Wait()

				state = runner.raffle.runningJob(j.id)
				g.Assert(state).IsZero("no more runstate")

				startWg.Add(1)
				started = false
				// now wait for fullsync retry
				startWg.Wait()

				state = runner.raffle.runningJob(j.id)
				g.Assert(state).IsNotZero("should be a new runstate")
				g.Assert(state.started == originalStartTime).IsFalse("should not be same runstate as RunJob")

				_ = os.Unsetenv("JOB_FULLSYNC_RETRY_INTERVAL")
			})

			g.It("Should only queue up one scheduled run during ongoing jobrun", func() {
				// count number of queued up jobs via statsD
				var backPressureCnt int32 = 0
				statsdClient.CountCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.backpressure" {
						atomic.AddInt32(&backPressureCnt, 1)
					}
				}

				// get a doneWg signal when the first jobrun is finished
				doneWg := sync.WaitGroup{}
				doneWg.Add(1)
				var started bool
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(4) {
						//gauge goes down, a "start"
						started = true
					}
					if data["name"] == "jobs.tickets.full" && data["value"] == float64(5) && started {
						//gauge goes up again, a "done"
						doneWg.Done()
					}
				}

				_ = os.Setenv("JOB_FULLSYNC_RETRY_INTERVAL", "100ms")
				config := &JobConfiguration{
					Id:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "200ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeFull, Schedule: "@every 100ms"},
					}}

				// add the job to run it
				scheduler.AddJob(config)

				// get handle to same job config
				js, _ := scheduler.toTriggeredJobs(config)
				j := js[0]

				// add a 3 more scheduled runs (TestSched triggers after 100ms, well within slowSource duration)
				jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
				jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
				jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))

				// wait for first jobRun to finish
				doneWg.Wait()

				// make sure only 1 additional run was queued up
				g.Assert(backPressureCnt).Eql(1,
					"while the job was running, max one additional run should have queued up")
			})
		})

		g.Describe("Should validate job configuration", func() {
			validSource := map[string]interface{}{"Type": "SampleSource"}
			validSink := map[string]interface{}{"Type": "DevNullSink"}

			g.It("Should fail if ID is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{})
				g.Assert(err).Eql(errors.New("job configuration needs an id"))
			})

			g.It("Should fail if source is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x"})
				g.Assert(err).Eql(errors.New("you must configure a source"))
			})

			g.It("Should fail if sink is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{
					Id:     "x",
					Source: validSource,
				})
				g.Assert(err).Eql(errors.New("you must configure a sink"))
			})

			g.It("Should fail if TriggerType is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{
					Id:       "x",
					Triggers: []JobTrigger{{}},
					Source:   validSource,
					Sink:     validSink,
				})
				g.Assert(err).Eql(errors.New("need to set 'triggerType'. must be one of: cron, onchange"))
			})

			g.It("Should fail if JobType is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron},
					},
				})
				g.Assert(err).Eql(errors.New("need to set 'jobType'. must be one of: fullsync, incremental"))
			})

			g.It("Should fail if trigger type is unknown", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: "foo"},
					},
				})
				g.Assert(err).Eql(errors.New("need to set 'triggerType'. must be one of: cron, onchange"))
			})
			g.It("Should fail if sync type is unknown", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: "foo"},
					},
				})
				g.Assert(err).Eql(errors.New("need to set 'jobType'. must be one of: fullsync, incremental"))
			})

			g.It("Should fail if schedule is missing", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental},
					},
				})
				g.Assert(err).Eql(errors.New("trigger type cron requires a valid 'Schedule' expression. But: empty spec string"))

			})

			g.It("Should fail if source is unknown type", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x",
					Source: map[string]interface{}{"Type": "foo"},
					Sink:   validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					},
				})
				g.Assert(err).Eql(errors.New("unknown source type: foo"))
			})

			g.It("Should fail if sink is unknown type", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource,
					Sink: map[string]interface{}{"Type": "foo"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					},
				})
				g.Assert(err).Eql(errors.New("unknown sink type: foo"))
			})

			g.It("Should fail if transform is unknown type", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeOnChange, JobType: JobTypeIncremental, MonitoredDataset: "foo"},
					},
					Transform: map[string]interface{}{},
				})
				g.Assert(err).Eql(errors.New("transform config must contain 'Type'. can be one of: JavascriptTransform, HttpTransform"))
			})

			g.It("Should fail if job type is 'event', but schedule is set", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeOnChange, JobType: JobTypeIncremental, Schedule: "@midnight"},
					},
				})
				g.Assert(err).Eql(errors.New("trigger type 'onchange' requires that 'MonitoredDataset' parameter also is set"))
			})

			g.It("Should fail if schedule is not parsable", func() {
				err := scheduler.AddJob(&JobConfiguration{Id: "x", Source: validSource, Sink: validSink,
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "midnight"},
					},
				})
				g.Assert(err).Eql(errors.New("trigger type cron requires a valid 'Schedule' expression. But: expected exactly 5 fields, found 1: [midnight]"))

			})
		})
	})
}

type TestSched struct {
	started  bool
	Callback func()
}

// test sched: run once after 100ms, and then every 100 hours
func (s *TestSched) Next(t time.Time) time.Time {
	if !s.started {
		s.started = true
		if s.Callback != nil {
			s.Callback()
		}
		return time.Now().Add(100 * time.Millisecond)
	} else {
		return t.Add(100 * time.Hour)
	}
}

type Continuation struct {
	Id    string `json:"id"`
	Token string `json:"token"`
}

type MockService struct {
	RecordedEntities        map[string][]*server.Entity
	HttpNotificationChannel chan string
	echo                    *echo.Echo
}

func (m MockService) getContinuationTokenForDataset(dsName string) string {
	token := ""
	for _, e := range m.RecordedEntities[dsName] {
		if e.ID == "@continuation" {
			//continue so that we find the last token
			token = fmt.Sprintf("%v", e.Properties)
		}
	}
	return token
}

func (m MockService) getRecordedEntitiesForDataset(dsName string) []*server.Entity {
	allEntities := m.RecordedEntities[dsName]
	var readEntities []*server.Entity
	for _, e := range allEntities {
		if e.ID != "@context" {
			readEntities = append(readEntities, e)
		}
	}
	return readEntities
}

func NewMockService() MockService {
	e := echo.New()
	result := MockService{}
	result.RecordedEntities = make(map[string][]*server.Entity)
	result.HttpNotificationChannel = make(chan string, 100)
	result.echo = e
	// attach middleware to wrap every handler with channel notifications
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// after handler (right before response is sent), notify channel
			c.Response().Before(func() {
				result.HttpNotificationChannel <- fmt.Sprintf("%v", c.Request())
			})
			return next(c)
		}
	})
	e.HideBanner = true

	ctx := make(map[string]interface{})
	ctx["id"] = "@context"
	ns := make(map[string]string)
	ns["ex"] = "http://example.mimiro.io/"
	ns["_"] = "http://default.mimiro.io/"
	ctx["namespaces"] = ns

	e.GET("/datasets/people/changes", func(context echo.Context) error {
		result := make([]interface{}, 0)
		result = append(result, ctx)

		// add some objects
		for i := 0; i < 10; i++ {
			e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
			result = append(result, e)
		}
		return context.JSON(http.StatusOK, result)
	})

	e.GET("/datasets/people/changeswithcontinuation", func(context echo.Context) error {
		result := make([]interface{}, 0)
		result = append(result, ctx)

		// check for since
		since := context.QueryParam("since")
		if since == "" {
			// add some objects
			for i := 10; i < 20; i++ {
				e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
				result = append(result, e)
			}
			c := &Continuation{Id: "@continuation", Token: "nextplease"}
			result = append(result, c)
		} else {
			// return more objects
			for i := 0; i < 10; i++ {
				e := server.NewEntity("ex:e-"+strconv.Itoa(i), 0)
				result = append(result, e)
			}
		}

		return context.JSON(http.StatusOK, result)
	})

	e.POST("/transforms/identity", func(context echo.Context) error {
		body, err := ioutil.ReadAll(context.Request().Body)
		if err != nil {
			return err
		}
		return context.JSONBlob(http.StatusOK, body)
	})

	e.POST("/datasets/writeabledevnull", func(context echo.Context) error {
		body, err := ioutil.ReadAll(context.Request().Body)
		if err != nil {
			return err
		}

		var entities []*server.Entity
		err = json.Unmarshal(body, &entities)
		if err != nil {
			return err
		}

		return context.NoContent(http.StatusOK)
	})

	e.POST("/datasets/:name/fullsync", func(context echo.Context) error {
		body, err := ioutil.ReadAll(context.Request().Body)
		datasetName := context.Param("name")
		if err != nil {
			return err
		}

		var entities []*server.Entity
		err = json.Unmarshal(body, &entities)
		if err != nil {
			return err
		}
		result.RecordedEntities[datasetName] = append(result.RecordedEntities[datasetName], entities...)
		return context.NoContent(http.StatusOK)
	})

	return result
}

func setupScheduler(storeLocation string, t *testing.T) (*Scheduler, *server.Store, *Runner, *server.DsManager, *StatsDRecorder) {
	statsdClient := &StatsDRecorder{}
	statsdClient.Reset()
	e := &conf.Env{
		Logger:        logger,
		StoreLocation: storeLocation,
	}

	eb := server.NoOpBus()

	// temp redirect of stdout and stderr to swallow some annoying init messages in fx and jobrunner and mockService
	devNull, _ := os.Open("/dev/null")
	oldErr := os.Stderr
	oldStd := os.Stdout
	os.Stderr = devNull
	os.Stdout = devNull
	lc := fxtest.NewLifecycle(t)
	store := server.NewStore(lc, e, statsdClient)

	runner := NewRunner(&RunnerConfig{
		PoolIncremental: 10,
		PoolFull:        5,
		Concurrent:      0,
	}, e, store, nil, eb, statsdClient)

	dsm := server.NewDsManager(lc, e, store, server.NoOpBus())

	s := NewScheduler(lc, e, store, dsm, runner)

	// undo redirect of stdout and stderr after successful init of fx and jobrunner
	os.Stderr = oldErr
	os.Stdout = oldStd
	err := lc.Start(context.Background())
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}

	return s, store, runner, dsm, statsdClient
}

type StatsDRecorder struct {
	Gauges         map[string]float64
	Counts         map[string]int64
	GaugesCallback func(map[string]interface{})
	CountCallback  func(map[string]interface{})
}

func (r *StatsDRecorder) Reset() {
	r.Gauges = make(map[string]float64)
	r.Counts = make(map[string]int64)
	r.GaugesCallback = nil
	r.CountCallback = nil
}

func (r *StatsDRecorder) Gauge(name string, value float64, tags []string, rate float64) error {
	r.Gauges[name] = value
	if r.GaugesCallback != nil {
		r.GaugesCallback(map[string]interface{}{
			"name":  name,
			"value": value,
			"tags":  tags,
			"rate":  rate,
		})
	}
	return nil
}

func (r *StatsDRecorder) Count(name string, value int64, tags []string, rate float64) error {
	//r.Counts[name] = value
	if r.CountCallback != nil {
		r.CountCallback(map[string]interface{}{
			"name":  name,
			"value": value,
			"tags":  tags,
			"rate":  rate,
		})
	}
	return nil
}

func (r *StatsDRecorder) Histogram(name string, value float64, tags []string, rate float64) error {
	return nil
}
func (r *StatsDRecorder) Distribution(name string, value float64, tags []string, rate float64) error {
	return nil
}
func (r *StatsDRecorder) Decr(name string, tags []string, rate float64) error { return nil }
func (r *StatsDRecorder) Incr(name string, tags []string, rate float64) error { return nil }
func (r *StatsDRecorder) Set(name string, value string, tags []string, rate float64) error {
	return nil
}
func (r *StatsDRecorder) Timing(name string, value time.Duration, tags []string, rate float64) error {
	return nil
}
func (r *StatsDRecorder) TimeInMilliseconds(name string, value float64, tags []string, rate float64) error {
	return nil
}
func (r *StatsDRecorder) Event(e *statsd.Event) error                { return nil }
func (r *StatsDRecorder) SimpleEvent(title, text string) error       { return nil }
func (r *StatsDRecorder) ServiceCheck(sc *statsd.ServiceCheck) error { return nil }
func (r *StatsDRecorder) SimpleServiceCheck(name string, status statsd.ServiceCheckStatus) error {
	return nil
}
func (r *StatsDRecorder) Close() error                          { return nil }
func (r *StatsDRecorder) Flush() error                          { return nil }
func (r *StatsDRecorder) SetWriteTimeout(d time.Duration) error { return nil }
