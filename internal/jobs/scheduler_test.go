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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/bamzi/jobrunner"
	"github.com/labstack/echo/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
)

var logger = zap.NewNop().Sugar()

var _ = Describe("The Scheduler", func() {
	testCnt := 0
	var dsm *server.DsManager
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var storeLocation string
	var statsdClient *StatsDRecorder
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./testscheduler_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)
		scheduler, store, runner, dsm, statsdClient = setupScheduler(storeLocation)
	})
	AfterEach(func() {
		statsdClient.Reset()
		runner.Stop()
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	It("Should return a job's history", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")

		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		_, ok := runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeFalse(), "Does not have job id, because it is paused")

		// register callback to get notified when job is done
		wg := sync.WaitGroup{}
		wg.Add(2) // ticket is borrowed and returned = 2 callback calls
		statsdClient.GaugesCallback = func(data map[string]interface{}) {
			if data["name"] == "jobs.tickets.incr" {
				wg.Done()
			}
		}

		id, err := scheduler.RunJob(sj.ID, JobTypeIncremental)
		Expect(err).To(BeNil(), "RunJob returns no error")
		Expect(id).To(Equal(sj.ID), "Correct job id is returned from RunJob")

		wg.Wait()
		history := scheduler.GetJobHistory()
		Expect(len(history)).NotTo(BeZero(), "We found something in the job history")
		Expect(history[0].ID).To(Equal(sj.ID), "History contains only our job in first place")
	})

	It("Should reset a job when asked to", func() {
		syncJobState := &SyncJobState{
			ID:                "job-1",
			ContinuationToken: "cont-token-123",
		}
		err := store.StoreObject(server.JobDataIndex, "job-1", syncJobState)
		Expect(err).To(BeNil(), "We could store a syncJobState")

		err = scheduler.ResetJob("job-1", "hello-world")
		Expect(err).To(BeNil(), "We called ResetJob without error")

		s2 := &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, "job-1", s2)
		Expect(err).To(BeNil(), "We could load the syncJobState back")
		Expect(s2.ContinuationToken).To(Equal("hello-world"),
			"We find the continuation token that was injected with ResetJob in the syncState")
	})

	It("Should kill a job when asked to", func() {
		// install a job that runs 50*100 ms (6 sec, exceeding goblins 5s timeout)
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		// register callback to get notified when job is started and done
		startWg := sync.WaitGroup{}
		startWg.Add(1)
		doneWg := sync.WaitGroup{}
		doneWg.Add(1)
		statsdClient.GaugesCallback = func(data map[string]interface{}) {
			// ticket is borrowed -> available ticket count decreases to 9
			if data["name"] == "jobs.tickets.incr" && data["value"] == float64(9) {
				startWg.Done()
			}
			// ticket is returned -> available ticket count back to 10
			if data["name"] == "jobs.tickets.incr" && data["value"] == float64(10) {
				doneWg.Done()
			}
		}

		_, err = scheduler.RunJob(sj.ID, JobTypeIncremental)
		Expect(err).To(BeNil())

		startWg.Wait()

		Expect(scheduler.GetRunningJob(sj.ID)).NotTo(BeNil(), "Our job is running now")

		scheduler.KillJob(sj.ID)

		// wait until our job is not running anymore
		doneWg.Wait()
		Expect(scheduler.GetRunningJob(sj.ID)).To(BeNil(), "Our job is killed now")
	})

	It("Should pause a job when asked to", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		_, ok := runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeTrue(), "Our job is registered as schedule")

		err = scheduler.PauseJob(sj.ID)
		Expect(err).To(BeNil(), "we could call PauseJob without error")

		_, ok = runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeFalse(), "Our job is no longer registered as schedule")
	})

	It("Should unpause a job when asked to", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		_, ok := runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeFalse(), "Our job is not registered as schedule (paused in json)")

		err = scheduler.UnpauseJob(sj.ID)
		Expect(err).To(BeNil(), "we could call UnpauseJob without error")

		_, ok = runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeTrue(), "Our job is registered as schedule now")
	})
	It("Should immediately run a job when asked to", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		_, ok := runner.scheduledJobs[sj.ID]
		Expect(ok).To(BeFalse(), "Our job is not registered as schedule (paused in json)")

		_, _ = dsm.CreateDataset("People", nil)

		// register callback to get notified when job is done
		wg := sync.WaitGroup{}
		wg.Add(2)
		statsdClient.GaugesCallback = func(data map[string]interface{}) {
			if data["name"] == "jobs.tickets.incr" {
				wg.Done()
			}
		}

		id, err := scheduler.RunJob(sj.ID, JobTypeIncremental)
		Expect(err).To(BeNil(), "We could invoke RunJob without error")
		Expect(id).To(Equal(sj.ID), "RunJob returned the correct job id")

		wg.Wait()

		peopleDataset := dsm.GetDataset("People")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "We could read entities without error")
		Expect(len(result.Entities)).To(Equal(50), "We see all entities in the sink")
	})

	It("Should delete a job when asked to", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")

		job, err := scheduler.LoadJob(sj.ID)
		Expect(err).To(BeNil())
		Expect(job).NotTo(BeNil())
		Expect(job.ID).To(Equal("sync-samplesource-to-datasetsink"), "We could read back the job")
		_, ok := runner.scheduledJobs[job.ID]
		Expect(ok).To(BeTrue(), "our job is registered as schedule")

		err = scheduler.DeleteJob(job.ID)
		Expect(err).To(BeNil(), "Could invoke DeleteJob without error")

		job, err = scheduler.LoadJob(sj.ID)
		Expect(err).To(BeNil())
		Expect(job).NotTo(BeNil()) // not ideal detail - we get an empty object back if not found
		Expect(*job).To(BeZero(), "We get an empty configuration back, confirming deletion")
		_, ok = runner.scheduledJobs[job.ID]
		Expect(ok).To(BeFalse(), "our job is no longer registered as schedule")
	})

	It("Should accept jobs with both incremental and fullsync schedule", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		Expect(sj).NotTo(BeNil(), "Could parse jobConfiguration json")
		Expect(sj.ID).To(Equal("sync-customer-from-adventure-works-to-datahub"),
			"The produced configuration object is not empty")
		Expect(len(sj.Triggers)).To(Equal(2))
	})

	It("Should persist and reload job configuration after a restart", func() {
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
		Expect(err).To(BeNil(), "Error free parsing of jobConfiguration json")
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil(), "Could add job to scheduler")
		Expect(runner.scheduledJobs[sj.ID]).
			To(Equal([]cron.EntryID{cron.EntryID(1)}), "Our job has received internal id 1")

		// close
		runner.Stop()
		err = store.Close()
		Expect(err).To(BeNil(), "We could close the datahub store without error")

		// reopen
		scheduler, store, runner, dsm, statsdClient = setupScheduler(storeLocation)
		Expect(runner.scheduledJobs[sj.ID]).
			To(Equal([]cron.EntryID{cron.EntryID(1)}), "Our job has received internal id 1")
	})

	It("Should marshal entities back and forth without data loss", func() {
		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity
		data, _ := json.Marshal(entities)

		var transformedEntities []*server.Entity
		err := json.Unmarshal(data, &transformedEntities)
		Expect(err).To(BeNil())
		Expect(len(transformedEntities)).To(Equal(1))
		Expect(*transformedEntities[0]).To(Equal(*entity),
			"after going through marshal and unmarshal, the result should equal the input")
	})

	Describe("Should handle concurrent run requests", func() {
		It("Should ignore RunJob (return an error message), if job is running", func() {
			config := &JobConfiguration{
				ID:     "j1",
				Title:  "j1",
				Sink:   map[string]interface{}{"Type": "DevNullSink"},
				Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "300ms"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			}
			err := scheduler.AddJob(config)
			Expect(err).To(BeNil(), "could add job config without error")

			// add additional schedule which triggers "now"
			js, err := scheduler.toTriggeredJobs(config)
			Expect(err).To(BeNil())
			j := js[0]
			// register callback to get notified when job is started
			wg := sync.WaitGroup{}
			wg.Add(1) // we will never see the return of the borrowed ticket since the job is longrunning
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				if data["name"] == "jobs.tickets.incr" {
					wg.Done()
				}
			}

			id := jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
			Expect(id).To(Equal(cron.EntryID(2)), "this should be the second schedule (same job though)")

			// wait for schedule to start
			wg.Wait()

			Expect(runner.raffle.runningJob(j.id).id).To(Equal(j.id), "scheduled job has started")
			runJobID, err := scheduler.RunJob(j.id, JobTypeIncremental)
			Expect(runJobID).To(BeZero())
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal("job with id 'j1' (j1) already running"))

			// Also try to shortcut "running" check in scheduler.RunJob. tickets still should prevent concurrent run
			Expect(len(scheduler.GetRunningJobs())).To(Equal(1), "there is one (scheduled) job running")
			originalStartTime := runner.raffle.runningJob(j.id).started
			scheduler.Runner.startJob(j)
			// ideally we need to wait for the job runner to go through raffle etc.
			Expect(runner.raffle.runningJob(j.id).started).To(Equal(originalStartTime), "runState did not change")
			scheduler.KillJob(j.id)
		})

		It("Should skip a scheduled run, while a RunJob is active", func() {
			config := &JobConfiguration{
				ID:     "j1",
				Title:  "j1",
				Sink:   map[string]interface{}{"Type": "DevNullSink"},
				Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "100ms"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			}
			err := scheduler.AddJob(config)
			Expect(err).To(BeNil(), "could add job config without error")

			// add additional schedule which triggers "now"
			js, err := scheduler.toTriggeredJobs(config)
			Expect(err).To(BeNil())
			j := js[0]
			schedwg := sync.WaitGroup{}
			schedwg.Add(1)
			id := jobrunner.MainCron.Schedule(&TestSched{Callback: func() { schedwg.Done() }}, jobrunner.New(j))
			Expect(id).To(Equal(cron.EntryID(2)), "this should be the second schedule (same job though)")

			// register callback to get notified when job is started and done
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
			runJobID, err := scheduler.RunJob(j.id, JobTypeFull)
			Expect(runJobID).To(Equal(j.id), "The RunJob has succeeded")
			Expect(err).To(BeNil())
			// wait for RunJob to reach running state
			wg.Wait()

			originalStartTime := runner.raffle.runningJob(j.id).started
			// wait for schedule to trigger another run
			schedwg.Wait()
			state := runner.raffle.runningJob(j.id)
			Expect(state.started).To(Equal(originalStartTime), "runState did not change")
			Expect(state).NotTo(BeZero(), "there is a runstate")
			// wait for RunJob to finish (should run more 100ms)
			donewg.Wait()
			state = runner.raffle.runningJob(j.id)
			Expect(state).To(BeZero(), "no more runstate")
		})
		It("Should update the syncState and history of an incremental job after RunJob", func() {
			config := &JobConfiguration{
				ID:     "j1",
				Title:  "j1",
				Sink:   map[string]interface{}{"Type": "DevNullSink"},
				Source: map[string]interface{}{"Type": "SampleSource"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			}
			err := scheduler.AddJob(config)
			Expect(err).To(BeNil(), "could add job config without error")
			js, err := scheduler.toTriggeredJobs(config)
			Expect(err).To(BeNil())
			j := js[0]

			// register callback to get notified when job is done
			donewg := sync.WaitGroup{}
			donewg.Add(2)
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				if data["name"] == "jobs.tickets.incr" {
					donewg.Done()
				}
			}
			runJobID, err := scheduler.RunJob(j.id, JobTypeIncremental)
			Expect(runJobID).To(Equal(j.id), "The RunJob has succeeded")
			Expect(err).To(BeNil())

			donewg.Wait()

			hist := scheduler.GetJobHistory()
			Expect(len(hist)).To(Equal(1), "our RunJob is in history")
			syncJobState := &SyncJobState{}
			err = runner.store.GetObject(server.JobDataIndex, j.id, syncJobState)
			Expect(err).To(BeNil())
			Expect(syncJobState).To(Equal(&SyncJobState{ID: j.id, ContinuationToken: "0"}))
		})
		It(
			"Should let a fullsync  RunJob fail, while a scheduled run is active (user can try again soon)",
			func() {
				config := &JobConfiguration{
					ID:     "j1",
					Title:  "j1",
					Sink:   map[string]interface{}{"Type": "DevNullSink"},
					Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "300ms"},
					Triggers: []JobTrigger{
						{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
					},
				}
				err := scheduler.AddJob(config)
				Expect(err).To(BeNil(), "could add job config without error")

				// add additional schedule which triggers "now"
				js, err := scheduler.toTriggeredJobs(config)
				Expect(err).To(BeNil())
				j := js[0]

				// register callback to get notified when job is started
				startWg := sync.WaitGroup{}
				startWg.Add(1)
				statsdClient.GaugesCallback = func(data map[string]interface{}) {
					if data["name"] == "jobs.tickets.incr" && data["value"] == float64(9) {
						startWg.Done()
					}
				}

				id := jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
				Expect(id).To(Equal(cron.EntryID(2)), "this should be the second schedule (same job though)")

				// wait for schedule to start
				startWg.Wait()
				Expect(runner.raffle.runningJob(j.id).id).To(Equal(j.id), "scheduled job has started")

				// Now start a fullSync RunJob
				runJobID, err := scheduler.RunJob(j.id, JobTypeFull)
				Expect(runJobID).To(BeZero())
				Expect(err).NotTo(BeNil())
				Expect(err.Error()).To(Equal("job with id 'j1' (j1) already running"))

				// Also try to shortcut "running" check in scheduler.RunJob. tickets still should prevent concurrent run
				Expect(len(scheduler.GetRunningJobs())).To(Equal(1), "there is one (scheduled) job running")
				originalStartTime := runner.raffle.runningJob(j.id).started
				scheduler.Runner.startJob(j)
				Expect(runner.raffle.runningJob(j.id).started).To(Equal(originalStartTime), "runState did not change")
			},
		)

		It("Should start a scheduled fullsync after a RunJob if the schedule triggers during RunJob", func() {
			_ = os.Setenv("JOB_FULLSYNC_RETRY_INTERVAL", "100ms")
			config := &JobConfiguration{
				ID:     "j1",
				Title:  "j1",
				Sink:   map[string]interface{}{"Type": "DevNullSink"},
				Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "200ms"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeFull, Schedule: "@midnight"},
				},
			}
			err := scheduler.AddJob(config)
			Expect(err).To(BeNil(), "could add job config without error")

			// add additional schedule which triggers "now"
			js, err := scheduler.toTriggeredJobs(config)
			Expect(err).To(BeNil())
			j := js[0]

			// register callback to get notified when job is started and done
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
			Expect(id).To(Equal(cron.EntryID(2)), "this should be the second schedule (same job though)")

			runJobID, err := scheduler.RunJob(j.id, JobTypeFull)
			Expect(runJobID).To(Equal(j.id), "The RunJob has succeeded")
			Expect(err).To(BeNil())
			// wait for RunJob to reach running state
			startWg.Wait()
			// capture startingTime of RunJob for comparison
			originalStartTime := runner.raffle.runningJob(j.id).started

			// wait for schedule to trigger another run
			schedWg.Wait()

			state := runner.raffle.runningJob(j.id)
			Expect(state.started).To(Equal(originalStartTime), "runState should not change")
			Expect(state).NotTo(BeZero(), "there should be a runstate")

			// wait for RunJob to finish
			doneWg.Wait()

			state = runner.raffle.runningJob(j.id)
			Expect(state).To(BeZero(), "no more runstate")

			startWg.Add(1)
			started = false
			// now wait for fullsync retry
			startWg.Wait()

			state = runner.raffle.runningJob(j.id)
			Expect(state).NotTo(BeZero(), "should be a new runstate")
			Expect(state.started.Equal(originalStartTime)).To(BeFalse(), "should not be same runstate as RunJob")

			_ = os.Unsetenv("JOB_FULLSYNC_RETRY_INTERVAL")
		})

		It("Should only queue up one scheduled run during ongoing jobrun", func() {
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
			var done bool
			statsdClient.GaugesCallback = func(data map[string]interface{}) {
				if data["name"] == "jobs.tickets.full" && data["value"] == float64(4) {
					// gauge goes down, a "start"
					started = true
				}
				if data["name"] == "jobs.tickets.full" && data["value"] == float64(5) && started && !done {
					// gauge goes up again, a "done"
					doneWg.Done()
					done = true
				}
			}

			// set retry interval larger than job duration, so that subsequent retries do not mess with our backpressure counts.
			// we want to only measure the number queued jobs in parellel (not in series)
			// also set it larger than runtime of complete testsuite to avoid that it kicks in while other tests run
			_ = os.Setenv("JOB_FULLSYNC_RETRY_INTERVAL", "5m")
			config := &JobConfiguration{
				ID:     "j1",
				Title:  "j1",
				Sink:   map[string]interface{}{"Type": "DevNullSink"},
				Source: map[string]interface{}{"Type": "SlowSource", "Sleep": "200ms"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeFull, Schedule: "@every 100ms"},
				},
			}

			// add the job to run it
			scheduler.AddJob(config)

			// get handle to same job config
			js, _ := scheduler.toTriggeredJobs(config)
			j := js[0]

			// add a 30 more scheduled runs (TestSched triggers after 100ms, well within slowSource duration)
			for i := 0; i < 30; i++ {
				jobrunner.MainCron.Schedule(&TestSched{}, jobrunner.New(j))
			}

			// wait for first jobRun to finish
			doneWg.Wait()

			// make sure only 1 additional run was queued up
			Expect(int(backPressureCnt)).To(Equal(1),
				"while the job was running, max one additional run should have queued up")
		})
	})

	Describe("Should validate job configuration", func() {
		validSource := map[string]interface{}{"Type": "SampleSource"}
		validSink := map[string]interface{}{"Type": "DevNullSink"}

		It("Should fail if ID is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{})
			Expect(err).To(Equal(errors.New("job configuration needs an id")))
		})

		It("Should fail if title is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{ID: "x"})
			Expect(err).To(Equal(errors.New("job configuration needs a title")))
		})

		It("Should fail if source is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{ID: "x", Title: "x"})
			Expect(err).To(Equal(errors.New("you must configure a source")))
		})

		It("Should fail if sink is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID:     "x",
				Title:  "x",
				Source: validSource,
			})
			Expect(err).To(Equal(errors.New("you must configure a sink")))
		})

		It("Should fail if TriggerType is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID:       "x",
				Title:    "x",
				Triggers: []JobTrigger{{}},
				Source:   validSource,
				Sink:     validSink,
			})
			Expect(err).To(Equal(errors.New("need to set 'triggerType'. must be one of: cron, onchange")))
		})

		It("Should fail if JobType is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron},
				},
			})
			Expect(err).To(Equal(errors.New("need to set 'jobType'. must be one of: fullsync, incremental")))
		})

		It("Should fail if trigger type is unknown", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: "foo"},
				},
			})
			Expect(err).To(Equal(errors.New("need to set 'triggerType'. must be one of: cron, onchange")))
		})
		It("Should fail if sync type is unknown", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: "foo"},
				},
			})
			Expect(err).To(Equal(errors.New("need to set 'jobType'. must be one of: fullsync, incremental")))
		})

		It("Should fail if schedule is missing", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental},
				},
			})
			Expect(err).
				To(Equal(errors.New("trigger type cron requires a valid 'Schedule' expression. But: empty spec string")))
		})

		It("Should fail if source is unknown type", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x",
				Source: map[string]interface{}{"Type": "foo"},
				Sink:   validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			})
			Expect(err).To(Equal(errors.New("unknown source type: foo")))
		})

		It("Should fail if sink is unknown type", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource,
				Sink: map[string]interface{}{"Type": "foo"},
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			})
			Expect(err).To(Equal(errors.New("unknown sink type: foo")))
		})

		It("Should fail if transform is unknown type", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeOnChange, JobType: JobTypeIncremental, MonitoredDataset: "foo"},
				},
				Transform: map[string]interface{}{},
			})
			Expect(err).
				To(Equal(errors.New("transform config must contain 'Type'. can be one of: JavascriptTransform, HttpTransform")))
		})

		It("Should fail if job type is 'event', but schedule is set", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeOnChange, JobType: JobTypeIncremental, Schedule: "@midnight"},
				},
			})
			Expect(err).
				To(Equal(errors.New("trigger type 'onchange' requires that 'MonitoredDataset' parameter also is set")))
		})

		It("Should fail if schedule is not parsable", func() {
			err := scheduler.AddJob(&JobConfiguration{
				ID: "x", Title: "x", Source: validSource, Sink: validSink,
				Triggers: []JobTrigger{
					{TriggerType: TriggerTypeCron, JobType: JobTypeIncremental, Schedule: "midnight"},
				},
			})
			Expect(err).
				To(Equal(errors.New("trigger type cron requires a valid 'Schedule' expression. But: expected exactly 5 fields, found 1: [midnight]")))
		})
	})
})

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
	ID    string `json:"id"`
	Token string `json:"token"`
}

type MockService struct {
	RecordedEntities        map[string][]*server.Entity
	HTTPNotificationChannel chan *http.Request
	echo                    *echo.Echo
}

func (m MockService) getContinuationTokenForDataset(dsName string) string {
	token := ""
	for _, e := range m.RecordedEntities[dsName] {
		if e.ID == "@continuation" {
			// continue so that we find the last token
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
	result.HTTPNotificationChannel = make(chan *http.Request, 100)
	result.echo = e
	// attach middleware to wrap every handler with channel notifications
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// after handler (right before response is sent), notify channel
			c.Response().Before(func() {
				result.HTTPNotificationChannel <- c.Request()
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
	e.GET("/datasets/people/entities", func(context echo.Context) error {
		result := make([]interface{}, 0)
		result = append(result, ctx)

		// add some objects
		for i := 0; i < 10; i++ {
			e := server.NewEntity("ex:fs-"+strconv.Itoa(i), 0)
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
			c := &Continuation{ID: "@continuation", Token: "nextplease"}
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
	e.POST("/datasets/people/entities", func(context echo.Context) error {
		body, err := ioutil.ReadAll(context.Request().Body)
		if err != nil {
			return err
		}

		var entities []*server.Entity
		err = json.Unmarshal(body, &entities)
		if err != nil {
			return err
		}
		datasetName := "people"
		result.RecordedEntities[datasetName] = append(result.RecordedEntities[datasetName], entities...)
		return context.NoContent(http.StatusOK)
	})

	return result
}

func setupScheduler(storeLocation string) (*Scheduler, *server.Store, *Runner, *server.DsManager, *StatsDRecorder) {
	GinkgoHelper()
	statsdClient := &StatsDRecorder{}
	statsdClient.Reset()
	e := &conf.Env{
		Logger:        logger,
		StoreLocation: storeLocation,
		RunnerConfig: &conf.RunnerConfig{
			PoolIncremental: 10,
			PoolFull:        5,
			Concurrent:      0,
		},
	}

	eb := server.NoOpBus()

	// temp redirect of stdout to swallow some annoying init messages in fx and jobrunner and mockService
	devNull, _ := os.Open("/dev/null")
	oldStd := os.Stdout
	os.Stdout = devNull
	// lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
	store := server.NewStore(e, statsdClient)

	pm := security.NewProviderManager(e, store, logger)
	tps := security.NewTokenProviders(logger, pm, nil)
	runner := NewRunner(e, store, tps, eb, statsdClient)
	dsm := server.NewDsManager(e, store, server.NoOpBus())
	s := NewScheduler(e, store, dsm, runner)

	// undo redirect of stdout after successful init of fx and jobrunner
	os.Stdout = oldStd
	// err := lc.Start(context.Background())
	//if err != nil {
	// 	Fail(err.Error())
	// }
	// add basic auth provider called local
	tps.Add(security.ProviderConfig{
		Name:     "local",
		Type:     "basic",
		User:     &security.ValueReader{Type: "text", Value: "u100"},
		Password: &security.ValueReader{Type: "text", Value: "p200"},
	})

	return s, store, runner, dsm, statsdClient
}

type StatsDRecorder struct {
	Gauges         map[string]float64
	Counts         map[string]int64
	GaugesCallback func(map[string]interface{})
	CountCallback  func(map[string]interface{})
}

func (r *StatsDRecorder) GaugeWithTimestamp(
	name string,
	value float64,
	tags []string,
	rate float64,
	timestamp time.Time,
) error {
	return nil
}

func (r *StatsDRecorder) CountWithTimestamp(
	name string,
	value int64,
	tags []string,
	rate float64,
	timestamp time.Time,
) error {
	return nil
}

func (r *StatsDRecorder) IsClosed() bool {
	return false
}

func (r *StatsDRecorder) GetTelemetry() statsd.Telemetry {
	return statsd.Telemetry{}
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
	// r.Counts[name] = value
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
