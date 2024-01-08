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

package server_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mustafaturan/bus"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/web"
)

var _ = Describe("The Eventbus", func() {
	testCnt := 0
	var storeLocation string
	var eventBus *server.MEventBus
	var store *server.Store
	var dsm *server.DsManager
	var scheduler *jobs.Scheduler
	var runner *jobs.Runner
	var webService *web.WebService
	var peopleDs *server.Dataset
	BeforeEach(func() {
		testCnt += 1
		// since we wire up our actual web server, we need to provide a file matching 'views/*html' to avoid errors
		_ = os.MkdirAll("./views", os.ModePerm)
		_, _ = os.Create("./views/x.html")

		storeLocation = fmt.Sprintf("./test_events_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		e := &conf.Config{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
			Port:          "25555",
			Auth:          &conf.AuthConfig{Middleware: "noop"},
			RunnerConfig:  &conf.RunnerConfig{PoolIncremental: 10, PoolFull: 5},
		}

		devNull, _ := os.Open("/dev/null")
		oldOut := os.Stdout
		os.Stdout = devNull

		// lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		store = server.NewStore(e, &statsd.NoOpClient{})
		newBus, _ := server.NewBus(&conf.Config{Logger: zap.NewNop().Sugar()})
		eventBus = newBus.(*server.MEventBus)
		dsm = server.NewDsManager(e, store, newBus)

		runner = jobs.NewRunner(
			e, store, nil, eventBus, &statsd.NoOpClient{})
		scheduler = jobs.NewScheduler(e, store, dsm, runner)

		serviceContext := &web.ServiceContext{}
		serviceContext.Store = store
		serviceContext.EventBus = newBus
		serviceContext.Env = e
		serviceContext.JobsScheduler = scheduler
		serviceContext.DatasetManager = dsm
		serviceContext.Logger = e.Logger
		serviceContext.Statsd = &statsd.NoOpClient{}
		webService, err = web.NewWebService(serviceContext)
		webService.Start(context.Background())

		// mw := web.NewMiddleware(lc, e, webHander, mockServer, nil)
		// web.NewDatasetHandler(lc, mockServer, e.Logger, mw, dsm, store, newBus, nil)
		// err = lc.Start(serviceContext.Background())
		// Expect(err).To(BeNil())

		os.Stdout = oldOut

		peopleDs, err = dsm.CreateDataset("people", nil)
		Expect(err).To(BeNil())
	})
	AfterEach(func() {
		runner.Stop()
		_ = webService.Stop(context.Background())
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
		_ = os.RemoveAll("./views")
	})

	It("Should be an MEventBus", func() {
		busType := reflect.TypeOf(eventBus)
		Expect(busType.String()).To(Equal("*server.MEventBus"))
	})

	It("Should have a topic for each existing dataset", func() {
		allTopics := eventBus.Bus.Topics()
		for _, dsn := range dsm.GetDatasetNames() {
			seen := false
			for _, registeredTopic := range allTopics {
				if registeredTopic == "dataset."+dsn.Name {
					seen = true
				}
			}
			Expect(seen).To(BeTrue())
		}
	})

	It("Should emit an event if entities are posted to /entitites", func(_ SpecContext) {
		var eventReceived bool
		var wg sync.WaitGroup
		wg.Add(1)
		eventBus.SubscribeToDataset("people", "*", func(e *bus.Event) {
			if e.Topic == "dataset.people" {
				eventReceived = true
				wg.Done()
			}
		})
		reader := strings.NewReader(`[
				{ "id" : "@context", "namespaces" : { "_" : "http://data.mimiro.io/core/" } },
				{ "id" : "homer" }
            ]`)

		_, err := http.Post("http://localhost:25555/datasets/people/entities", "application/json", reader)
		Expect(err).To(BeNil())
		wg.Wait()
		Expect(eventReceived).To(BeTrue())
	}, SpecTimeout(1*time.Minute))

	It("Should emit event on sink's topic when a job with datasetSink is done", func() {
		var eventReceived bool
		var wg sync.WaitGroup
		wg.Add(2)
		eventBus.SubscribeToDataset("people", "*", func(e *bus.Event) {
			if e.Topic == "dataset.people" {
				eventReceived = true
			}
			wg.Done()
		})
		sj, err := scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
				"paused": true,
				"source" : {
					"Type" : "SampleSource",
					"NumberOfEntities" : 1
				},
				"sink" : {
					"Type" : "DatasetSink",
					"Name": "people"
				}
			}`)))
		Expect(err).To(BeNil())
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil())

		_, err = scheduler.RunJob(sj.ID, jobs.JobTypeIncremental)
		Expect(err).To(BeNil())
		wg.Wait()
		Expect(eventReceived).To(BeTrue(), "Should have observed event for people dataset")
	})

	It("Should trigger jobs that listen on the dataset's topic", func() {
		// add a extra listener for this test to know when the job is done
		var wg sync.WaitGroup
		wg.Add(1)
		eventBus.SubscribeToDataset("people", "*", func(e *bus.Event) {
			if e.Topic == "dataset.peoplecopy" {
				wg.Done()
			}
		})

		// add data to people dataset
		err := peopleDs.StoreEntities([]*server.Entity{server.NewEntity("homer", 0)})
		Expect(err).To(BeNil())
		target, err := dsm.CreateDataset("peoplecopy", nil)
		Expect(err).To(BeNil())

		// setup job
		sj, err := scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "onchange", "jobType": "incremental", "monitoredDataset": "people"}],
				"source" : {
					"Type" : "DatasetSource",
					"Name": "people"
				},
				"sink" : {
					"Type" : "DatasetSink",
					"Name": "peoplecopy"
				}
			}`)))
		Expect(err).To(BeNil())
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil())

		// verify that target is empty before event
		res, err := target.GetEntities("", 1)
		Expect(err).To(BeNil())
		Expect(len(res.Entities)).To(Equal(0), "target dataset should be empty")

		// emit event on people dataset
		eventBus.Emit(context.Background(), "dataset.people", nil)

		wg.Wait()

		// verify that the job is done
		res, err = target.GetEntities("", 1)
		Expect(err).To(BeNil())
		Expect(len(res.Entities)).To(Equal(1), "Job should have copied entity to target dataset")
	})

	It("Should deregister an onChange job when it's triggerType is changed", func() {
		sj, err := scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "onchange", "jobType": "incremental", "monitoredDataset": "people"}],
				"source" : { "Type" : "SampleSource" },
				"sink" : { "Type" : "ConsoleSink" }
			}`)))

		Expect(err).To(BeNil())
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil())

		Expect(eventBus.Bus.HandlerKeys()).To(Equal([]string{"job1"}), "job1 should have a subscription now")
		Expect(len(scheduler.GetScheduleEntries().Entries)).To(BeZero(), "there should not be any cron jobs")

		sj, err = scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 24h"}],
				"source" : { "Type" : "SampleSource" },
				"sink" : { "Type" : "ConsoleSink" }
			}`)))
		Expect(err).To(BeNil())
		err = scheduler.AddJob(sj)
		Expect(err).To(BeNil())

		Expect(len(eventBus.Bus.HandlerKeys())).To(BeZero(), "job1 should be deregistered")
		Expect(len(scheduler.GetScheduleEntries().Entries)).NotTo(BeZero(), "there should be a cron job now")
	})
})
