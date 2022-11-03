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
	"github.com/mimiro-io/datahub/internal"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/mimiro-io/datahub/internal/web"

	"github.com/mimiro-io/datahub/internal/server"

	"github.com/mimiro-io/datahub/internal/jobs"

	"github.com/mustafaturan/bus"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/franela/goblin"
)

func TestEvents(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("The Eventbus", func() {
		testCnt := 0
		var storeLocation string
		var eventBus *server.MEventBus
		var store *server.Store
		var dsm *server.DsManager
		var scheduler *jobs.Scheduler
		var runner *jobs.Runner
		var mockServer *echo.Echo
		var peopleDs *server.Dataset
		g.BeforeEach(func() {
			testCnt += 1
			//since we wire up our actual web server, we need to provide a file matching 'views/*html' to avoid errors
			_ = os.MkdirAll("./views", os.ModePerm)
			_, _ = os.Create("./views/x.html")

			storeLocation = fmt.Sprintf("./test_events_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)

			e := &conf.Env{
				Logger:        zap.NewNop().Sugar(),
				StoreLocation: storeLocation,
				Port:          "5555",
				Auth:          &conf.AuthConfig{Middleware: "noop"},
				RunnerConfig:  &conf.RunnerConfig{PoolIncremental: 10, PoolFull: 5}}

			devNull, _ := os.Open("/dev/null")
			oldOut := os.Stdout
			os.Stdout = devNull

			lc := fxtest.NewLifecycle(internal.FxTestLog(t, false))
			store = server.NewStore(lc, e, &statsd.NoOpClient{})
			newBus, _ := server.NewBus(&conf.Env{Logger: zap.NewNop().Sugar()})
			eventBus = newBus.(*server.MEventBus)
			dsm = server.NewDsManager(lc, e, store, newBus)

			runner = jobs.NewRunner(
				e, store, nil, eventBus, &statsd.NoOpClient{})
			scheduler = jobs.NewScheduler(lc, e, store, dsm, runner)

			var webHander *web.WebHandler
			webHander, mockServer = web.NewWebServer(lc, e, e.Logger, &statsd.NoOpClient{})
			mw := web.NewMiddleware(lc, e, webHander, mockServer, web.NewAuthorizer(e, e.Logger, nil), nil)
			web.NewDatasetHandler(lc, mockServer, e.Logger, mw, dsm, store, newBus, nil)

			err = lc.Start(context.Background())
			g.Assert(err).IsNil()

			os.Stdout = oldOut

			peopleDs, err = dsm.CreateDataset("people", nil)
			g.Assert(err).IsNil()
		})
		g.AfterEach(func() {
			runner.Stop()
			_ = mockServer.Close()
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
			_ = os.RemoveAll("./views")
		})

		g.It("Should be an MEventBus", func() {
			busType := reflect.TypeOf(eventBus)
			g.Assert(busType.String()).Eql("*server.MEventBus")
		})

		g.It("Should have a topic for each existing dataset", func() {
			allTopics := eventBus.Bus.Topics()
			for _, dsn := range dsm.GetDatasetNames() {
				var seen = false
				for _, registeredTopic := range allTopics {
					if registeredTopic == "dataset."+dsn.Name {
						seen = true
					}
				}
				g.Assert(seen).IsTrue()
			}
		})

		g.It("Should emit an event if entities are posted to /entitites", func() {
			g.Timeout(10 * time.Minute)
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

			_, err := http.Post("http://localhost:5555/datasets/people/entities", "application/json", reader)
			g.Assert(err).IsNil()
			wg.Wait()
			g.Assert(eventReceived).IsTrue()
		})

		g.It("Should emit event on sink's topic when a job with datasetSink is done", func() {
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
			g.Assert(err).IsNil()
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil()

			_, err = scheduler.RunJob(sj.Id, jobs.JobTypeIncremental)
			g.Assert(err).IsNil()
			wg.Wait()
			g.Assert(eventReceived).IsTrue("Should have observed event for people dataset")
		})

		g.It("Should trigger jobs that listen on the dataset's topic", func() {
			//add a extra listener for this test to know when the job is done
			var wg sync.WaitGroup
			wg.Add(1)
			eventBus.SubscribeToDataset("people", "*", func(e *bus.Event) {
				if e.Topic == "dataset.peoplecopy" {
					wg.Done()
				}
			})

			//add data to people dataset
			err := peopleDs.StoreEntities([]*server.Entity{server.NewEntity("homer", 0)})
			g.Assert(err).IsNil()
			target, err := dsm.CreateDataset("peoplecopy", nil)
			g.Assert(err).IsNil()

			//setup job
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
			g.Assert(err).IsNil()
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil()

			//verify that target is empty before event
			res, err := target.GetEntities("", 1)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(0, "target dataset should be empty")

			// emit event on people dataset
			eventBus.Emit(context.Background(), "dataset.people", nil)

			wg.Wait()

			//verify that the job is done
			res, err = target.GetEntities("", 1)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(1, "Job should have copied entity to target dataset")
		})

		g.It("Should deregister an onChange job when it's triggerType is changed", func() {
			sj, err := scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "onchange", "jobType": "incremental", "monitoredDataset": "people"}],
				"source" : { "Type" : "SampleSource" },
				"sink" : { "Type" : "ConsoleSink" }
			}`)))

			g.Assert(err).IsNil()
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil()

			g.Assert(eventBus.Bus.HandlerKeys()).Eql([]string{"job1"}, "job1 should have a subscription now")
			g.Assert(len(scheduler.GetScheduleEntries().Entries)).IsZero("there should not be any cron jobs")

			sj, err = scheduler.Parse([]byte((`{
				"id" : "job1",
				"title" : "job1",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 24h"}],
				"source" : { "Type" : "SampleSource" },
				"sink" : { "Type" : "ConsoleSink" }
			}`)))
			g.Assert(err).IsNil()
			err = scheduler.AddJob(sj)
			g.Assert(err).IsNil()

			g.Assert(len(eventBus.Bus.HandlerKeys())).IsZero("job1 should be deregistered")
			g.Assert(len(scheduler.GetScheduleEntries().Entries)).IsNotZero("there should be a cron job now")
		})
	})
}
