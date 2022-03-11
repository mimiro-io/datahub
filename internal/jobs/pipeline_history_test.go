// Copyright 2022 MIMIRO AS
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
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/franela/goblin"

	"github.com/mimiro-io/datahub/internal/server"
)

func TestPipelineHistory(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A pipeline", func() {
		testCnt := 0
		var dsm *server.DsManager
		var scheduler *Scheduler
		var store *server.Store
		var runner *Runner
		var storeLocation string
		var mockService MockService
		g.BeforeEach(func() {
			// temp redirect of stdout and stderr to swallow some annoying init messages in fx and jobrunner and mockService
			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			oldStd := os.Stdout
			os.Stderr = devNull
			os.Stdout = devNull

			testCnt += 1
			storeLocation = fmt.Sprintf("./testpipeline_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			mockService = NewMockService()
			go func() {
				_ = mockService.echo.Start(":7777")
			}()
			scheduler, store, runner, dsm, _ = setupScheduler(storeLocation, t)

			// undo redirect of stdout and stderr after successful init of fx and jobrunner
			os.Stderr = oldErr
			os.Stdout = oldStd

		})
		g.AfterEach(func() {
			runner.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_ = mockService.echo.Shutdown(ctx)
			cancel()
			mockService.HttpNotificationChannel = nil
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})
		g.It("Should produce consistent output for single change", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, _ := setupCheckers(g, t, employees, people, companies, ns)
			//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			// run job twice, output should not change
			job.Run()
			job.Run()

			checkEntities("homer_1", "Mimiro_1", "")
			checkChange(0, "homer_1", "Mimiro_1", "")
			assertChangeCount(1)

		})
		g.It("Should produce consistent output for changes in dependency dataset", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, _ := setupCheckers(g, t, employees, people, companies, ns)
			//checkChange, checkEntities, assertChangeCount, printState := setupCheckers(g, t, employees, people, companies, ns)

			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()
			//change company
			g.Assert(mimiro("Mimiro_2")).IsNil()

			// run job many times, output should not change
			for i := 0; i < 10; i++ {
				job.Run()
			}
			//printState()
			checkEntities("homer_1", "Mimiro_2", "")
			checkChange(0, "homer_1", "Mimiro_2", "")
			assertChangeCount(1)
		})
		g.It("Should produce consistent output for changes in source dataset", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			//checkChange, checkEntities, assertChangeCount, _ := setupCheckers(g, t, employees, people, companies, ns)
			checkChange, checkEntities, assertChangeCount, printState := setupCheckers(g, t, employees, people, companies, ns)

			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()
			//change homer
			g.Assert(homer("homer_2")).IsNil()

			// run job many times, output should not change
			for i := 0; i < 5; i++ {
				job.Run()
			}
			printState()
			checkEntities("homer_2", "Mimiro_1", "")
			checkChange(0, "homer_1", "Mimiro_1", "")
			checkChange(1, "homer_2", "Mimiro_1", "")
			checkChange(8, "homer_1", "Mimiro_1", "")
			checkChange(9, "homer_2", "Mimiro_1", "")
			assertChangeCount(10) //"2 changes in source, time 5 job runs"
		})
		g.It("Should produce consistent output for LastOnly changes in source dataset", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, true)
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			//checkChange, checkEntities, assertChangeCount, _ := setupCheckers(g, t, employees, people, companies, ns)
			checkChange, checkEntities, assertChangeCount, printState := setupCheckers(g, t, employees, people, companies, ns)

			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()
			//change homer
			g.Assert(homer("homer_2")).IsNil()

			// run job many times, output should not change
			for i := 0; i < 5; i++ {
				job.Run()
			}
			printState()
			checkEntities("homer_2", "Mimiro_1", "")
			assertChangeCount(1) //only one version of homer is expected to be emitted, therefore only one resulting change version
			checkChange(0, "homer_2", "Mimiro_1", "")
		})
		g.It("Should produce consistent output for 2 changes in different batches", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			// set batchsize to 2
			job.pipeline.(*FullSyncPipeline).batchSize = 2
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, print := setupCheckers(g, t, employees, people, companies, ns)
			//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()
			g.Assert(homer("homer_2")).IsNil()
			g.Assert(mimiro("Mimiro_2")).IsNil()
			g.Assert(homer("homer_3")).IsNil()

			// run job many times, output should not change
			for i := 0; i < 5; i++ {
				job.Run()
			}
			print()
			checkEntities("homer_3", "Mimiro_2", "")
			checkChange(0, "homer_1", "Mimiro_2", "")
			checkChange(1, "homer_2", "Mimiro_2", "")
			checkChange(2, "homer_3", "Mimiro_2", "")
			checkChange(12, "homer_1", "Mimiro_2", "")
			checkChange(13, "homer_2", "Mimiro_2", "")
			checkChange(14, "homer_3", "Mimiro_2", "")
			assertChangeCount(15) //"3 changes in source, time 5 job runs"

		})
		g.It("Should produce consistent output for 2 LastOnly changes in different batches", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, true)
			// set batchsize to 2
			job.pipeline.(*FullSyncPipeline).batchSize = 2
			homer, _, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, print := setupCheckers(g, t, employees, people, companies, ns)
			//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()
			g.Assert(homer("homer_2")).IsNil()
			g.Assert(mimiro("Mimiro_2")).IsNil()
			g.Assert(homer("homer_3")).IsNil()

			// run job many times, output should not change
			for i := 0; i < 5; i++ {
				job.Run()
			}
			print()
			checkEntities("homer_3", "Mimiro_2", "")
			checkChange(0, "homer_3", "Mimiro_2", "")
			assertChangeCount(1) //since only one "Latest" version of homer is emitted, we expect only one result

		})
		g.It("Should preserve state of composed change products also if sources change with time", func() {
			/*
				Here we simulate changes in two datasets over time, and verify how these changes affect the results
				of transforms which use Query to join both datasets.

				As baseline we have a person, homer, and a company, Mimiro. a transform job joins Mimiro onto homer and produces
				an entity with data from both source entities.

				With the baseline set, we add a couple of changes to both homer and Mimiro, which each should produce a new version
				of the joined "employee" entity if we run the transform again.
			*/
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, print := setupCheckers(g, t, employees, people, companies, ns)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			// now, first run with baseline
			job.Run()

			checkEntities("homer_1", "Mimiro_1", "")
			assertChangeCount(1)
			checkChange(0, "homer_1", "Mimiro_1", "")

			///////////// first change: homer new name, add friend /////////////////
			g.Assert(homerWithFriend("homer_2")).IsNil()
			job.Run()

			checkEntities("homer_2", "Mimiro_1", "barney")
			assertChangeCount(2)
			checkChange(0, "homer_1", "Mimiro_1", "")
			checkChange(1, "homer_2", "Mimiro_1", "barney")

			//////////// 2nd change: Mimiro new name //////////////////
			g.Assert(mimiro("Mimiro_2")).IsNil()
			job.Run()
			/////////// 3rd change: homer new name again, rm friend
			g.Assert(homer("homer_3")).IsNil()
			job.Run()
			/////////// 4th change: homer back to old name
			g.Assert(homer("homer_1")).IsNil()
			job.Run()
			/////////// 5th change: Mimiro new name
			g.Assert(mimiro("Mimiro_3")).IsNil()
			job.Run()
			/////////// 6th change: homer  final change
			g.Assert(homer("homer_4")).IsNil()
			job.Run()
			/////////// 7th change: mimiro  final change
			g.Assert(mimiro("Mimiro_4")).IsNil()
			job.Run()

			print()
			checkEntities("homer_4", "Mimiro_4", "")
			assertChangeCount(24)
			checkChange(0, "homer_1", "Mimiro_1", "")
			checkChange(1, "homer_2", "Mimiro_1", "barney")
			checkChange(2, "homer_1", "Mimiro_2", "")
			checkChange(3, "homer_2", "Mimiro_2", "barney")
			checkChange(23, "homer_4", "Mimiro_4", "")
		})
		g.It("Should preserve state of composed LatestOnly change products also if sources change with time", func() {
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, true)
			homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, print := setupCheckers(g, t, employees, people, companies, ns)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			// now, first run with baseline
			job.Run()

			checkEntities("homer_1", "Mimiro_1", "")
			assertChangeCount(1)
			checkChange(0, "homer_1", "Mimiro_1", "")

			///////////// first change: homer new name, add friend /////////////////
			g.Assert(homerWithFriend("homer_2")).IsNil()
			job.Run()

			checkEntities("homer_2", "Mimiro_1", "barney")
			assertChangeCount(2)
			checkChange(0, "homer_1", "Mimiro_1", "")
			checkChange(1, "homer_2", "Mimiro_1", "barney")

			//////////// 2nd change: Mimiro new name //////////////////
			g.Assert(mimiro("Mimiro_2")).IsNil()
			job.Run()
			/////////// 3rd change: homer new name again, rm friend
			g.Assert(homer("homer_3")).IsNil()
			job.Run()
			/////////// 4th change: homer back to old name
			g.Assert(homer("homer_1")).IsNil()
			job.Run()
			/////////// 5th change: Mimiro new name
			g.Assert(mimiro("Mimiro_3")).IsNil()
			job.Run()
			/////////// 6th change: homer  final change
			g.Assert(homer("homer_4")).IsNil()
			job.Run()
			/////////// 7th change: mimiro  final change
			g.Assert(mimiro("Mimiro_4")).IsNil()
			job.Run()

			print()
			checkEntities("homer_4", "Mimiro_4", "")
			assertChangeCount(8)
			checkChange(0, "homer_1", "Mimiro_1", "")
			checkChange(1, "homer_2", "Mimiro_1", "barney")
			checkChange(2, "homer_2", "Mimiro_2", "barney")
			checkChange(3, "homer_3", "Mimiro_2", "")
			checkChange(4, "homer_1", "Mimiro_2", "")
			checkChange(5, "homer_1", "Mimiro_3", "")
			checkChange(6, "homer_4", "Mimiro_3", "")
			checkChange(7, "homer_4", "Mimiro_4", "")
		})
		g.It("Should compose existing changes correctly in transform with Query", func() {
			/*
				  This is the same order of changes as above, except for that we only run the
					transform job once at the end.
			*/
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner, false)
			homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
			checkChange, checkEntities, assertChangeCount, print := setupCheckers(g, t, employees, people, companies, ns)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			///////////// first change: homer new name, add friend rel /////////////////
			g.Assert(homerWithFriend("homer_2")).IsNil()
			//////////// 2nd change: Mimiro new name //////////////////
			g.Assert(mimiro("Mimiro_2")).IsNil()
			/////////// 3rd change: homer new name again, remove friend
			g.Assert(homer("homer_3")).IsNil()
			/////////// 4th change: homer back to old name
			g.Assert(homer("homer_1")).IsNil()
			/////////// 5th change: Mimiro new name
			g.Assert(mimiro("Mimiro_3")).IsNil()
			/////////// 6th change: homer  final change
			g.Assert(homer("homer_4")).IsNil()
			/////////// 7th change: mimiro  final change
			g.Assert(mimiro("Mimiro_4")).IsNil()
			job.Run()

			print()
			checkEntities("homer_4", "Mimiro_4", "")
			assertChangeCount(5)
			checkChange(0, "homer_1", "Mimiro_4", "")
			checkChange(1, "homer_2", "Mimiro_4", "barney")
			checkChange(2, "homer_3", "Mimiro_4", "")
			checkChange(3, "homer_1", "Mimiro_4", "")
			checkChange(4, "homer_4", "Mimiro_4", "")
		})
	})
}

func setupCheckers(g *goblin.G, t *testing.T, employees *server.Dataset, people *server.Dataset, companies *server.Dataset, ns string) (
	func(changeIdx int, expectedHomerName string, expedtedMimiroName string, expectedHomerFriend string),
	func(expectedHomerName string, expedtedMimiroName string, expectedHomerFriend string),
	func(expectedCount int), func()) {

	fv := func(homerFriend string, ns string) interface{} {
		var friendVal interface{}
		if homerFriend != "" {
			friendVal = homerFriend
			if !strings.HasPrefix(homerFriend, ns) {
				friendVal = ns + ":" + homerFriend
			}
		}
		return friendVal
	}
	checkEntities := func(homerName, mimiroName, homerFriend string) {
		friendVal := fv(homerFriend, ns)
		entities, _ := employees.GetEntities("", math.MaxInt)
		g.Assert(len(entities.Entities)).Eql(1)
		g.Assert(entities.Entities[0].IsDeleted).IsFalse()
		g.Assert(entities.Entities[0].Properties).Eql(map[string]interface{}{
			"companyname": mimiroName,
			"name":        homerName})
		g.Assert(entities.Entities[0].References[ns+":friend"]).Eql(friendVal)
	}

	checkChanges := func(changeIdx int, homerName, mimiroName, homerFriend string) {
		friendVal := fv(homerFriend, ns)
		changes, _ := employees.GetChanges(0, math.MaxInt, false)
		g.Assert(changes.Entities[changeIdx].IsDeleted).IsFalse()
		g.Assert(changes.Entities[changeIdx].Properties["name"]).Eql(homerName)
		g.Assert(changes.Entities[changeIdx].Properties["companyname"]).Eql(mimiroName)
		g.Assert(changes.Entities[changeIdx].References[ns+":friend"]).Eql(friendVal)
	}
	assertChangeCount := func(expectedCount int) {
		changes, _ := employees.GetChanges(0, math.MaxInt, false)
		g.Assert(len(changes.Entities)).Eql(expectedCount)
	}
	printState := func() {
		// comment out or remove shortcut return to activate printing
		if 1 == 1 {
			return
		}
		// print out state
		result, _ := people.GetChanges(0, math.MaxInt, false)
		//t.Logf("\npeople change count: %v", len(result.Entities))
		for _, e := range result.Entities {
			t.Logf("people: %+v; %+v", e.Properties, e.References)
		}
		result, _ = companies.GetChanges(0, math.MaxInt, false)
		//t.Logf("\ncompanies change count: %v", len(result.Entities))
		for _, e := range result.Entities {
			t.Logf("companies: %+v", e.Properties)
		}
		result, _ = employees.GetChanges(0, math.MaxInt, false)
		//t.Log("\nexpected employees change count: 8 (baseline plus 7 changes)")
		//t.Logf("\nemployees change count: %v", len(result.Entities))

		for _, e := range result.Entities {
			t.Logf("changes: %+v; %+v", e.Properties, e.References)
		}

		entities, _ := employees.GetEntities("", math.MaxInt)
		for _, e := range entities.Entities {
			t.Logf("entities: %+v; %+v", e.Properties, e.References)
		}
	}

	return checkChanges, checkEntities, assertChangeCount, printState
}

func entityUpdaters(people *server.Dataset, ns string, companies *server.Dataset) (func(name string) error,
	func(name string) error, func(name string) error) {
	homer := func(name string) error {
		return people.StoreEntities([]*server.Entity{server.NewEntityFromMap(map[string]interface{}{
			"id":    ns + ":homer",
			"props": map[string]interface{}{"name": name},
			"refs":  map[string]interface{}{ns + ":worksfor": ns + ":mimiro"},
		})})
	}
	homerWithFriend := func(name string) error {
		return people.StoreEntities([]*server.Entity{server.NewEntityFromMap(map[string]interface{}{
			"id":    ns + ":homer",
			"props": map[string]interface{}{"name": name},
			"refs":  map[string]interface{}{ns + ":worksfor": ns + ":mimiro", ns + ":friend": ns + ":barney"},
		})})
	}
	mimiro := func(name string) error {
		return companies.StoreEntities([]*server.Entity{server.NewEntityFromMap(map[string]interface{}{
			"id":    ns + ":mimiro",
			"props": map[string]interface{}{"name": name},
			"refs":  map[string]interface{}{},
		})})
	}
	return homer, homerWithFriend, mimiro
}

func setupJob(scheduler *Scheduler, g *goblin.G, runner *Runner, latestOnly bool) *job {
	// now combine homer and mimiro into homer-the-employee
	jsFun := `function transform_entities(entities) {
			var result = []
		    for (e of entities) {
		        var relatedCompanies = Query([ e["ID"] ],  "ns3:worksfor", false);
				for (r of relatedCompanies) {
					var company = r[2];
					var employee = NewEntity()
					employee.ID = e.ID
					employee["Properties"]["name"] = e["Properties"]["name"]
					employee["Properties"]["companyname"] = company["Properties"]["name"]
					if (e["References"]["ns3:friend"]) {
						employee["References"]["ns3:friend"] = e["References"]["ns3:friend"]
					}
					result.push(employee)
				}
		    }
		    return result;
		}`
	// define job
	jobConfig, _ := scheduler.Parse([]byte(fmt.Sprintf(`{
			"id" : "sync-datasetsource-to-datasetsink-with-js-and-query",
			"triggers": [{"triggerType": "cron", "jobType": "fullSync", "schedule": "@every 2s"}],
			"source" : { "Type" : "DatasetSource", "Name" : "People", "LatestOnly": "%v" },
			"transform" : { "Type" : "JavascriptTransform", "Code" : "%v" },
			"sink" : { "Type" : "DatasetSink", "Name" : "Employees" }}`,
		latestOnly,
		base64.StdEncoding.EncodeToString([]byte(jsFun)))))

	pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
	g.Assert(err).IsNil()
	job := &job{
		id:       jobConfig.Id,
		pipeline: pipeline,
		schedule: jobConfig.Triggers[0].Schedule,
		runner:   runner,
	}
	return job
}

func setupDatasets(store *server.Store, dsm *server.DsManager) (string, *server.Dataset, *server.Dataset, *server.Dataset) {
	ns, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://namespace/")
	// we have two source datasets: People and Companies
	people, _ := dsm.CreateDataset("People", nil)
	companies, _ := dsm.CreateDataset("Companies", nil)
	// and we compose both sources into a new dataset: Workers
	employees, _ := dsm.CreateDataset("Employees", nil)
	return ns, employees, people, companies
}
