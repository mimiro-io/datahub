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
		g.It("Should preserve state of composed change products also if sources change with time", func() {
			g.Timeout(1 * time.Hour)
			/*
				Here we simulate changes in two datasets over time, and verify how these changes affect the results
				of transforms which use Query to join both datasets.

				As baseline we have a person, homer, and a company, Mimiro. a transform job joins Mimiro onto homer and produces
				an entity with data from both source entities.

				With the baseline set, we add a couple of changes to both homer and Mimiro, which each should produce a new version
				of the joined "employee" entity if we run the transform again.
			*/
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner)
			homer, mimiro := entityUpdaters(people, ns, companies)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			// now, first run with baseline
			job.Run()

			// check number of entities in target dataset
			result, err := employees.GetChanges(0, math.MaxInt)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
			check(g, result, 0, "homer_1", "Mimiro_1")

			///////////// first change: homer new name /////////////////
			g.Assert(homer("homer_2")).IsNil()
			job.Run()
			// expected to add one change to sink, cnt=2
			result, _ = employees.GetChanges(0, math.MaxInt)
			g.Assert(len(result.Entities)).Eql(2)
			check(g, result, 0, "homer_1", "Mimiro_1")
			check(g, result, 1, "homer_2", "Mimiro_1")

			//////////// 2nd change: Mimiro new name //////////////////
			g.Assert(mimiro("Mimiro_2")).IsNil()
			job.Run()
			/////////// 3rd change: homer new name again
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

			// print out state
			result, _ = people.GetChanges(0, math.MaxInt)
			t.Logf("\npeople change count: %v", len(result.Entities))
			for _, e := range result.Entities {
				t.Logf("people: %+v", e.Properties)
			}
			result, _ = companies.GetChanges(0, math.MaxInt)
			t.Logf("\ncompanies change count: %v", len(result.Entities))
			for _, e := range result.Entities {
				t.Logf("companies: %+v", e.Properties)
			}
			result, _ = employees.GetChanges(0, math.MaxInt)
			t.Log("\nexpected employees change count: 8 (baseline plus 7 changes)")
			t.Logf("\nemployees change count: %v", len(result.Entities))

			for _, e := range result.Entities {
				t.Logf("employees: %+v", e.Properties)
			}

			g.Assert(len(result.Entities)).Eql(8)
			check(g, result, 0, "homer_1", "Mimiro_1")
			check(g, result, 1, "homer_2", "Mimiro_1")
			check(g, result, 2, "homer_2", "Mimiro_2")
			check(g, result, 3, "homer_3", "Mimiro_2")
			check(g, result, 4, "homer_1", "Mimiro_2")
			check(g, result, 5, "homer_1", "Mimiro_3")
			check(g, result, 6, "homer_4", "Mimiro_3")
			check(g, result, 7, "homer_4", "Mimiro_4")
		})
		g.It("Should compose existing changes correctly in transform with Query", func() {
			/*
				  This is almost the same order of changes as above, except for that we only run the
					transform job once at the end.
			*/
			ns, employees, people, companies := setupDatasets(store, dsm)
			job := setupJob(scheduler, g, runner)
			homer, mimiro := entityUpdaters(people, ns, companies)

			// store first version of "homer"
			// and first version of "mimiro"
			g.Assert(homer("homer_1")).IsNil()
			g.Assert(mimiro("Mimiro_1")).IsNil()

			///////////// first change: homer new name /////////////////
			g.Assert(homer("homer_2")).IsNil()
			//////////// 2nd change: Mimiro new name //////////////////
			g.Assert(mimiro("Mimiro_2")).IsNil()
			/////////// 3rd change: homer new name again
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

			// print out state
			result, _ := people.GetChanges(0, math.MaxInt)
			t.Logf("\npeople change count: %v", len(result.Entities))
			for _, e := range result.Entities {
				t.Logf("people: %+v", e.Properties)
			}
			result, _ = companies.GetChanges(0, math.MaxInt)
			t.Logf("\ncompanies change count: %v", len(result.Entities))
			for _, e := range result.Entities {
				t.Logf("companies: %+v", e.Properties)
			}
			result, _ = employees.GetChanges(0, math.MaxInt)
			t.Log("\nexpected employees change count: 8 (baseline plus 7 changes)")
			t.Logf("\nemployees change count: %v", len(result.Entities))

			for _, e := range result.Entities {
				t.Logf("employees: %+v", e.Properties)
			}

			g.Assert(len(result.Entities)).Eql(8)
			check(g, result, 0, "homer_1", "Mimiro_1")
			check(g, result, 1, "homer_2", "Mimiro_1")
			check(g, result, 2, "homer_2", "Mimiro_2")
			check(g, result, 3, "homer_3", "Mimiro_2")
			check(g, result, 4, "homer_1", "Mimiro_2")
			check(g, result, 5, "homer_1", "Mimiro_3")
			check(g, result, 6, "homer_4", "Mimiro_3")
			check(g, result, 7, "homer_4", "Mimiro_4")
		})
	})
}

func check(g *goblin.G, result *server.Changes, changeIdx int, homer, mimiro string) {
	g.Assert(result.Entities[changeIdx].Properties["name"]).Eql(homer)
	g.Assert(result.Entities[changeIdx].Properties["companyname"]).Eql(mimiro)
}

func entityUpdaters(people *server.Dataset, ns string, companies *server.Dataset) (func(name string) error, func(name string) error) {
	homer := func(name string) error {
		return people.StoreEntities([]*server.Entity{server.NewEntityFromMap(map[string]interface{}{
			"id":    ns + ":homer",
			"props": map[string]interface{}{"name": name},
			"refs":  map[string]interface{}{ns + ":worksfor": ns + ":mimiro"},
		})})
	}
	mimiro := func(name string) error {
		return companies.StoreEntities([]*server.Entity{server.NewEntityFromMap(map[string]interface{}{
			"id":    ns + ":mimiro",
			"props": map[string]interface{}{"name": name},
			"refs":  map[string]interface{}{},
		})})
	}
	return homer, mimiro
}

func setupJob(scheduler *Scheduler, g *goblin.G, runner *Runner) *job {
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
					result.push(employee)
				}
		    }
		    return result;
		}`
	// define job
	jobConfig, _ := scheduler.Parse([]byte(fmt.Sprintf(`{
			"id" : "sync-datasetsource-to-datasetsink-with-js-and-query",
			"triggers": [{"triggerType": "cron", "jobType": "fullSync", "schedule": "@every 2s"}],
			"source" : { "Type" : "DatasetSource", "Name" : "People" },
			"transform" : { "Type" : "JavascriptTransform", "Code" : "%v" },
			"sink" : { "Type" : "DatasetSink", "Name" : "Employees" }}`,
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
	people, _ := dsm.CreateDataset("People")
	companies, _ := dsm.CreateDataset("Companies")
	// and we compose both sources into a new dataset: Workers
	employees, _ := dsm.CreateDataset("Employees")
	return ns, employees, people, companies
}
