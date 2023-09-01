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
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/datahub/internal/server"
)

func TestJobs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Jobs Suite")
}

var _ = Describe("Over time, a pipeline", func() {
	testCnt := 0
	var dsm *server.DsManager
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var storeLocation string
	BeforeEach(func() {
		// temp redirect of stdout and stderr to swallow some annoying init messages in fx and jobrunner and mockService
		devNull, _ := os.Open("/dev/null")
		oldErr := os.Stderr
		oldStd := os.Stdout
		os.Stderr = devNull
		os.Stdout = devNull

		testCnt += 1
		storeLocation = fmt.Sprintf("./testpipeline_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean testfiles in "+storeLocation)

		scheduler, store, runner, dsm, _ = setupScheduler(storeLocation)

		// undo redirect of stdout and stderr after successful init of fx and jobrunner
		os.Stderr = oldErr
		os.Stdout = oldStd
	})
	AfterEach(func() {
		runner.Stop()
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})
	It("Should produce consistent output for single change", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, _ := setupCheckers(employees, people, companies, ns)
		//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

		// store first version of "homer"
		// and first version of "mimiro"
		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())

		// run job twice, output should not change
		job.Run()
		job.Run()

		checkEntities("homer_1", "Mimiro_1", "")
		checkChange(0, "homer_1", "Mimiro_1", "")
		assertChangeCount(1)
	})
	It("Should produce consistent output for changes in dependency dataset", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, _ := setupCheckers(employees, people, companies, ns)
		// checkChange, checkEntities, assertChangeCount, printState := setupCheckers(employees, people, companies, ns)

		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())
		// change company
		Expect(mimiro("Mimiro_2")).To(BeNil())

		// run job many times, output should not change
		for i := 0; i < 10; i++ {
			job.Run()
		}
		// printState()
		checkEntities("homer_1", "Mimiro_2", "")
		checkChange(0, "homer_1", "Mimiro_2", "")
		assertChangeCount(1)
	})
	It("Should produce consistent output for changes in source dataset", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		// checkChange, checkEntities, assertChangeCount, _ := setupCheckers(employees, people, companies, ns)
		checkChange, checkEntities, assertChangeCount, printState := setupCheckers(
			employees, people, companies, ns)

		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())
		// change homer
		Expect(homer("homer_2")).To(BeNil())

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
	It("Should produce consistent output for LastOnly changes in source dataset", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, true)
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		// checkChange, checkEntities, assertChangeCount, _ := setupCheckers(employees, people, companies, ns)
		checkChange, checkEntities, assertChangeCount, printState := setupCheckers(
			employees, people, companies, ns)

		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())
		// change homer
		Expect(homer("homer_2")).To(BeNil())

		// run job many times, output should not change
		for i := 0; i < 5; i++ {
			job.Run()
		}
		printState()
		checkEntities("homer_2", "Mimiro_1", "")
		assertChangeCount(
			1,
		) // only one version of homer is expected to be emitted, therefore only one resulting change version
		checkChange(0, "homer_2", "Mimiro_1", "")
	})
	It("Should produce consistent output for 2 changes in different batches", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		// set batchsize to 2
		job.pipeline.(*FullSyncPipeline).batchSize = 2
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, print := setupCheckers(
			employees, people, companies, ns)
		//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())
		Expect(homer("homer_2")).To(BeNil())
		Expect(mimiro("Mimiro_2")).To(BeNil())
		Expect(homer("homer_3")).To(BeNil())

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
	It("Should produce consistent output for 2 LastOnly changes in different batches", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, true)
		// set batchsize to 2
		job.pipeline.(*FullSyncPipeline).batchSize = 2
		homer, _, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, print := setupCheckers(
			employees, people, companies, ns)
		//	homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)

		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())
		Expect(homer("homer_2")).To(BeNil())
		Expect(mimiro("Mimiro_2")).To(BeNil())
		Expect(homer("homer_3")).To(BeNil())

		// run job many times, output should not change
		for i := 0; i < 5; i++ {
			job.Run()
		}
		print()
		checkEntities("homer_3", "Mimiro_2", "")
		checkChange(0, "homer_3", "Mimiro_2", "")
		assertChangeCount(1) // since only one "Latest" version of homer is emitted, we expect only one result
	})
	It("Should preserve state of composed change products also if sources change with time", func() {
		/*
			Here we simulate changes in two datasets over time, and verify how these changes affect the results
			of transforms which use Query to join both datasets.

			As baseline we have a person, homer, and a company, Mimiro. a transform job joins Mimiro onto homer and produces
			an entity with data from both source entities.

			With the baseline set, we add a couple of changes to both homer and Mimiro, which each should produce a new version
			of the joined "employee" entity if we run the transform again.
		*/
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, print := setupCheckers(
			employees, people, companies, ns)

		// store first version of "homer"
		// and first version of "mimiro"
		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())

		// now, first run with baseline
		job.Run()

		checkEntities("homer_1", "Mimiro_1", "")
		assertChangeCount(1)
		checkChange(0, "homer_1", "Mimiro_1", "")

		///////////// first change: homer new name, add friend /////////////////
		Expect(homerWithFriend("homer_2")).To(BeNil())
		job.Run()

		checkEntities("homer_2", "Mimiro_1", "barney")
		assertChangeCount(2)
		checkChange(0, "homer_1", "Mimiro_1", "")
		checkChange(1, "homer_2", "Mimiro_1", "barney")

		//////////// 2nd change: Mimiro new name //////////////////
		Expect(mimiro("Mimiro_2")).To(BeNil())
		job.Run()
		/////////// 3rd change: homer new name again, rm friend
		Expect(homer("homer_3")).To(BeNil())
		job.Run()
		/////////// 4th change: homer back to old name
		Expect(homer("homer_1")).To(BeNil())
		job.Run()
		/////////// 5th change: Mimiro new name
		Expect(mimiro("Mimiro_3")).To(BeNil())
		job.Run()
		/////////// 6th change: homer  final change
		Expect(homer("homer_4")).To(BeNil())
		job.Run()
		/////////// 7th change: mimiro  final change
		Expect(mimiro("Mimiro_4")).To(BeNil())
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
	It("Should preserve state of composed LatestOnly change products also if sources change with time", func() {
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, true)
		homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, print := setupCheckers(
			employees, people, companies, ns)

		// store first version of "homer"
		// and first version of "mimiro"
		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())

		// now, first run with baseline
		job.Run()

		checkEntities("homer_1", "Mimiro_1", "")
		assertChangeCount(1)
		checkChange(0, "homer_1", "Mimiro_1", "")

		///////////// first change: homer new name, add friend /////////////////
		Expect(homerWithFriend("homer_2")).To(BeNil())
		job.Run()

		checkEntities("homer_2", "Mimiro_1", "barney")
		assertChangeCount(2)
		checkChange(0, "homer_1", "Mimiro_1", "")
		checkChange(1, "homer_2", "Mimiro_1", "barney")

		//////////// 2nd change: Mimiro new name //////////////////
		Expect(mimiro("Mimiro_2")).To(BeNil())
		job.Run()
		/////////// 3rd change: homer new name again, rm friend
		Expect(homer("homer_3")).To(BeNil())
		job.Run()
		/////////// 4th change: homer back to old name
		Expect(homer("homer_1")).To(BeNil())
		job.Run()
		/////////// 5th change: Mimiro new name
		Expect(mimiro("Mimiro_3")).To(BeNil())
		job.Run()
		/////////// 6th change: homer  final change
		Expect(homer("homer_4")).To(BeNil())
		job.Run()
		/////////// 7th change: mimiro  final change
		Expect(mimiro("Mimiro_4")).To(BeNil())
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
	It("Should compose existing changes correctly in transform with Query", func() {
		/*
			  This is the same order of changes as above, except for that we only run the
				transform job once at the end.
		*/
		ns, employees, people, companies := setupDatasets(store, dsm)
		job := setupJob(scheduler, runner, dsm, false)
		homer, homerWithFriend, mimiro := entityUpdaters(people, ns, companies)
		checkChange, checkEntities, assertChangeCount, print := setupCheckers(
			employees, people, companies, ns)

		// store first version of "homer"
		// and first version of "mimiro"
		Expect(homer("homer_1")).To(BeNil())
		Expect(mimiro("Mimiro_1")).To(BeNil())

		///////////// first change: homer new name, add friend rel /////////////////
		Expect(homerWithFriend("homer_2")).To(BeNil())
		//////////// 2nd change: Mimiro new name //////////////////
		Expect(mimiro("Mimiro_2")).To(BeNil())
		/////////// 3rd change: homer new name again, remove friend
		Expect(homer("homer_3")).To(BeNil())
		/////////// 4th change: homer back to old name
		Expect(homer("homer_1")).To(BeNil())
		/////////// 5th change: Mimiro new name
		Expect(mimiro("Mimiro_3")).To(BeNil())
		/////////// 6th change: homer  final change
		Expect(homer("homer_4")).To(BeNil())
		/////////// 7th change: mimiro  final change
		Expect(mimiro("Mimiro_4")).To(BeNil())
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

func setupCheckers(
	employees *server.Dataset,
	people *server.Dataset,
	companies *server.Dataset,
	ns string,
) (
	func(changeIdx int, expectedHomerName string, expedtedMimiroName string, expectedHomerFriend string),
	func(expectedHomerName string, expedtedMimiroName string, expectedHomerFriend string),
	func(expectedCount int), func(),
) {
	GinkgoHelper()
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
		Expect(len(entities.Entities)).To(Equal(1))
		Expect(entities.Entities[0].IsDeleted).To(BeFalse())
		Expect(entities.Entities[0].Properties).To(Equal(map[string]interface{}{
			"companyname": mimiroName,
			"name":        homerName,
		}))
		if friendVal != nil {
			Expect(entities.Entities[0].References[ns+":friend"]).To(Equal(friendVal))
		} else {
			Expect(entities.Entities[0].References[ns+":friend"]).To(BeNil())
		}
	}

	checkChanges := func(changeIdx int, homerName, mimiroName, homerFriend string) {
		friendVal := fv(homerFriend, ns)
		changes, _ := employees.GetChanges(0, math.MaxInt, false)
		Expect(changes.Entities[changeIdx].IsDeleted).To(BeFalse())
		Expect(changes.Entities[changeIdx].Properties["name"]).To(Equal(homerName))
		Expect(changes.Entities[changeIdx].Properties["companyname"]).To(Equal(mimiroName))
		if friendVal != nil {
			Expect(changes.Entities[changeIdx].References[ns+":friend"]).To(Equal(friendVal))
		} else {
			Expect(changes.Entities[changeIdx].References[ns+":friend"]).To(BeNil())
		}
	}
	assertChangeCount := func(expectedCount int) {
		changes, _ := employees.GetChanges(0, math.MaxInt, false)
		Expect(len(changes.Entities)).To(Equal(expectedCount))
	}
	printState := func() {
		// comment out or remove shortcut return to activate printing
		if 1 == 1 {
			return
		}
		t := GinkgoT()
		// print out state
		result, _ := people.GetChanges(0, math.MaxInt, false)
		// t.Logf("\npeople change count: %v", len(result.Entities))
		for _, e := range result.Entities {
			t.Logf("people: %+v; %+v", e.Properties, e.References)
		}
		result, _ = companies.GetChanges(0, math.MaxInt, false)
		// t.Logf("\ncompanies change count: %v", len(result.Entities))
		for _, e := range result.Entities {
			t.Logf("companies: %+v", e.Properties)
		}
		result, _ = employees.GetChanges(0, math.MaxInt, false)
		// t.Log("\nexpected employees change count: 8 (baseline plus 7 changes)")
		// t.Logf("\nemployees change count: %v", len(result.Entities))

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

func entityUpdaters(people *server.Dataset, ns string, companies *server.Dataset) (
	func(name string) error,
	func(name string) error,
	func(name string) error,
) {
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

func setupJob(scheduler *Scheduler, runner *Runner, dsm *server.DsManager, latestOnly bool) *job {
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
	Expect(err).To(BeNil())
	job := &job{
		id:       jobConfig.ID,
		pipeline: pipeline,
		schedule: jobConfig.Triggers[0].Schedule,
		runner:   runner,
		dsm:      dsm,
	}
	return job
}

func setupDatasets(
	store *server.Store,
	dsm *server.DsManager,
) (string, *server.Dataset, *server.Dataset, *server.Dataset) {
	ns, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://namespace/")
	// we have two source datasets: People and Companies
	people, _ := dsm.CreateDataset("People", nil)
	companies, _ := dsm.CreateDataset("Companies", nil)
	// and we compose both sources into a new dataset: Workers
	employees, _ := dsm.CreateDataset("Employees", nil)
	return ns, employees, people, companies
}