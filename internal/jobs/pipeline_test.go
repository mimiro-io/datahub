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
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/server"
)

var _ = Describe("A pipeline", func() {
	testCnt := 0
	var dsm *server.DsManager
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var storeLocation string
	var mockService MockService
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
		mockService = NewMockService()
		go func() {
			_ = mockService.echo.Start(":7777")
		}()
		scheduler, store, runner, dsm, _ = setupScheduler(storeLocation)

		// undo redirect of stdout and stderr after successful init of fx and jobrunner
		os.Stderr = oldErr
		os.Stdout = oldStd
	})
	AfterEach(func() {
		runner.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_ = mockService.echo.Shutdown(ctx)
		cancel()
		mockService.HTTPNotificationChannel = nil
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	It("Should support internal js transform with txn writing to several datasets", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)
		_, _ = dsm.CreateDataset("ProductAudit", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// transform js
		js := `
			function transform_entities(entities) {
				for (e of entities) {
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
					txn.DatasetEntities["NewProducts"] = newentities;
					txn.DatasetEntities["ProductAudit"] = newentities;
					ExecuteTransaction(txn);
				}
				return entities;
			}
			`
		jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetsource-to-datasetsink-with-js",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "` + jscriptEnc + `"
			},
			"sink" : {
				"Type" : "DevNullSink"
			}
		}`
		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")

		job := &job{
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
			dsm:      dsm,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		Expect(peopleDataset).NotTo(BeNil(), "expected dataset is not present")

		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "no result is retrieved")

		Expect(len(result.Entities)).To(Equal(1), "incorrect number of entities retrieved")

		auditDataset := dsm.GetDataset("ProductAudit")
		Expect(auditDataset).NotTo(BeNil(), "expected dataset is not present")

		result, err = auditDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "no result is retrieved")

		Expect(len(result.Entities)).To(Equal(1), "incorrect number of entities retrieved")
	})

	It("Should support internal js transform with txn", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// transform js
		js := `
			function transform_entities(entities) {
				for (e of entities) {
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
					txn.DatasetEntities["NewProducts"] = newentities;
					ExecuteTransaction(txn);
				}
				return entities;
			}
			`
		jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetsource-to-datasetsink-with-js",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "` + jscriptEnc + `"
			},
			"sink" : {
				"Type" : "DevNullSink"
			}
		}`
		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		Expect(peopleDataset).NotTo(BeNil(), "expected dataset is not present")

		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "no result is retrieved")

		Expect(len(result.Entities)).To(Equal(1), "incorrect number of entities retrieved")
	})

	It("Should fullsync to an HttpDatasetSink", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)

		entities := make([]*server.Entity, 2)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		entity = server.NewEntity("http://data.mimiro.io/people/homer1", 0)
		entity.Properties["name"] = "homer"
		entities[1] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "dataset.StoreEntites returns no error")

		dsName := "fstohttp"
		jobJSON := `{
			"id" : "sync-datasetssource-to-httpdatasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
			"fullSyncSchedule" : "@every 2s",
			"runOnce" : true,
			"batchSize": 1,
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"sink" : {
				"Type" : "HttpDatasetSink",
				"Url" : "http://localhost:7777/datasets/` + dsName + `/fullsync"
			}}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "jobConfig to Pipeline returns no error")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		Expect(pipeline.spec().batchSize).To(Equal(1), "Batch size should be 1")

		job.Run()
		Expect(len(scheduler.GetRunningJobs())).To(Equal(0), "running job list is empty, indicating job done")
		Expect(len(mockService.getRecordedEntitiesForDataset(dsName))).To(Equal(2), "both 'pages' have been posted")
	})

	It("Should fullsync from an untokenized HttpDatasetSource", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("People", nil)

		// define job
		jobJSON := `{
			"id" : "sync-httpdatasetsource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:7777/datasets/people/changes"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			}}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "jobConfig to Pipeline returns no error")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		Expect(len(scheduler.GetRunningJobs())).To(Equal(0), "running job list is empty")
		rs, err := ds.GetEntities("", 100)
		Expect(err).To(BeNil(), "we found data in sink")
		Expect(len(rs.Entities)).To(Equal(10), "we found 10 entites (MockService generates 10 results)")
	})

	It("Should fullsync from a tokenized HttpDatasetSource", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("People", nil)

		// define job
		jobJSON := `
		{
			"id" : "sync-httpdatasetsource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:7777/datasets/people/changeswithcontinuation"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			}
		}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "jobConfig to Pipeline returns no error")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		Expect(len(scheduler.GetRunningJobs())).To(Equal(0), "running job list is empty")
		rs, err := ds.GetEntities("", 100)
		Expect(err).To(BeNil(), "we found data in sink")
		Expect(len(rs.Entities)).To(Equal(20), "we found 10 entites (MockService generates 20 results)")
	})

	// func TestDatasetToHttpDatasetSink(m *testing.T) {
	It("Should incrementally sync to an HttpDatasetSink", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "Entities are stored correctly")

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetssource-to-httpdatasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"sink" : {
				"Type" : "HttpDatasetSink",
				"Url" : "http://localhost:7777/datasets/writeabledevnull"
			}
		}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		Expect(len(scheduler.GetRunningJobs())).To(Equal(0), "running job list not empty")
	})

	It("Should incrementally do internal sync with js transform in parallel", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		count := 19
		entities := make([]*server.Entity, count)
		for i := 0; i < count; i++ {
			entity := server.NewEntity("http://data.mimiro.io/people/p"+strconv.Itoa(i), 0)
			entity.Properties["name"] = "homer" + strconv.Itoa(i)
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entities[i] = entity
		}

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetsource-to-datasetsink-with-js",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Parallelism" : 10,
				"Code" : "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo="
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewProducts"
			}
		}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		Expect(peopleDataset).NotTo(BeNil(), "dataset is present")

		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "result is retrieved")

		Expect(len(result.Entities)).To(Equal(19), "correct number of entities retrieved")
	})

	It("Should incrementally do internal sync with js transform in parallel when les than workers count", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		entities := make([]*server.Entity, 2)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity
		entity1 := server.NewEntity("http://data.mimiro.io/people/marge", 0)
		entity1.Properties["name"] = "marge"
		entity1.References["type"] = "http://data.mimiro.io/model/Person"
		entities[1] = entity1

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetsource-to-datasetsink-with-js",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo="
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewProducts"
			}
		}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		Expect(peopleDataset).NotTo(BeNil(), "dataset is present")

		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "result is retrieved")

		Expect(len(result.Entities)).To(Equal(2), "correct number of entities retrieved")
	})

	// func TestDatasetToDatasetWithJavascriptTransformJob(m *testing.T) {
	It("Should incrementally do internal sync with js transform", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// define job
		jobJSON := `
		{
			"id" : "sync-datasetsource-to-datasetsink-with-js",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo="
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewProducts"
			}
		}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		Expect(peopleDataset).NotTo(BeNil(), "dataset is present")

		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil(), "result is retrieved")

		Expect(len(result.Entities)).To(Equal(1), "correct number of entities retrieved")
	})
	It("Should run a transform with query in internal jobs", func() {
		testNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/test/",
		)
		Expect(err).To(BeNil())
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("People", nil)
		ds1, _ := dsm.CreateDataset("Companies", nil)
		_, _ = dsm.CreateDataset("NewPeople", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity(testNamespacePrefix+":gra", 0)
		entity.Properties[testNamespacePrefix+":name"] = "homer"
		entity.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Person"
		entity.References[testNamespacePrefix+":worksfor"] = testNamespacePrefix + ":mimiro"
		entities[0] = entity

		Expect(ds.StoreEntities(entities)).To(BeNil())

		companies := make([]*server.Entity, 1)
		mimiro := server.NewEntity(testNamespacePrefix+":mimiro", 0)
		mimiro.Properties[testNamespacePrefix+":name"] = "Mimiro"
		mimiro.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Company"
		companies[0] = mimiro

		Expect(ds1.StoreEntities(companies)).To(BeNil())

		jsFun := `function transform_entities(entities) {
		    var test_ns = GetNamespacePrefix("http://data.mimiro.io/test/")
		    for (e of entities) {
		        Log(e["ID"])
		        var relatedCompanies = Query([ e["ID"] ], test_ns + ":worksfor", false);
				if (relatedCompanies.length == 1) {
					var firstCompany = relatedCompanies[0][2];
					Log(firstCompany);
					e["Properties"][test_ns + ":companyname"] = firstCompany["Properties"][test_ns + ":name"];
				}
		    }
		    return entities;
		}`
		// define job
		jobJSON := fmt.Sprintf(`{
			"id" : "sync-datasetsource-to-datasetsink-with-js-and-query",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "People"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "%v"
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewPeople"
			}}`, base64.StdEncoding.EncodeToString([]byte(jsFun)))

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewPeople")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(1))
		Expect(result.Entities[0].Properties["ns3:companyname"]).To(Equal("Mimiro"))
	})
	It("Should run a transform with subentities in internal jobs", func() {
		testNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion(
			"http://data.mimiro.io/test/",
		)
		Expect(err).To(BeNil())

		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("People", nil)
		_, _ = dsm.CreateDataset("NewPeople", nil)

		address := server.NewEntity(testNamespacePrefix+":home", 0)
		address.Properties[testNamespacePrefix+":street"] = "homestreet"

		entities := make([]*server.Entity, 2)

		entities[0] = server.NewEntity(testNamespacePrefix+":homer", 0)
		entities[0].Properties[testNamespacePrefix+":name"] = "homer"
		entities[0].Properties[testNamespacePrefix+":address"] = address
		entities[0].Properties[testNamespacePrefix+":listref"] = []string{"//homer", "//male"}
		entities[0].Properties[testNamespacePrefix+":ref"] = "//homer"

		entities[1] = server.NewEntity(testNamespacePrefix+":barney", 0)
		entities[1].Properties[testNamespacePrefix+":name"] = "barney"
		entities[1].Properties[testNamespacePrefix+":address"] = map[string]interface{}{
			"id":    testNamespacePrefix + ":barn",
			"props": map[string]interface{}{testNamespacePrefix + ":street": "barnstreet"},
			"refs":  map[string]interface{}{},
		}

		Expect(ds.StoreEntities(entities)).To(BeNil())

		jsFun := `function transform_entities(entities) {
		    var test_ns = GetNamespacePrefix("http://data.mimiro.io/test/");
			var result = [];
		    for (e of entities) {
                var address = GetProperty(e, test_ns, "address");
				var listref = GetProperty(e, test_ns, "listref");
				var ref = GetProperty(e, test_ns, "ref");
                //sub-entities must be converted to Entity instances before GetProperty and other helpers work on them
                var addressEntity = AsEntity(address)
                var street = GetProperty(addressEntity, test_ns, "street");
                var r = NewEntity();
				SetId(r, GetId(e));
	            SetProperty(r, test_ns,"street",street);
	            SetProperty(r, test_ns,"address",AsEntity(address));
	            SetProperty(r, test_ns,"no_address",AsEntity(street));
				AddReference(r, test_ns, "listref", "listref")
				AddReference(r, test_ns, "ref", "ref")
                result.push(r);
		    }

		    return result;
		}`
		// define job
		jobJSON := fmt.Sprintf(`{
			"id" : "sync-datasetsource-to-datasetsink-with-js-and-query",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "People"
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "%v"
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewPeople"
			}}`, base64.StdEncoding.EncodeToString([]byte(jsFun)))

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewPeople")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(2))
		Expect(result.Entities[0].Properties["ns3:street"]).To(Equal("homestreet"))
		Expect(result.Entities[0].Properties["ns3:no_address"]).To(BeNil())
		Expect(result.Entities[0].Properties["ns3:address"]).NotTo(BeNil())
		Expect(result.Entities[1].Properties["ns3:street"]).To(Equal("barnstreet"))
	})

	It("Should run external transforms in internal jobs", func() {
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		Expect(ds.StoreEntities(entities)).To(BeNil())

		// define job
		jobJSON := `{
			"id" : "sync-datasetsource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"transform" : {
				"Type" : "HttpTransform",
				"Url" : "http://localhost:7777/transforms/identity"
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewProducts"
			}}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(1))
	})

	It("Should not write to the sink if an external transform endpoint is 404", func() {
		jobJSON := ` {
			"id" : "sync-httpdatasetsource-to-datasetsink-1",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:7777/datasetsarecool/people/changes"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			} }`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		Expect(dsm.GetDataset("People")).To(BeZero(), "dataset should not exist as job failed")
	})

	It("Should copy datasetsource to datasetsink in first run of internal job", func() {
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entity.References["type"] = "http://data.mimiro.io/model/Person"
		entity.References["typed"] = "http://data.mimiro.io/model/Person"
		entities[0] = entity

		Expect(ds.StoreEntities(entities)).To(BeNil())

		jobJSON := ` {
			"id" : "sync-datasetsource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "DatasetSource",
				"Name" : "Products"
			},
			"sink" : {
				"Type" : "DatasetSink",
                "Name" : "NewProducts"
			} }`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// check number of entities in target dataset
		peopleDataset := dsm.GetDataset("NewProducts")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(1))
	})

	It("Should copy from samplesource to datasetsink in first run of new job", func() {
		_, _ = dsm.CreateDataset("People", nil)

		jobJSON := `{
			"id" : "sync-samplesource-to-datasetsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities" : 1
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			}}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// get entities from people dataset
		peopleDataset := dsm.GetDataset("People")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(1))
	})

	It("Should copy from HttpDatasetSource to datasetSink in first run of new job", func() {
		_, _ = dsm.CreateDataset("People", nil)

		jobJSON := `{
			"id" : "sync-httpdatasetsource-to-datasetsink-1",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "HttpDatasetSource",
				"Url" : "http://localhost:7777/datasets/people/changes"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "People"
			}}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		// get entities from people dataset
		peopleDataset := dsm.GetDataset("People")
		result, err := peopleDataset.GetEntities("", 50)
		Expect(err).To(BeNil())
		Expect(len(result.Entities)).To(Equal(10))
	})
	It(
		"Should copy all pages using continuatin tokens from httpDatasetSource to datasetSink if first run",
		func() {
			_, _ = dsm.CreateDataset("People", nil)

			jobJSON := `{
					"id" : "sync-httpdatasetsource-to-datasetsink-1",
					"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
					"source" : {
						"Type" : "HttpDatasetSource",
						"Url" : "http://localhost:7777/datasets/people/changeswithcontinuation"
					},
					"sink" : {
						"Type" : "DatasetSink",
						"Name" : "People"
					} }`

			jobConfig, _ := scheduler.Parse([]byte(jobJSON))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			Expect(err).To(BeNil())

			job := &job{
				dsm:      dsm,
				id:       jobConfig.ID,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			// run once
			job.Run()

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			Expect(err).To(BeNil())
			Expect(len(result.Entities)).To(Equal(20))

			// run again
			job.Run()
			peopleDataset = dsm.GetDataset("People")
			result, err = peopleDataset.GetEntities("", 50)
			Expect(err).To(BeNil())
			Expect(len(result.Entities)).To(Equal(20), "results should stay the same")
		},
	)

	It(
		"Should mark entities that have not been received again during fullsync to internal dataset as deleted",
		func() {
			sourceDs, _ := dsm.CreateDataset("people", nil)
			sinkDs, _ := dsm.CreateDataset("people2", nil)

			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = sourceDs.StoreEntities([]*server.Entity{e1, e2})

			pipeline := &FullSyncPipeline{PipelineSpec{
				source: &source.DatasetSource{DatasetName: "people", Store: store, DatasetManager: dsm},
				sink:   &datasetSink{DatasetName: "people2", Store: store, DatasetManager: dsm},
			}}

			job := &job{id: "fullsync-1", pipeline: pipeline, runner: runner, dsm: dsm}

			// run once, both entities should sync
			job.Run()

			res, err := sinkDs.GetEntities("", 100)
			Expect(err).To(BeNil())
			Expect(len(res.Entities)).To(Equal(2))
			Expect(res.Entities[0].ID).To(Equal("1"))
			Expect(res.Entities[0].IsDeleted).To(Equal(false))
			Expect(res.Entities[1].ID).To(Equal("2"))
			Expect(res.Entities[1].IsDeleted).To(Equal(false))

			// delete ds and recreate with only 1 entity
			Expect(dsm.DeleteDataset("people")).To(BeNil())
			sourceDs, _ = dsm.CreateDataset("people", nil)
			Expect(sourceDs.StoreEntities([]*server.Entity{e2})).To(BeNil())

			// run again. deletion detection should apply
			job.Run()

			res, err = sinkDs.GetEntities("", 100)
			Expect(err).To(BeNil())
			Expect(len(res.Entities)).To(Equal(2))
			Expect(res.Entities[0].ID).To(Equal("1"))
			Expect(res.Entities[0].IsDeleted).To(Equal(true), "Entity 1 should be deleted now")
			Expect(res.Entities[1].ID).To(Equal("2"))
			Expect(res.Entities[1].IsDeleted).To(Equal(false))
		},
	)
	It("Should store continuation token after every page in incremental job", func() {
		pipeline := &IncrementalPipeline{PipelineSpec{
			batchSize: 5,
			source:    &source.SampleSource{NumberOfEntities: 10, Store: store},
			sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/inctest/fullsync", Store: store},
		}}
		job := &job{id: "inc-1", pipeline: pipeline, runner: runner, dsm: dsm}

		// run async, so we can verify tokens in parallel
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			job.Run()
			wg.Done()
		}()

		// block and wait for channel notification - indicating the first page/batch has been received
		<-mockService.HTTPNotificationChannel
		Expect(len(mockService.getRecordedEntitiesForDataset("inctest"))).
			To(Equal(5), "After first batch, 5 entities should have been postet to httpSink")

		// block for next batch request finished - this should be before syncState is updated
		<-mockService.HTTPNotificationChannel
		syncJobState := &SyncJobState{}
		err := store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal("5"), "Between batch 1 and 2, token should be continuation of batch 1")

		wg.Wait()
		syncJobState = &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token = syncJobState.ContinuationToken
		Expect(token).To(Equal("10"))
	})
	It("Should store continuation token only after finished run in fullsync job", func() {
		pipeline := &FullSyncPipeline{PipelineSpec{
			batchSize: 5,
			source:    &source.SampleSource{NumberOfEntities: 10, Store: store},
			// sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
			sink: &devNullSink{},
		}}
		job := &job{id: "full-1", pipeline: pipeline, runner: runner, dsm: dsm}

		// run async, so we can verify tokens in parallel
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			job.Run()
			wg.Done()
		}()
		//block and wait for channel notification - indicating the first page/batch has been received
		//_ = <-mockService.HttpNotificationChannel
		//Expect(len(mockService.getRecordedEntitiesForDataset("fulltest"))).
		//	To(Equal(5, "After first batch, 5 entities should have been postet to httpSink"))

		//wait for first syncState (token) update in badger (should be in db when 2nd batch arrives)
		//_ = <-mockService.HttpNotificationChannel
		syncJobState := &SyncJobState{}
		err := store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal(""), "there should not be a token stored after first batch")

		// wait for job to finish
		wg.Wait()
		syncJobState = &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token = syncJobState.ContinuationToken
		Expect(token).To(Equal("10"), "First after job there should be a token")
	})

	It("Should post changes to HttpDatasetSink endpoint if jobType is incremental", func() {
		srcDs, _ := dsm.CreateDataset("src", nil)
		e1 := server.NewEntity("1", 0)
		e2 := server.NewEntity("2", 0)
		_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
		e1.IsDeleted = true
		_ = srcDs.StoreEntities([]*server.Entity{e1})
		e1.IsDeleted = false
		_ = srcDs.StoreEntities([]*server.Entity{e1})

		var sourceChanges []*server.Entity
		_, err := srcDs.ProcessChanges(0, 100, false, func(entity *server.Entity) {
			sourceChanges = append(sourceChanges, entity)
		})
		Expect(err).To(BeNil())
		Expect(len(sourceChanges)).To(Equal(4), "Expected 4 changes for our two entities in source")

		pipeline := &IncrementalPipeline{PipelineSpec{
			batchSize: 5,
			source:    &source.DatasetSource{DatasetName: "src", Store: store, DatasetManager: dsm},
			sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/inctest/fullsync", Store: store},
		}}
		job := &job{id: "inc-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()
		sinkChanges := mockService.getRecordedEntitiesForDataset("inctest")
		Expect(len(sinkChanges)).To(Equal(4), "Expected all 4 changes in sink for incremental")
	})

	It("Should post entities to HttpDatasetSink endpoint if jobType is fullsync", func() {
		srcDs, _ := dsm.CreateDataset("src", nil)
		e1 := server.NewEntity("1", 0)
		e2 := server.NewEntity("2", 0)
		_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
		e1.IsDeleted = true
		_ = srcDs.StoreEntities([]*server.Entity{e1})
		e1.IsDeleted = false
		_ = srcDs.StoreEntities([]*server.Entity{e1})

		var sourceChanges []*server.Entity
		_, err := srcDs.ProcessChanges(0, 100, false, func(entity *server.Entity) {
			sourceChanges = append(sourceChanges, entity)
		})
		Expect(err).To(BeNil())
		Expect(len(sourceChanges)).To(Equal(4), "Expected 4 changes for our two entities in source")

		pipeline := &FullSyncPipeline{PipelineSpec{
			batchSize: 5,
			source:    &source.DatasetSource{DatasetName: "src", Store: store, DatasetManager: dsm},
			sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
		}}
		job := &job{id: "inc-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()
		sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(2), "Expected only 2 entities in current state in fullsync")
	})

	It("Should store dependency watermarks after fullsync in MultiSource jobs", func() {
		srcDs, _ := dsm.CreateDataset("src", nil)
		_ = srcDs.StoreEntities([]*server.Entity{
			server.NewEntity("1", 0),
			server.NewEntity("2", 0),
		})

		depDs, _ := dsm.CreateDataset("dep", nil)
		_ = depDs.StoreEntities([]*server.Entity{
			server.NewEntity("3", 0),
			server.NewEntity("4", 0),
			server.NewEntity("5", 0),
		})

		pipeline := &FullSyncPipeline{PipelineSpec{
			batchSize: 5,
			source: &source.MultiSource{
				DatasetName:    "src",
				Store:          store,
				DatasetManager: dsm,
				Dependencies: []source.Dependency{
					{
						Dataset: "dep",
						Joins:   []source.Join{{Dataset: "src", Predicate: "http:/a/predicate", Inverse: false}},
					},
				},
			},
			sink: &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
		}}
		job := &job{id: "fs-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()

		sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(2), "Expected only 2 entities in current state in fullsync")

		syncJobState := &SyncJobState{}
		err := store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"3\"}}}"),
			"after job there should be a token")
	})

	It("Should store dependency watermarks after incremental in MultiSource jobs", func() {
		srcDs, _ := dsm.CreateDataset("src", nil)
		ns, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://namespace/")
		_ = srcDs.StoreEntities([]*server.Entity{
			server.NewEntity(ns+":1", 0),
			server.NewEntity(ns+":2", 0),
		})

		depDs, _ := dsm.CreateDataset("dep", nil)
		_ = depDs.StoreEntities([]*server.Entity{
			server.NewEntity(ns+":3", 0),
			server.NewEntity(ns+":4", 0),
			server.NewEntity(ns+":5", 0),
		})

		pipeline := &IncrementalPipeline{PipelineSpec{
			batchSize: 5,
			source: &source.MultiSource{
				DatasetName:    "src",
				Store:          store,
				DatasetManager: dsm,
				Dependencies: []source.Dependency{
					{
						Dataset: "dep",
						Joins:   []source.Join{{Dataset: "src", Predicate: ns + ":predicate", Inverse: false}},
					},
				},
			},
			sink: &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
		}}
		job := &job{id: "fs-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()

		sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(2), "Expected only 2 entities")

		syncJobState := &SyncJobState{}
		err := store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"3\"}}}"),
			"after job there should be a token")

		// reset recorder
		for k := range mockService.RecordedEntities {
			delete(mockService.RecordedEntities, k)
		}
		// add dependency link
		e := server.NewEntity(ns+":5", 0)
		e.References[ns+":predicate"] = ns + ":1"
		_ = depDs.StoreEntities([]*server.Entity{e})

		job.Run()

		sinkChanges = mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(1), "Expected only 1 entity (id=1 was linked to by dependency)")
		Expect(sinkChanges[0].ID).To(Equal(ns+":1"), "new dependency points to id 1")

		syncJobState = &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token = syncJobState.ContinuationToken
		Expect(token).To(Equal("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"4\"}}}"),
			"dependency watermarks should be forwarded by 1")

		// reset recorder
		for k := range mockService.RecordedEntities {
			delete(mockService.RecordedEntities, k)
		}

		// run one more time without changes to source data, make sure nothing is done
		job.Run()

		sinkChanges = mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(0))

		syncJobState = &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token = syncJobState.ContinuationToken
		Expect(token).To(Equal("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"4\"}}}"),
			"dependency watermarks should be unchanged")
	})
	It("Should not store UnionDatasetContinuation after fullsync on UnionDatasetSource", func() {
		ds1, _ := dsm.CreateDataset("src1", nil)
		_ = ds1.StoreEntities([]*server.Entity{
			server.NewEntity("1", 0),
			server.NewEntity("2", 0),
		})
		ds2, _ := dsm.CreateDataset("src2", nil)
		_ = ds2.StoreEntities([]*server.Entity{
			server.NewEntity("3", 0),
			server.NewEntity("4", 0),
			server.NewEntity("5", 0),
		})

		jobJSON := ` {
			"id" : "sync-uniondatasetsource-to-nullsink",
			"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "UnionDatasetSource",
				"DatasetSources" : [{"Name":"src1"},{"Name":"src2"}]
			},
			"sink" : {
				"Type" : "HttpDatasetSink",
				"Url":"http://localhost:7777/datasets/fulltest/fullsync"
			} }`

		jobConfig, err := scheduler.Parse([]byte(jobJSON))
		Expect(err).To(BeNil())
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil())
		job := &job{id: "fs-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()

		sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
		Expect(len(sinkChanges)).To(Equal(5), "Expected 5 entities in current state in fullsync")

		syncJobState := &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal(""),
			"after job there should be NO token because fullsync uses entities towards httpsink")
	})
	It("Should store UnionDatasetContinuation after incremental on UnionDatasetSource", func() {
		ds1, _ := dsm.CreateDataset("src1", nil)
		_ = ds1.StoreEntities([]*server.Entity{
			server.NewEntity("1", 0),
			server.NewEntity("2", 0),
		})
		ds2, _ := dsm.CreateDataset("src2", nil)
		_ = ds2.StoreEntities([]*server.Entity{
			server.NewEntity("3", 0),
			server.NewEntity("4", 0),
			server.NewEntity("5", 0),
		})

		jobJSON := ` {
			"id" : "sync-uniondatasetsource-to-nullsink",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "UnionDatasetSource",
				"DatasetSources" : [{"Name":"src1"},{"Name":"src2", "LatestOnly": true}]
			},
			"sink" : {
				"Type" : "HttpDatasetSink",
				"Url":"http://localhost:7777/datasets/inctest/fullsync"
			} }`

		jobConfig, err := scheduler.Parse([]byte(jobJSON))
		Expect(err).To(BeNil())
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil())
		job := &job{id: "inc-1", pipeline: pipeline, runner: runner, dsm: dsm}
		job.Run()

		sinkChanges := mockService.getRecordedEntitiesForDataset("inctest")
		Expect(len(sinkChanges)).To(Equal(5), "Expected 5 entities in current state")

		syncJobState := &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		token := syncJobState.ContinuationToken
		Expect(token).To(Equal(
			"{\"Tokens\":[{\"Token\":\"2\"},{\"Token\":\"3\"}],\"DatasetNames\":[\"src1\",\"src2\"]}"),
			"after job there should be a token stored")
	})
	It("Should fail run and return error when error in transform js", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("Products", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// transform js
		js := `
			function transform_entities(entities) {
				for (e of entities) {
					var something = null;
					var fail = something[0][2];
				}
				return entities;
			}
			`
		jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))

		// define job
		jobJSON := `
			{
				"id" : "sync-datasetsource-to-datasetsink-with-js",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "DatasetSource",
					"Name" : "Products",
					"LatestOnly": "true"
				},
				"transform" : {
					"Type" : "JavascriptTransform",
					"Code" : "` + jscriptEnc + `"
				},
				"sink" : {
					"Type" : "DevNullSink"
				}
			}`
		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed")
		Expect(pipeline.spec().source.(*source.DatasetSource).LatestOnly).To(BeTrue())

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()

		syncJobState := &SyncJobState{}
		err = store.GetObject(server.JobDataIndex, job.id, syncJobState)
		Expect(err).To(BeNil())
		Expect(syncJobState.LastRunCompletedOk).To(BeFalse())

		jobResult := &jobResult{}
		err = store.GetObject(server.JobResultIndex, job.id, jobResult)
		Expect(err).To(BeNil())
		Expect(jobResult.LastError == "").To(BeFalse())
	})

	It("Should support proxy dataset as incremental datasetSource", func() {
		_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL: "http://localhost:7777/datasets/people",
			},
		})
		Expect(err).To(BeNil())

		// define job
		jobJSON := `
			{
				"id" : "sync-proxydatasetsource-to-httpdatasetsink",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "DatasetSource",
					"Name" : "proxy"
				},
				"sink" : {
					"Type" : "HttpDatasetSink",
					"Url" : "http://localhost:7777/datasets/proxysink/fullsync"
				}
			}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		Expect(len(mockService.RecordedEntities["proxysink"])).To(Equal(11), "sink received content")
		Expect(mockService.RecordedEntities["proxysink"][1].ID).To(Equal("ns3:e-0"), "sink received entity from proxy")
	})

	It("Should support proxy dataset as fullsync datasetSource", func() {
		_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL: "http://localhost:7777/datasets/people",
			},
		})
		Expect(err).To(BeNil())

		// define job
		jobJSON := `
			{
				"id" : "sync-proxydatasetsource-to-httpdatasetsink",
				"triggers": [{"triggerType": "cron", "jobType": "fullsync", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "DatasetSource",
					"Name" : "proxy"
				},
				"sink" : {
					"Type" : "HttpDatasetSink",
					"Url" : "http://localhost:7777/datasets/proxysink/fullsync"
				}
			}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		ents := mockService.getRecordedEntitiesForDataset("proxysink")
		Expect(len(ents)).To(Equal(10), "sink received content")
		Expect(ents[0].ID).To(Equal("ns3:fs-0"), "sink received entity from proxy")
	})

	It("Should support proxy dataset as incremental datasetSink", func() {
		_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL:        "http://localhost:7777/datasets/people",
				AuthProviderName: "local",
			},
		})
		Expect(err).To(BeNil())

		// define job
		jobJSON := `
			{
				"id" : "sync-proxydatasetsource-to-httpdatasetsink",
				"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "HttpDatasetSource",
					"Url" : "http://localhost:7777/datasets/people/changeswithcontinuation"
				},
				"sink" : {
					"Type" : "DatasetSink",
					"Name" : "proxy"
				}
			}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		var receivedMockRequests []*http.Request
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for afterCh := time.After(100 * time.Millisecond); ; {
				select {
				case d := <-mockService.HTTPNotificationChannel:
					receivedMockRequests = append(receivedMockRequests, d)
				case <-afterCh:
					return
				}
			}
		}()
		wg.Wait()
		Expect(len(receivedMockRequests) > 0).To(BeTrue())
		firstSinkReq := receivedMockRequests[1]
		Expect(firstSinkReq.Header["Authorization"]).To(Equal([]string{"Basic dTEwMDpwMjAw"}))
		ents := mockService.getRecordedEntitiesForDataset("people")
		Expect(len(receivedMockRequests)).To(Equal(4), "2 requests to source and 2 to sink")
		Expect(len(ents)).To(Equal(20), "sink received 2 batches of content")
		Expect(ents[10].ID).To(Equal("ns3:e-0"))
	})

	It("Should support proxy dataset as fullsync datasetSink", func() {
		_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
			ProxyDatasetConfig: &server.ProxyDatasetConfig{
				RemoteURL:        "http://localhost:7777/datasets/people",
				AuthProviderName: "local",
			},
		})
		Expect(err).To(BeNil())

		// define job
		jobJSON := `
			{
				"id" : "sync-proxydatasetsource-to-httpdatasetsink",
				"triggers": [{"triggerType": "cron", "jobType": "fullSync", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "HttpDatasetSource",
					"Url" : "http://localhost:7777/datasets/people/changeswithcontinuation"
				},
				"sink" : {
					"Type" : "DatasetSink",
					"Name" : "proxy"
				}
			}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		var receivedMockRequests []*http.Request
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for afterCh := time.After(100 * time.Millisecond); ; {
				select {
				case d := <-mockService.HTTPNotificationChannel:
					receivedMockRequests = append(receivedMockRequests, d)
				case <-afterCh:
					return
				}
			}
		}()
		wg.Wait()

		Expect(len(receivedMockRequests) > 0).To(BeTrue())
		firstSinkReq := receivedMockRequests[1]
		Expect(firstSinkReq.Header["Authorization"]).To(Equal([]string{"Basic dTEwMDpwMjAw"}))
		Expect(firstSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).NotTo(BeZero())
		Expect(firstSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).To(Equal([]string{"true"}))
		Expect(firstSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).To(BeZero())
		secondSinkReq := receivedMockRequests[3]
		Expect(secondSinkReq.Header["Authorization"]).To(Equal([]string{"Basic dTEwMDpwMjAw"}))
		Expect(secondSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).NotTo(BeZero())
		Expect(secondSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).To(BeZero())
		Expect(secondSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).To(BeZero())
		thirdSinkReq := receivedMockRequests[4]
		Expect(thirdSinkReq.Header["Authorization"]).To(Equal([]string{"Basic dTEwMDpwMjAw"}))
		Expect(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).NotTo(BeZero())
		Expect(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).To(BeZero())
		Expect(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).To(Equal([]string{"true"}))
		ents := mockService.getRecordedEntitiesForDataset("people")
		Expect(len(receivedMockRequests)).To(Equal(5), "2 requests to source and 2 to sink plus fullsync end to sink")
		Expect(len(ents)).To(Equal(20), "sink received 2 batches of content")
		Expect(ents[10].ID).To(Equal("ns3:e-0"))
	})

	It("Should not panic when pipeline sink dataset is missing", func() {
		// define job where datasets in source and sink are missing
		jobJSON := `
			{
				"id" : "sync-multi-to-dataset",
				"triggers": [{"triggerType": "cron", "jobType": "fullSync", "schedule": "@every 2s"}],
				"source" : {
					"Type" : "MultiSource",
					"Name" : "people", "Dependencies": [ {
					"dataset": "address",
					"joins": [
						{ "dataset": "office", "predicate": "http://office/location", "inverse": true },
						{ "dataset": "people", "predicate": "http://office/contact", "inverse": false },
						{ "dataset": "team", "predicate": "http://team/lead", "inverse": true },
						{ "dataset": "people", "predicate": "http://team/member", "inverse": false }
					] } ]
				},
				"sink" : {
					"Type" : "DatasetSink",
					"Name" : "testsink"
				}
			}`

		jobConfig, _ := scheduler.Parse([]byte(jobJSON))
		pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
		Expect(err).To(BeNil(), "pipeline is parsed correctly")

		job := &job{
			dsm:      dsm,
			id:       jobConfig.ID,
			pipeline: pipeline,
			schedule: jobConfig.Triggers[0].Schedule,
			runner:   runner,
		}

		job.Run()
		history := scheduler.GetJobHistory()
		Expect(history[0].LastError).To(Equal("dataset does not exist: testsink"))

		// fix sink, now missing source should not panic
		_, _ = dsm.CreateDataset("testsink", nil)
		job.Run()
		history = scheduler.GetJobHistory()
		Expect(history[0].LastError).To(Equal("dataset people is missing, dataset is missing"))

		// fix main source as well, errors should be gone
		_, _ = dsm.CreateDataset("people", nil)
		job.Run()
		history = scheduler.GetJobHistory()
		Expect(history[0].LastError).To(BeZero())
	})
})
