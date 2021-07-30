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
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/franela/goblin"

	"github.com/mimiro-io/datahub/internal/server"
)

func TestPipeline(t *testing.T) {
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


		g.It("Should support internal js transform with txn writing to several datasets", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")
			_, _ = dsm.CreateDataset("ProductAudit")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

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
			jobJson := `
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
			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("expected dataset is not present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")

			auditDataset := dsm.GetDataset("ProductAudit")
			g.Assert(auditDataset).IsNotNil("expected dataset is not present")

			result, err = auditDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")
		})


		g.It("Should support internal js transform with txn", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

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
			jobJson := `
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
			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("expected dataset is not present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")
		})

		g.It("Should fullsync to an HttpDatasetSink", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")

			entities := make([]*server.Entity, 2)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			entity = server.NewEntity("http://data.mimiro.io/people/homer1", 0)
			entity.Properties["name"] = "homer"
			entities[1] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("dataset.StoreEntites returns no error")

			dsName := "fstohttp"
			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			g.Assert(pipeline.spec().batchSize).Eql(1, "Batch size should be 1")

			job.Run()
			g.Assert(len(scheduler.GetRunningJobs())).Eql(0, "running job list is empty, indicating job done")
			g.Assert(len(mockService.getRecordedEntitiesForDataset(dsName))).Eql(2, "both 'pages' have been posted")
		})

		g.It("Should fullsync from an untokenized HttpDatasetSource", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People")

			// define job
			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()
			g.Assert(len(scheduler.GetRunningJobs())).Eql(0, "running job list is empty")
			rs, err := ds.GetEntities("", 100)
			g.Assert(err).IsNil("we found data in sink")
			g.Assert(len(rs.Entities)).Eql(10, "we found 10 entites (MockService generates 10 results)")
		})

		g.It("Should fullsync from a tokenized HttpDatasetSource", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People")

			// define job
			jobJson := `
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()
			g.Assert(len(scheduler.GetRunningJobs())).Eql(0, "running job list is empty")
			rs, err := ds.GetEntities("", 100)
			g.Assert(err).IsNil("we found data in sink")
			g.Assert(len(rs.Entities)).Eql(20, "we found 10 entites (MockService generates 20 results)")
		})

		//func TestDatasetToHttpDatasetSink(m *testing.T) {
		g.It("Should incrementally sync to an HttpDatasetSink", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("Entities are stored correctly")

			// define job
			jobJson := `
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()
			g.Assert(len(scheduler.GetRunningJobs())).Eql(0, "running job list not empty")
		})

		g.It("Should incrementally do internal sync with js transform in parallel", func() {
			g.Timeout(time.Hour)
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

			count := 19
			entities := make([]*server.Entity, count)
			for i:=0;i<count;i++ {
				entity := server.NewEntity("http://data.mimiro.io/people/p" + strconv.Itoa(i) , 0)
				entity.Properties["name"] = "homer" + strconv.Itoa(i)
				entity.References["type"] = "http://data.mimiro.io/model/Person"
				entities[i] = entity
			}

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// define job
			jobJson := `
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(19, "correct number of entities retrieved")
		})


		g.It("Should incrementally do internal sync with js transform in parallel when les than workers count", func() {
			g.Timeout(time.Hour)
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

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
			g.Assert(err).IsNil("entities are stored")

			// define job
			jobJson := `
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(2, "correct number of entities retrieved")
		})


		//func TestDatasetToDatasetWithJavascriptTransformJob(m *testing.T) {
		g.It("Should incrementally do internal sync with js transform", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// define job
			jobJson := `
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil("pipeline is parsed")

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "correct number of entities retrieved")
		})
		g.It("Should run a transform with query in internal jobs", func() {
			testNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/test/")

			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People")
			ds1, _ := dsm.CreateDataset("Companies")
			_, _ = dsm.CreateDataset("NewPeople")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity(testNamespacePrefix+":gra", 0)
			entity.Properties[testNamespacePrefix+":name"] = "homer"
			entity.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Person"
			entity.References[testNamespacePrefix+":worksfor"] = testNamespacePrefix + ":mimiro"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			companies := make([]*server.Entity, 1)
			mimiro := server.NewEntity(testNamespacePrefix+":mimiro", 0)
			mimiro.Properties[testNamespacePrefix+":name"] = "Mimiro"
			mimiro.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Company"
			companies[0] = mimiro

			g.Assert(ds1.StoreEntities(companies)).IsNil()

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
			jobJson := fmt.Sprintf(`{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewPeople")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
			g.Assert(result.Entities[0].Properties["ns3:companyname"]).Eql("Mimiro")
		})
		g.It("Should run external transforms in internal jobs", func() {
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			// define job
			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should not write to the sink if an external transform endpoint is 404", func() {
			jobJson := ` {
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()
			g.Assert(dsm.GetDataset("People")).IsZero("dataset should not exist as job failed")
		})

		g.It("Should copy datasetsource to datasetsink in first run of internal job", func() {
			ds, _ := dsm.CreateDataset("Products")
			_, _ = dsm.CreateDataset("NewProducts")

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			jobJson := ` {
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should copy from samplesource to datasetsink in first run of new job", func() {
			_, _ = dsm.CreateDataset("People")

			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should copy from HttpDatasetSource to datasetSink in first run of new job", func() {
			_, _ = dsm.CreateDataset("People")

			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			job.Run()

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(10)
		})
		g.It("Should copy all pages using continuatin tokens from httpDatasetSource to datasetSink if first run", func() {
			_, _ = dsm.CreateDataset("People")

			jobJson := `{
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

			jobConfig, _ := scheduler.Parse([]byte(jobJson))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			g.Assert(err).IsNil()

			job := &job{
				id:       jobConfig.Id,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
			}

			// run once
			job.Run()

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(20)

			// run again
			job.Run()
			peopleDataset = dsm.GetDataset("People")
			result, err = peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(20, "results should stay the same")
		})

		g.It("Should mark entities that have not been received again during fullsync to internal dataset as deleted", func() {
			sourceDs, _ := dsm.CreateDataset("people")
			sinkDs, _ := dsm.CreateDataset("people2")

			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = sourceDs.StoreEntities([]*server.Entity{e1, e2})

			pipeline := &FullSyncPipeline{PipelineSpec{
				source: &datasetSource{DatasetName: "people", Store: store, DatasetManager: dsm},
				sink:   &datasetSink{DatasetName: "people2", Store: store, DatasetManager: dsm},
			}}

			job := &job{id: "fullsync-1", pipeline: pipeline, runner: runner}

			// run once, both entities should sync
			job.Run()

			res, err := sinkDs.GetEntities("", 100)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(2)
			g.Assert(res.Entities[0].ID).Eql("1")
			g.Assert(res.Entities[0].IsDeleted).Eql(false)
			g.Assert(res.Entities[1].ID).Eql("2")
			g.Assert(res.Entities[1].IsDeleted).Eql(false)

			// delete ds and recreate with only 1 entity
			g.Assert(dsm.DeleteDataset("people")).IsNil()
			sourceDs, _ = dsm.CreateDataset("people")
			g.Assert(sourceDs.StoreEntities([]*server.Entity{e2})).IsNil()

			// run again. deletion detection should apply
			job.Run()

			res, err = sinkDs.GetEntities("", 100)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(2)
			g.Assert(res.Entities[0].ID).Eql("1")
			g.Assert(res.Entities[0].IsDeleted).Eql(true, "Entity 1 should be deleted now")
			g.Assert(res.Entities[1].ID).Eql("2")
			g.Assert(res.Entities[1].IsDeleted).Eql(false)

		})
		g.It("Should store continuation token after every page in incremental job", func() {
			pipeline := &IncrementalPipeline{PipelineSpec{
				batchSize: 5,
				source:    &sampleSource{NumberOfEntities: 10, Store: store},
				sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/inctest/fullsync", Store: store},
			}}
			job := &job{id: "inc-1", pipeline: pipeline, runner: runner}

			//run async, so we can verify tokens in parallel
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				job.Run()
				wg.Done()
			}()

			//block and wait for channel notification - indicating the first page/batch has been received
			_ = <-mockService.HttpNotificationChannel
			g.Assert(len(mockService.getRecordedEntitiesForDataset("inctest"))).
				Eql(5, "After first batch, 5 entities should have been postet to httpSink")

			//block for next batch request finished - this should be before syncState is updated
			_ = <-mockService.HttpNotificationChannel
			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("5", "Between batch 1 and 2, token should be continuation of batch 1")

			wg.Wait()
			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("10")
		})
		g.It("Should store continuation token only after finished run in fullsync job", func() {
			pipeline := &FullSyncPipeline{PipelineSpec{
				batchSize: 5,
				source:    &sampleSource{NumberOfEntities: 10, Store: store},
				//sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
				sink: &devNullSink{},
			}}
			job := &job{id: "full-1", pipeline: pipeline, runner: runner}

			//run async, so we can verify tokens in parallel
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				job.Run()
				wg.Done()
			}()
			//block and wait for channel notification - indicating the first page/batch has been received
			//_ = <-mockService.HttpNotificationChannel
			//g.Assert(len(mockService.getRecordedEntitiesForDataset("fulltest"))).
			//	Eql(5, "After first batch, 5 entities should have been postet to httpSink")

			//wait for first syncState (token) update in badger (should be in db when 2nd batch arrives)
			//_ = <-mockService.HttpNotificationChannel
			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("", "there should not be a token stored after first batch")

			//wait for job to finish
			wg.Wait()
			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, job.id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("10", "First after job there should be a token")
		})

		g.It("Should post changes to HttpDatasetSink endpoint if jobType is incremental", func() {
			srcDs, _ := dsm.CreateDataset("src")
			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
			e1.IsDeleted = true
			_ = srcDs.StoreEntities([]*server.Entity{e1})
			e1.IsDeleted = false
			_ = srcDs.StoreEntities([]*server.Entity{e1})

			var sourceChanges []*server.Entity
			_, err := srcDs.ProcessChanges(0, 100, func(entity *server.Entity) {
				sourceChanges = append(sourceChanges, entity)
			})
			g.Assert(err).IsNil()
			g.Assert(len(sourceChanges)).Eql(4, "Expected 4 changes for our two entities in source")

			pipeline := &IncrementalPipeline{PipelineSpec{
				batchSize: 5,
				source:    &datasetSource{DatasetName: "src", Store: store, DatasetManager: dsm},
				sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/inctest/fullsync", Store: store},
			}}
			job := &job{id: "inc-1", pipeline: pipeline, runner: runner}
			job.Run()
			sinkChanges := mockService.getRecordedEntitiesForDataset("inctest")
			g.Assert(len(sinkChanges)).Eql(4, "Expected all 4 changes in sink for incremental")
		})

		g.It("Should post entities to HttpDatasetSink endpoint if jobType is fullsync", func() {
			srcDs, _ := dsm.CreateDataset("src")
			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
			e1.IsDeleted = true
			_ = srcDs.StoreEntities([]*server.Entity{e1})
			e1.IsDeleted = false
			_ = srcDs.StoreEntities([]*server.Entity{e1})

			var sourceChanges []*server.Entity
			_, err := srcDs.ProcessChanges(0, 100, func(entity *server.Entity) {
				sourceChanges = append(sourceChanges, entity)
			})
			g.Assert(err).IsNil()
			g.Assert(len(sourceChanges)).Eql(4, "Expected 4 changes for our two entities in source")

			pipeline := &FullSyncPipeline{PipelineSpec{
				batchSize: 5,
				source:    &datasetSource{DatasetName: "src", Store: store, DatasetManager: dsm},
				sink:      &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
			}}
			job := &job{id: "inc-1", pipeline: pipeline, runner: runner}
			job.Run()
			sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(2, "Expected only 2 entities in current state in fullsync")

		})
	})
}
