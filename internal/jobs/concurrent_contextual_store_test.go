package jobs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/datahub/internal/service/scheduler"
	"go.uber.org/zap"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestContextualStore(t *testing.T) {
	t.Skip("Skipping this test as it is resource intensive, uncomment this skip to run the test when needed")
	testCnt := 0
	var storeLocation string
	var scheduler *Scheduler
	var store *server.Store
	var runner *Runner
	var dsm *server.DsManager
	setup := func() func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./concurrent_contextual_store_test_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		if err != nil {
			t.Fatal(err)
		}

		e := &conf.Config{
			Logger:        logger,
			StoreLocation: storeLocation,
			RunnerConfig:  &conf.RunnerConfig{PoolIncremental: 10, PoolFull: 5, Concurrent: 0},
		}
		// temp redirect of stdout to swallow some annoying init messages in fx and jobrunner and mockService
		store = server.NewStore(e, &statsd.NoOpClient{})

		//pm := security.NewProviderManager(e, store, logger)
		//tps := security.NewTokenProviders(logger, pm, nil)
		runner = NewRunner(e, store, nil, server.NoOpBus(), &statsd.NoOpClient{})
		dsm = server.NewDsManager(e, store, server.NoOpBus())
		scheduler = NewScheduler(e, store, dsm, runner)
		// return teardown function
		return func() {
			defer func(path string) {
				err := os.RemoveAll(path)
				if err != nil {
					t.Fatal()
				}
			}(storeLocation)
			runner.Stop()
			err := scheduler.Stop(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			err = store.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Run("import and transform at same time", func(t *testing.T) {
		defer setup()()
		ds, _ := dsm.CreateDataset("Products", nil)
		_, _ = dsm.CreateDataset("NewProducts", nil)
		_, _ = dsm.CreateDataset("ProductAudit", nil)

		wg := sync.WaitGroup{}
		wg.Add(3)

		fail := func() {
			t.Fatal("error")
		}
		// import
		go func() {

			for j := 0; j < 100; j++ {
				numEntities := 1000
				entities := make([]*server.Entity, numEntities)
				for i := 0; i < numEntities; i++ {
					id := j*1000 + i
					entity := server.NewEntity(fmt.Sprintf("http://data.mimiro.io/people/%v", id), 0)
					entity.Properties["name"] = "homer"
					entity.Properties["age"] = id
					entity.References["r"] = fmt.Sprintf("http://data.mimiro.io/addresses/%v", id+1000000)
					entities[i] = entity
				}
				err := ds.StoreEntities(entities)
				if err != nil {
					fail()
				}
			}
			fmt.Println("done importing")
			wg.Done()
		}()

		// transform 1
		go func() {
			// transform js
			js := `
			function transform_entities(entities) {
				for (e of entities) {
					AssertNamespacePrefix("http://data.mimiro.io/people/"+GetId(e)+"/");
					e.Properties["name"] = "Marge "+GetId(e);
					e.References["r"] = "http://data.mimiro.io/addresses/"+GetId(e)+"/123";
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
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
			"id" : "ajob",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities": 1000
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "` + jscriptEnc + `"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "NewProducts"
			}
		}`
			jobConfig, _ := scheduler.Parse([]byte(jobJSON))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			if err != nil {
				fail()
			}

			job := &job{
				id:       jobConfig.ID,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
				dsm:      dsm,
			}

			job.Run()
			fmt.Println("done transforming 1")
			wg.Done()
		}()

		// transform 2
		go func() {
			// transform js
			js := `
			function transform_entities(entities) {
				for (e of entities) {
					AssertNamespacePrefix("http://data.mimiro.io/cars/"+GetId(e)+"/");
					e.Properties["name"] = "BMW "+GetId(e);
					e.References["r"] = "http://data.mimiro.io/parking/"+GetId(e)+"/123";
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
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
			"id" : "anotherjob",
			"triggers": [{"triggerType": "cron", "jobType": "incremental", "schedule": "@every 2s"}],
			"source" : {
				"Type" : "SampleSource",
				"NumberOfEntities": 1000
			},
			"transform" : {
				"Type" : "JavascriptTransform",
				"Code" : "` + jscriptEnc + `"
			},
			"sink" : {
				"Type" : "DatasetSink",
				"Name" : "NewProducts"
			}
		}`
			jobConfig, _ := scheduler.Parse([]byte(jobJSON))
			pipeline, err := scheduler.toPipeline(jobConfig, JobTypeIncremental)
			if err != nil {
				fail()
			}

			job := &job{
				id:       jobConfig.ID,
				pipeline: pipeline,
				schedule: jobConfig.Triggers[0].Schedule,
				runner:   runner,
				dsm:      dsm,
			}

			job.Run()
			fmt.Println("done transforming 2")
			wg.Done()
		}()
		wg.Wait()

		stats := getStats(t, store, dsm, zap.NewNop().Sugar())
		products := stats.get("entity", "DATASET_ENTITY_CHANGE_LOG", "Products", "keys")
		if products != 100000.0 {
			t.Fatalf("expected 100000 products, got %v", products)
		}

		newProducts := stats.get("entity", "DATASET_ENTITY_CHANGE_LOG", "NewProducts", "keys")
		if newProducts != 2000.0 {
			t.Fatalf("expected 200 newProducts, got %v", newProducts)
		}

		productAudit := stats.get("entity", "DATASET_ENTITY_CHANGE_LOG", "ProductAudit", "keys")
		if productAudit != 2000.0 {
			t.Fatalf("expected 200 productAudit, got %v", productAudit)
		}

		productRefsInc := stats.get("refs", "INCOMING_REF_INDEX", "Products", "keys")
		if productRefsInc != 100000.0 {
			t.Fatalf("expected 100000 productRefsInc, got %v", productRefsInc)
		}

		productRefsNew := stats.get("refs", "INCOMING_REF_INDEX", "NewProducts", "keys")
		if productRefsNew != 2000.0 {
			t.Fatalf("expected 200 productRefsNew, got %v", productRefsNew)
		}

		productRefsAudit := stats.get("refs", "INCOMING_REF_INDEX", "ProductAudit", "keys")
		if productRefsAudit != 2000.0 {
			t.Fatalf("expected 200 productRefsAudit, got %v", productRefsAudit)
		}

		productsRefsOut := stats.get("refs", "OUTGOING_REF_INDEX", "Products", "keys")
		if productsRefsOut != 100000.0 {
			t.Fatalf("expected 100000 productsRefsOut, got %v", productsRefsOut)
		}

		newProductsRefsOut := stats.get("refs", "OUTGOING_REF_INDEX", "NewProducts", "keys")
		if newProductsRefsOut != 2000.0 {
			t.Fatalf("expected 200 newProductsRefsOut, got %v", newProductsRefsOut)
		}

		productAuditRefsOut := stats.get("refs", "OUTGOING_REF_INDEX", "ProductAudit", "keys")
		if productAuditRefsOut != 2000.0 {
			t.Fatalf("expected 200 productAuditRefsOut, got %v", productAuditRefsOut)
		}

		urisToIds := stats.get("urimap", "URI_TO_ID_INDEX_ID", "other", "keys")
		if urisToIds != 203007.0 {
			t.Fatalf("expected 203007 urisToIds, got %v", urisToIds)
		}
		idsToUris := stats.get("urimap", "ID_TO_URI_INDEX_ID", "other", "keys")
		if idsToUris != 203007.0 {
			t.Fatalf("expected 203007 idsToUris, got %v", idsToUris)
		}
	})
}

type testStats map[string]any

func (s testStats) get(keys ...string) any {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return s[keys[0]]
	}
	newMap := testStats(s[keys[0]].(map[string]any))
	newKeys := keys[1:]
	return newMap.get(newKeys...)
}
func getStats(t *testing.T, store *server.Store, dsm *server.DsManager, log *zap.SugaredLogger) testStats {
	t.Helper()
	// ba.GetDB().Flatten(4)
	// time.Sleep(10 * time.Second)

	ba := server.NewBadgerAccess(store, dsm)
	su := scheduler.NewStatisticsUpdater(log, ba)
	su.Run()
	for su.State() != scheduler.TaskStateScheduled {
		// fmt.Println("waiting for stats update")
		time.Sleep(100 * time.Millisecond)
	}
	sr := &server.Statistics{
		Store:  store,
		Logger: log,
	}
	stringWriter := &strings.Builder{}
	sr.GetStatistics(stringWriter)
	var o map[string]any
	err := json.Unmarshal([]byte(stringWriter.String()), &o)
	if err != nil {
		t.Fatalf("error unmarshalling stats: %v", err)
	}

	return o
}
