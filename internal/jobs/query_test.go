package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/mimiro-io/datahub/internal"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
)

// implements QueryResultWriter interface
type TestQueryResultWriter struct {
	Results []interface{}
}

func NewTestQueryResultWriter() *TestQueryResultWriter {
	return &TestQueryResultWriter{Results: make([]interface{}, 0)}
}

func (t *TestQueryResultWriter) WriteObject(result interface{}) error {
	t.Results = append(t.Results, result)
	return nil
}

var _ = Describe("QueryExecution", func() {
	testCnt := 0
	var dsm *server.DsManager
	var store *server.Store
	var storeLocation string
	BeforeEach(func() {
		testCnt += 1
		storeLocation = fmt.Sprintf("./test_store_relations_%v", testCnt)
		err := os.RemoveAll(storeLocation)
		Expect(err).To(BeNil(), "should be allowed to clean test files in "+storeLocation)
		e := &conf.Env{
			Logger:        zap.NewNop().Sugar(),
			StoreLocation: storeLocation,
		}
		lc := fxtest.NewLifecycle(internal.FxTestLog(GinkgoT(), false))
		store = server.NewStore(lc, e, &statsd.NoOpClient{})
		dsm = server.NewDsManager(lc, e, store, server.NoOpBus())

		err = lc.Start(context.Background())
		Expect(err).To(BeNil())
	})
	AfterEach(func() {
		_ = store.Close()
		_ = os.RemoveAll(storeLocation)
	})

	It("Should support js query that just writes to the response writer", func() {
		// transform js
		js := `
			function do_query() {
				let obj = { "name": "homer" };
				WriteQueryResult(obj);
			}
			`
		queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

		// create query transform
		transform, _ := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)

		// create test result writer
		resultWriter := NewTestQueryResultWriter()

		// run query
		err := transform.ExecuteQuery(resultWriter)
		if err != nil {
			Fail(err.Error())
		}

		// check result
		Expect(len(resultWriter.Results)).To(Equal(1))
		Expect(resultWriter.Results[0]).To(Equal(map[string]interface{}{"name": "homer"}))
	})

	It("Should support js query that can access dataset changes", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*server.Entity, 1)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// transform js
		js := `
			function do_query() {
				let obj = GetDatasetChanges("people", 0, 5);
				let entities = obj.Entities;
				let count = 0;
				for (let i = 0; i < entities.length; i++) {
					count++;
				}

				let result = { "count": count };
				WriteQueryResult(result);
			}
			`

		queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

		// create query transform
		transform, _ := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)

		// create test result writer
		resultWriter := NewTestQueryResultWriter()

		// run query
		err = transform.ExecuteQuery(resultWriter)
		if err != nil {
			Fail(err.Error())
		}

		// check result
		Expect(len(resultWriter.Results)).To(Equal(1))
		Expect(resultWriter.Results[0]).To(Equal(map[string]interface{}{"count": int64(1)}))
	})

	It("Should support js query that can access dataset changes with continuation token", func() {
		// populate dataset with some entities
		ds, _ := dsm.CreateDataset("people", nil)

		entities := make([]*server.Entity, 2)
		entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
		entity.Properties["name"] = "homer"
		entities[0] = entity

		entity = server.NewEntity("http://data.mimiro.io/people/marge", 0)
		entity.Properties["name"] = "marge"
		entities[1] = entity

		err := ds.StoreEntities(entities)
		Expect(err).To(BeNil(), "entities are stored")

		// transform js
		js := `
			function do_query() {
				let count = 0;
				let names = [];

				let obj1 = GetDatasetChanges("people", 0, 1);
				let entities1 = obj1.Entities;
				for (let i = 0; i < entities1.length; i++) {
					names.push(entities1[i].ID);
					count++;
				}

				let obj2 = GetDatasetChanges("people", obj1.NextToken, 1);
				let entities2 = obj2.Entities;
				for (let i = 0; i < entities2.length; i++) {
					names.push(entities2[i].ID);
					count++;
				}

				let result = { "count": count, "names": names };
				WriteQueryResult(result);
			}
			`

		queryEncoded := base64.StdEncoding.EncodeToString([]byte(js))

		// create query transform
		transform, err := NewJavascriptTransform(zap.NewNop().Sugar(), queryEncoded, store, dsm)
		if err != nil {
			Fail(err.Error())
		}

		// create test result writer
		resultWriter := NewTestQueryResultWriter()

		// run query
		err = transform.ExecuteQuery(resultWriter)
		if err != nil {
			Fail(err.Error())
		}

		// check result
		Expect(len(resultWriter.Results)).To(Equal(1))
		Expect(resultWriter.Results[0]).
			To(Equal(map[string]interface{}{
				"count": int64(2),
				"names": []interface{}{
					"http://data.mimiro.io/people/homer",
					"http://data.mimiro.io/people/marge",
				},
			}))
	})
})
